"""Configurable HTTP adapter with a DB-backed budget ledger and no automatic retries."""
import asyncio
import json
import time
import os
import weakref
from dataclasses import dataclass
from sqlalchemy import select, func, text
import httpx
from .config import config, model_settings
from .db import SessionLocal, engine, uid
from .models import ModelRun
from .events import emit
from .domain import DomainError
from .incidents import record

LOCK_ID=87800017
_clients=weakref.WeakKeyDictionary()

def pooled_client():
    """One transport per event loop; share connections, never cross event loops."""
    loop=asyncio.get_running_loop()
    client=_clients.get(loop)
    if client is None or client.is_closed:
        client=httpx.AsyncClient(trust_env=False,limits=httpx.Limits(max_connections=1,max_keepalive_connections=1,keepalive_expiry=60))
        _clients[loop]=client
    return client

async def close_transport():
    client=_clients.pop(asyncio.get_running_loop(),None)
    if client is not None: await client.aclose()

@dataclass
class TurnBudget:
    interaction_id: str
    started: float = 0
    model_calls: int = 0
    tool_calls: int = 0
    def __post_init__(self):
        if not self.started: self.started=time.monotonic()
    def remaining(self):
        return max(0.0, config()["turn_seconds"]-(time.monotonic()-self.started))
    def tool(self):
        self.tool_calls+=1
        if self.tool_calls>config()["tool_limit"] or self.remaining()<=0:
            raise DomainError("turn_budget","本轮处理已达到资源上限，请稍后继续。",503)

def cost_total(db, interaction=None):
    q=select(func.coalesce(func.sum(func.coalesce(ModelRun.actual,ModelRun.reserved)),0))
    if interaction: q=q.where(ModelRun.interaction_id==interaction)
    return float(db.scalar(q))

async def request(kind,payload,budget):
    cfg=config()
    settings=model_settings(kind)
    payload={**payload,"model":settings["model"],**settings["extra"]}
    prices=settings["pricing"]
    if not cfg["ai_enabled"]:
        raise DomainError("scope_disabled","AI 服务当前已停用；原申请状态仍可查询。",503)
    if kind=="chat":
        budget.model_calls+=1
        if budget.model_calls>cfg["model_limit"]:
            raise DomainError("model_budget","本轮模型调用已达到上限。",503)
    if budget.remaining()<=0:
        raise DomainError("turn_timeout","本轮处理超时，尚未完成；你可以稍后继续。",503)
    serialized=json.dumps(payload,ensure_ascii=False)
    # UTF-8 byte count plus generous protocol margin is a conservative input-token bound.
    reserved=((len(serialized.encode())+2048)/1_000_000*prices["input_reservation_per_million"]+
              payload.get("max_tokens",1024)/1_000_000*prices["output_reservation_per_million"]) if kind=="chat" else prices[kind+"_reserve"]
    run_id=uid()
    connection=engine.connect()
    locked=False
    started=time.monotonic()
    try:
        while budget.remaining()>0:
            locked=bool(connection.scalar(text("SELECT pg_try_advisory_lock(:id)"),{"id":LOCK_ID}))
            if locked: break
            await asyncio.sleep(0.15)
        if not locked:
            raise DomainError("model_busy","模型服务当前繁忙，本轮未继续调用，请稍后再试。",503)
        cfg=config()
        if not cfg["ai_enabled"]:
            raise DomainError("scope_disabled","AI 服务在等待期间已停用；本次不再派发模型请求。",503)
        with SessionLocal.begin() as db:
            if cost_total(db)+reserved>cfg["total_credits"] or cost_total(db,budget.interaction_id)+reserved>cfg["turn_credits"]:
                emit(db,"runtime_budget_reached",budget_type="credits",interaction_id=budget.interaction_id)
                blocked=True
            else:
                blocked=False
                db.add(ModelRun(id=run_id,interaction_id=budget.interaction_id,kind=kind,model=payload["model"],
                                status="reserved",reserved=reserved))
        if blocked:
            raise DomainError("credit_budget","模型预算已达到上限，暂不能新增 AI 处理；已有申请仍可查询。",503)
        path={"chat":"/chat/completions","embedding":"/embeddings","rerank":"/rerank"}[kind]
        try:
            queued_ms=int((time.monotonic()-started)*1000)
            request_started=time.monotonic()
            response=await pooled_client().post(settings["base_url"]+path,json=payload,timeout=max(0.1,budget.remaining()),headers={"Authorization":"Bearer "+settings["key"]})
            request_ms=int((time.monotonic()-request_started)*1000)
            if response.status_code!=200:
                raise DomainError("model_http_"+str(response.status_code),"模型服务暂不可用，本次未完成。请稍后再试。",503)
            result=response.json()
            usage=result.get("usage") or {}
            actual=None
            if kind=="chat" and "prompt_tokens" in usage and "completion_tokens" in usage:
                inp=max(0,int(usage["prompt_tokens"])); out=max(0,int(usage["completion_tokens"]))
                hit=min(inp,max(0,int((usage.get("prompt_tokens_details") or {}).get("cached_tokens",0))))
                actual=((inp-hit)*prices["input_per_million"]+hit*prices["cached_input_per_million"]+out*prices["output_per_million"])/1_000_000
            with SessionLocal.begin() as db:
                r=db.get(ModelRun,run_id); r.status="success"; r.actual=actual; r.usage=usage
                r.latency_ms=int((time.monotonic()-started)*1000)
                emit(db,"model_run_finished",run_id=run_id,interaction_id=budget.interaction_id,model_version=payload["model"],
                     status="success",latency_ms=r.latency_ms,token_usage=usage,cost_status="estimated" if actual is not None else "reserved_unknown",
                     cost_amount=actual,currency=prices["unit"],queue_ms=queued_ms,request_ms=request_ms,transport="keepalive-v1")
            return result
        except BaseException as error:
            incident_id=record(error, "model_api", interaction_id=budget.interaction_id)
            try:
                with SessionLocal.begin() as db:
                    r=db.get(ModelRun,run_id)
                    r.status="failed"; r.error_code=error.code if isinstance(error,DomainError) else type(error).__name__
                    r.latency_ms=int((time.monotonic()-started)*1000)
                    emit(db,"model_run_finished",run_id=run_id,interaction_id=budget.interaction_id,status="failed",
                         error_code=r.error_code,cost_status="reserved_unknown",latency_ms=r.latency_ms,incident_id=incident_id)
            except Exception as ledger_error:
                record(ledger_error, "model_ledger", interaction_id=budget.interaction_id)
            if isinstance(error,asyncio.CancelledError): raise
            if isinstance(error,DomainError): raise
            failure=DomainError("model_unavailable","模型服务超时或返回异常，本次未完成；没有自动重试。",503)
            failure._yx_incident_id=incident_id
            raise failure from None
    finally:
        try:
            if locked: connection.execute(text("SELECT pg_advisory_unlock(:id)"),{"id":LOCK_ID})
        except Exception as error:
            record(error, "model_lock_release", interaction_id=budget.interaction_id)
            connection.invalidate()
        finally: connection.close()

async def structured(system,user,schema,name,budget,max_tokens=768):
    data=await request("chat",{"messages":[{"role":"system","content":system},{"role":"user","content":user}],
                             "temperature":0.1,"stream":False,"max_tokens":max_tokens,
                             "response_format":{"type":"json_schema","json_schema":{"name":name,"schema":schema}}},budget)
    try:
        return json.loads(data["choices"][0]["message"]["content"])
    except (KeyError,IndexError,ValueError,TypeError):
        raise DomainError("model_format","模型未返回有效结构化结果，本次未执行办理。",503) from None

async def embed(texts,budget):
    if any(len(t)>8192 for t in texts): raise DomainError("input_limit","知识片段过长",422)
    data=await request("embedding",{"input":texts},budget)
    vectors=[x["embedding"] for x in sorted(data["data"],key=lambda x:x["index"])]
    if len(vectors)!=len(texts) or any(len(v)!=1024 for v in vectors):
        raise DomainError("embedding_format","向量服务返回格式异常",503)
    return vectors

async def rerank(query,documents,budget):
    data=await request("rerank",{"query":query,"documents":documents,"top_n":min(5,len(documents)),"return_documents":False},budget)
    rows=data.get("results")
    if not isinstance(rows,list): raise DomainError("rerank_format","重排服务返回异常",503)
    indices=[r["index"] for r in rows if isinstance(r.get("index"),int) and 0<=r["index"]<len(documents)]
    if not indices: raise DomainError("rerank_empty","重排未返回有效依据",503)
    return indices
