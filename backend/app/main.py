import asyncio
import hashlib
import json
import secrets
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Literal
from uuid import UUID
from fastapi import FastAPI, Request, Response, Depends, Query
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from .db import SessionLocal, now, uid
from .models import BrowserSession, Consumer, Knowledge, Action, Task, Event, Message, Preview
from .config import AVATAR_PATH, ROOT, config
from .events import emit, CLIENT_EVENTS
from . import domain, agent
from .incidents import PUBLIC_MESSAGE, public_error, record, request_context
from .public_help import public_help

def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()

async def recover_loop():
    while True:
        await asyncio.sleep(20)
        from .audit import sweep
        try:
            await asyncio.to_thread(sweep)
            with SessionLocal() as db:
                rows=db.execute(select(Action.id,Action.consumer_id,Task.conversation_id)
                                .join(Task,Task.id==Action.task_id).where(Action.status=="unknown")).all()
        except Exception as error:
            record(error, "recovery_scan")
            continue
        for aid,consumer,cid in rows:
            try:
                await asyncio.to_thread(domain.reconcile,consumer,cid,aid,True)
            except Exception as error:
                record(error, "recovery_action")  # Keep unknown; never replay an uncertain write.

@asynccontextmanager
async def lifespan(app):
    from .audit import sweep
    try: await asyncio.to_thread(sweep)
    except Exception as error: record(error, "startup")
    recovery=asyncio.create_task(recover_loop())
    yield
    recovery.cancel()
    try: await recovery
    except asyncio.CancelledError: pass
    await agent.llm.close_transport()

app=FastAPI(title="YxWoof Consumer API",version="0.1.0",lifespan=lifespan,docs_url=None,redoc_url=None)
chat_tasks=set()

def chat_finished(task):
    chat_tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        record(task.exception(), "chat_background")

@app.exception_handler(domain.DomainError)
async def domain_error(request,error):
    try:
        if "/confirm" in request.url.path:
            with SessionLocal.begin() as db:
                emit(db,"confirmation_validated",validity="invalid",reason_code=error.code)
                emit(db,"action_dispatch_decided",decision="deny",reason_code=error.code)
    except Exception as audit_error:
        record(audit_error, "confirmation_audit")
    body, status = public_error(error)
    return JSONResponse(body,status_code=status)

@app.exception_handler(IntegrityError)
async def unique_conflict(request,error):
    return JSONResponse({"error":"conflict","message":"该操作已被处理或状态已变化，请读取当前状态。"},status_code=409)

@app.middleware("http")
async def same_origin(request,call_next):
    host=request.headers.get("host","").split(":")[0]
    if host not in ("127.0.0.1","localhost","testserver"):
        return JSONResponse({"error":"invalid_host","message":"请通过本机 SSH 隧道访问。"},status_code=400)
    if request.method in ("POST","PUT","PATCH","DELETE"):
        origin=request.headers.get("origin")
        expected=f"{request.url.scheme}://{request.headers.get('host')}"
        if origin!=expected:
            return JSONResponse({"error":"origin_required","message":"请求来源不匹配。"},status_code=403)
    context_token=request_context.set(uid())
    try:
        response=await call_next(request)
    except Exception as error:
        body, status=public_error(error)
        response=JSONResponse(body,status_code=status)
    finally:
        request_context.reset(context_token)
    response.headers["X-Content-Type-Options"]="nosniff"
    response.headers["Referrer-Policy"]="same-origin"
    response.headers["X-Frame-Options"]="DENY"
    response.headers["Content-Security-Policy"]="default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'"
    if request.url.path.startswith("/api/v1/") and "branding" not in request.url.path:
        response.headers["Cache-Control"]="no-store"
    return response

def auth(request:Request):
    token=request.cookies.get("yx_session","")
    with SessionLocal() as db:
        s=db.get(BrowserSession,digest(token))
        if not s or s.expires_at<=now():
            raise domain.DomainError("unauthenticated","请选择合成演示账号开始。",401)
        if request.method in ("POST","PUT","PATCH","DELETE"):
            csrf=request.headers.get("X-CSRF-Token","")
            if not secrets.compare_digest(digest(csrf),s.csrf_hash):
                raise domain.DomainError("csrf_invalid","会话校验失效，请重新选择演示账号。",403)
        return s.consumer_id

class StrictBody(BaseModel):
    model_config=ConfigDict(extra="forbid")

class Login(StrictBody):
    profile: Literal["lin","chen"]

class Chat(StrictBody):
    text: str=Field(min_length=1,max_length=2000)
    interaction_id: UUID
    selected_goal: Literal["qa","query","eligibility","application"]|None=None

class Bind(StrictBody):
    order_id: str=Field(max_length=80)
    interaction_id: UUID

class Fields(StrictBody):
    interaction_id: UUID|None=None
    reason: str=Field(min_length=2,max_length=300)
    condition: Literal["unused_intact"]

class Confirm(StrictBody):
    interaction_id: UUID|None=None
    content_hash: str=Field(min_length=64,max_length=64)
    confirmation_id: UUID

class Control(StrictBody):
    mode: Literal["paused","active"]
    interaction_id: UUID

class FeedbackBody(StrictBody):
    id: UUID
    task_id: UUID
    helpful: bool
    category: Literal["helpful","wrong_answer","not_resolved","slow","other"]
    comment: str=Field(default="",max_length=500)
    csat: int|None=Field(default=None,ge=1,le=5)
    ces: int|None=Field(default=None,ge=1,le=5)

class Operation(StrictBody):
    interaction_id: UUID|None=None

class ClientEvent(StrictBody):
    id: UUID
    name: str
    task_id: UUID|None=None
    message_id: UUID|None=None
    preview_id: UUID|None=None
    interaction_id: UUID|None=None
    client_monotonic_ms: float|None=Field(default=None,ge=0)
    duration_ms: float|None=Field(default=None,ge=0,le=86400000)
    action_result: str|None=Field(default=None,max_length=40)
    interaction_type: str|None=Field(default=None,max_length=40)
    object_ref: str|None=Field(default=None,max_length=80)
    candidate_count: int|None=Field(default=None,ge=0,le=10000)
    confirmation_id: UUID|None=None
    client_occurred_at: str|None=Field(default=None,max_length=40)
    answer_type: str|None=Field(default=None,max_length=40)
    preview_valid: bool|None=None
    task_elapsed_ms: float|None=Field(default=None,ge=0,le=604800000)
    clock_uncertainty_ms: float|None=Field(default=None,ge=0,le=5000)
    availability: str|None=Field(default=None,max_length=40)

@app.get("/api/v1/health")
def health():
    with SessionLocal() as db: db.execute(select(1))
    return {"status":"ok","name":"YxWoof","data_mode":"synthetic","release":config()["release_id"]}

@app.get("/api/v1/branding/avatar")
def avatar():
    return FileResponse(AVATAR_PATH,media_type="image/png",headers={"Cache-Control":"public, max-age=86400"})

@app.get("/api/v1/public-help")
def help_public():
    return public_help()

@app.post("/api/v1/demo-session")
def login(body:Login,request:Request,response:Response):
    token=secrets.token_urlsafe(32); csrf=secrets.token_urlsafe(32)
    with SessionLocal.begin() as db:
        if not db.get(Consumer,body.profile):
            raise domain.DomainError("demo_profile_unavailable","尚未配置演示业务数据",409)
        old=db.get(BrowserSession,digest(request.cookies.get("yx_session","")))
        if old: db.delete(old)
        db.add(BrowserSession(token_hash=digest(token),consumer_id=body.profile,csrf_hash=digest(csrf),
                              expires_at=now()+timedelta(days=7)))
    cid=domain.create_conversation(body.profile)
    response.set_cookie("yx_session",token,httponly=True,samesite="strict",max_age=7*86400,path="/")
    return {"csrf":csrf,"conversation_id":cid,"profile":body.profile}

@app.get("/api/v1/demo-profiles")
def demo_profiles():
    with SessionLocal() as db:
        present=set(db.scalars(select(Consumer.id).where(Consumer.id.in_(("lin","chen")))))
    return {"profiles":[profile for profile in ("lin","chen") if profile in present]}

@app.get("/api/v1/orders")
def orders(consumer=Depends(auth)):
    return {"orders":domain.list_orders(consumer)}

@app.get("/api/v1/conversations/{cid}")
def state(cid:str,resume:bool=False,consumer=Depends(auth)):
    return domain.get_state(consumer,cid,resume)

@app.post("/api/v1/conversations/{cid}/binding")
def bind(cid:str,body:Bind,consumer=Depends(auth)):
    domain.bind_order(consumer,cid,body.order_id,str(body.interaction_id))
    return domain.get_state(consumer,cid)

@app.post("/api/v1/conversations/{cid}/chat")
async def chat(cid:str,body:Chat,consumer=Depends(auth)):
    with SessionLocal() as db: domain.conversation(db,consumer,cid)
    async def stream():
        task=asyncio.create_task(agent.run_chat(consumer,cid,body.text,str(body.interaction_id),body.selected_goal))
        chat_tasks.add(task); task.add_done_callback(chat_finished)
        yield 'event: status\ndata: {"phase":"processing","message":"正在理解你的诉求"}\n\n'
        try:
            while not task.done():
                await asyncio.wait({task},timeout=2)
                if not task.done(): yield ": keep-alive\n\n"
            await task
            result=domain.get_state(consumer,cid)
            yield "event: state\ndata: "+json.dumps(result,ensure_ascii=False)+"\n\n"
            yield "event: done\ndata: {}\n\n"
        except Exception as error:
            error_body, _=public_error(error, "chat_stream")
            yield "event: error\ndata: "+json.dumps(error_body,ensure_ascii=False)+"\n\n"
            yield "event: done\ndata: {}\n\n"
        except asyncio.CancelledError:
            # The bounded background turn may finish read/preview work. No write is possible here.
            raise
    return StreamingResponse(stream(),media_type="text/event-stream",headers={"X-Accel-Buffering":"no"})

def record_operation(consumer,cid,iid,kind):
    with SessionLocal.begin() as db:
        c=domain.conversation(db,consumer,cid);t=domain.current_task(db,c,True)
        emit(db,"user_interaction_received",c,t,interaction_id=str(iid) if iid else uid(),interaction_type=kind)

@app.post("/api/v1/conversations/{cid}/preview")
def preview(cid:str,body:Fields,consumer=Depends(auth)):
    record_operation(consumer,cid,body.interaction_id,"preview")
    domain.make_preview(consumer,cid,body.model_dump(exclude={"interaction_id"}))
    return domain.get_state(consumer,cid)

@app.post("/api/v1/conversations/{cid}/previews/{pid}/confirm")
def confirm(cid:str,pid:str,body:Confirm,consumer=Depends(auth)):
    record_operation(consumer,cid,body.confirmation_id,"confirm")
    domain.confirm(consumer,cid,pid,body.content_hash,str(body.confirmation_id))
    return domain.get_state(consumer,cid)

@app.post("/api/v1/conversations/{cid}/cancel")
def cancel(cid:str,body:Operation,consumer=Depends(auth)):
    record_operation(consumer,cid,body.interaction_id,"cancel")
    domain.cancel_task(consumer,cid)
    return domain.get_state(consumer,cid)

@app.post("/api/v1/conversations/{cid}/control")
def control(cid:str,body:Control,consumer=Depends(auth)):
    domain.control(consumer,cid,body.mode,str(body.interaction_id))
    return domain.get_state(consumer,cid)

@app.get("/api/v1/conversations/{cid}/actions/{aid}")
def action(cid:str,aid:str,consumer=Depends(auth)):
    domain.reconcile(consumer,cid,aid)
    return domain.get_state(consumer,cid)

@app.get("/api/v1/conversations/{cid}/help")
def help_entry(cid:str,consumer=Depends(auth)):
    return domain.help_info(consumer,cid)

@app.get("/api/v1/conversations/{cid}/sources/{kid}")
def source(cid:str,kid:str,consumer=Depends(auth)):
    with SessionLocal.begin() as db:
        c=domain.conversation(db,consumer,cid)
        if not c.order_id: domain.reject_scope("source")
        o=domain.order_scope(db,consumer,c.order_id,c)
        k=db.get(Knowledge,kid)
        if not k or k.merchant_id!=o.merchant_id or k.sku!=o.sku or not k.enabled or not k.consumer_visible or not (k.valid_from<=now()<k.valid_until):
            domain.reject_scope("source")
        emit(db,"scope_check_decided",c,decision="allow",resource_type="source",evidence_ref=k.id)
        return {"id":k.id,"title":k.title,"content":k.content,"policy_version":k.policy_version,
                "merchant":o.merchant_name,"valid_until":k.valid_until.isoformat(),"updated_at":k.updated_at.isoformat()}

@app.post("/api/v1/conversations/{cid}/feedback")
def feedback(cid:str,body:FeedbackBody,consumer=Depends(auth)):
    return domain.save_feedback(consumer,cid,body.model_dump(mode="json"))

@app.post("/api/v1/conversations/{cid}/events")
def client_event(cid:str,body:ClientEvent,consumer=Depends(auth)):
    if body.name not in CLIENT_EVENTS: raise domain.DomainError("event_not_allowed","无效客户端事件",422)
    with SessionLocal.begin() as db:
        c=domain.conversation(db,consumer,cid)
        t=db.get(Task,str(body.task_id)) if body.task_id else None
        if body.task_id and (not t or t.conversation_id!=cid): domain.reject_scope("event_task")
        previous=db.get(Event,str(body.id))
        if previous:
            if previous.conversation_id!=cid: domain.reject_scope("event")
            return {"recorded":True}
        if body.message_id:
            m=db.get(Message,str(body.message_id))
            if not m or m.conversation_id!=cid or (t and m.task_id!=t.id): domain.reject_scope("event_message")
        if body.preview_id:
            p=db.get(Preview,str(body.preview_id))
            if not p or p.consumer_id!=consumer or not t or p.task_id!=t.id: domain.reject_scope("event_preview")
        payload=body.model_dump(mode="json",exclude_none=True)
        for key in ("id","name","task_id"): payload.pop(key,None)
        emit(db,body.name,c,t,producer="client",event_id=str(body.id),**payload)
    return {"recorded":True}

dist=ROOT/"frontend/dist"
if dist.exists():
    app.mount("/",StaticFiles(directory=dist,html=True),name="web")
