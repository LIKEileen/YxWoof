"""Bounded asynchronous ECNU calls sharing the original credit ledger and lock."""
import asyncio
import json
import math
import time
import weakref
import contextvars
from collections import deque
from dataclasses import dataclass, field
import httpx
from sqlalchemy import select, func, text
from ..config import config, credential
from ..db import uid, now
from ..domain import DomainError
from ..incidents import record
from ..models import ModelRun
from . import db as database
from .settings import settings

BASE_URL = "https://chat.ecnu.edu.cn/open/api/v1"
LOCK_ID = 87800017  # Same provider-wide lock as v1, including embedding and reranking.
_clients = weakref.WeakKeyDictionary()


class CircuitBreaker:
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.failures = deque()
        self.open_until = 0.0
        self.probing = False
        self.last_result = "not_probed"
        self.last_observed_at = None

    def check(self):
        if self.open_until > self.clock() or self.probing:
            raise DomainError("model_circuit_open", "服务正在恢复，请稍后再试。", 503)

    def acquire(self):
        self.check()
        self.probing = bool(self.open_until)

    def success(self):
        self.failures.clear(); self.open_until = 0.0; self.probing = False
        self.last_result = "available"; self.last_observed_at = now().isoformat()

    def failure(self):
        cfg = settings(); at = self.clock()
        self.failures.append(at)
        while self.failures and self.failures[0] < at - cfg["circuit_window_seconds"]:
            self.failures.popleft()
        if self.probing or len(self.failures) >= cfg["circuit_failures"]:
            self.open_until = at + cfg["circuit_open_seconds"]
        self.probing = False
        self.last_result = "unavailable"; self.last_observed_at = now().isoformat()

    def release_probe(self):
        self.probing = False

    def state(self):
        return "open" if self.open_until > self.clock() else "half_open" if self.open_until else "closed"


breakers = {kind: CircuitBreaker() for kind in ("chat", "embedding", "rerank", "database", "ledger")}
active_budget = contextvars.ContextVar("v2_active_budget", default=None)


@dataclass
class Budget:
    interaction_id: str
    started: float = field(default_factory=time.monotonic)
    complex: bool = False
    model_calls: int = 0
    tool_calls: int = 0
    cancelled: bool = False
    timings: dict = field(default_factory=dict)

    def remaining(self):
        return max(0.0, settings()["turn_seconds"] - (time.monotonic() - self.started))

    def check(self):
        if self.cancelled: raise DomainError("turn_cancelled", "本轮已停止。")
        if self.remaining() <= 0: raise DomainError("turn_timeout", "本轮处理已超时。", 503)

    def tool(self):
        self.check(); self.tool_calls += 1
        if self.tool_calls > settings()["tool_limit"]:
            raise DomainError("turn_budget", "本轮处理已达到资源上限。", 503)


def client():
    loop = asyncio.get_running_loop()
    value = _clients.get(loop)
    if value is None or value.is_closed:
        value = httpx.AsyncClient(trust_env=False, limits=httpx.Limits(max_connections=1,
                                  max_keepalive_connections=1, keepalive_expiry=60))
        _clients[loop] = value
    return value


async def close():
    value = _clients.pop(asyncio.get_running_loop(), None)
    if value: await value.aclose()


async def cost_total(db, interaction_id=None):
    query = select(func.coalesce(func.sum(func.coalesce(ModelRun.actual, ModelRun.reserved)), 0))
    if interaction_id: query = query.where(ModelRun.interaction_id == interaction_id)
    return float(await db.scalar(query))


async def mark_failed(run_id, error, elapsed):
    async with database.Ledger.begin() as db:
        row = await db.get(ModelRun, run_id)
        if row:
            row.status = "failed"
            row.error_code = getattr(error, "code", type(error).__name__)
            row.latency_ms = int(elapsed * 1000)


async def request(kind, payload, budget):
    budget.check()
    cfg = config()
    if not cfg["ai_enabled"]:
        raise DomainError("scope_disabled", "AI 服务当前暂停。", 503)
    if kind == "chat":
        budget.model_calls += 1
        limit = settings()["complex_model_limit" if budget.complex else "simple_model_limit"]
        if budget.model_calls > limit:
            raise DomainError("model_budget", "本轮模型调用已达到上限。", 503)
    breaker = breakers[kind]
    breaker.check()
    serialized = json.dumps(payload, ensure_ascii=False)
    reserved = ((len(serialized.encode()) + 2048) * .0001 + payload.get("max_tokens", 1024) * .0004
                if kind == "chat" else .05 if kind == "embedding" else .1)
    run_id, locked, dispatched, reserved_row = uid(), False, False, False
    started = time.monotonic()
    connection = None
    try:
        # All DB waits consume the same turn deadline. No synchronous DB call runs here.
        async with asyncio.timeout(budget.remaining()):
            connection = await database.ledger_engine.connect()
            connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
            queue_deadline = min(started + settings()["queue_seconds"], started + budget.remaining())
            while time.monotonic() < queue_deadline:
                locked = bool(await connection.scalar(text("SELECT pg_try_advisory_lock(:id)"), {"id": LOCK_ID}))
                if locked: break
                await asyncio.sleep(min(.05, max(0, queue_deadline - time.monotonic())))
            if not locked:
                raise DomainError("model_busy", "服务当前繁忙，本轮未继续调用。", 503)
            budget.check(); breaker.acquire()
            cfg = config()
            if not cfg["ai_enabled"]:
                raise DomainError("scope_disabled", "AI 服务在等待期间已暂停。", 503)
            async with database.Ledger.begin() as db:
                if (await cost_total(db)) + reserved > cfg["total_credits"] or \
                   (await cost_total(db, budget.interaction_id)) + reserved > cfg["turn_credits"]:
                    raise DomainError("credit_budget", "模型费用预算已达到上限。", 503)
                db.add(ModelRun(id=run_id, interaction_id=budget.interaction_id, kind=kind,
                                model=payload["model"], status="reserved", reserved=reserved))
            reserved_row = True
            queued_ms = int((time.monotonic() - started) * 1000)
            request_started = time.monotonic()
            path = {"chat": "/chat/completions", "embedding": "/embeddings", "rerank": "/rerank"}[kind]
            budget.check(); dispatched = True
            response = await client().post(BASE_URL + path, json=payload,
                headers={"Authorization": "Bearer " + credential()},
                timeout=httpx.Timeout(max(.1, budget.remaining()), connect=min(3, max(.1, budget.remaining()))))
            if response.status_code != 200:
                raise DomainError("model_http_" + str(response.status_code), "模型服务暂不可用。", 503)
            value = response.json()
            if not isinstance(value, dict): raise ValueError("Invalid model envelope")
            usage = value.get("usage") or {}
            actual = reserved if kind != "chat" else None
            if kind == "chat" and "prompt_tokens" in usage and "completion_tokens" in usage:
                inp, out = max(0, int(usage["prompt_tokens"])), max(0, int(usage["completion_tokens"]))
                cached = min(inp, max(0, int((usage.get("prompt_tokens_details") or {}).get("cached_tokens", 0))))
                actual = (inp - cached) * .0001 + cached * .00002 + out * .0004
            async with database.Ledger.begin() as db:
                row = await db.get(ModelRun, run_id)
                row.status, row.actual, row.usage = "success", actual, usage
                row.latency_ms = int((time.monotonic() - started) * 1000)
            budget.timings.setdefault("model_runs", []).append({"id": run_id, "kind": kind,
                "queue_ms": queued_ms, "request_ms": int((time.monotonic() - request_started) * 1000)})
            breaker.success()
            return value
    except BaseException as error:
        code = getattr(error, "code", "")
        transient = isinstance(error, (httpx.TransportError, TimeoutError, ValueError)) or \
            code == "model_http_429" or code.startswith("model_http_5")
        if dispatched and transient: breaker.failure()
        else: breaker.release_probe()
        if not isinstance(error, asyncio.CancelledError):
            record(error, "v2_model", interaction_id=budget.interaction_id)
        if reserved_row:
            try:
                await asyncio.wait_for(mark_failed(run_id, error, time.monotonic() - started), timeout=1)
            except Exception as ledger_error:
                record(ledger_error, "v2_model_ledger", interaction_id=budget.interaction_id)
        if isinstance(error, (asyncio.CancelledError, DomainError)): raise
        raise DomainError("model_unavailable", "服务暂时不可用。", 503) from None
    finally:
        if connection:
            try:
                if locked:
                    await asyncio.wait_for(connection.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": LOCK_ID}), 1)
            except BaseException:
                await connection.invalidate()
            finally:
                await connection.close()


async def structured(system, user, schema, name, budget, max_tokens=1400):
    data = await request("chat", {"model": config().get("model", "ecnu-plus"),
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "thinking": {"type": "disabled"}, "temperature": .1, "stream": False,
        "max_tokens": max_tokens, "response_format": {"type": "json_schema", "json_schema": {
            "name": name, "schema": schema}}}, budget)
    try: return json.loads(data["choices"][0]["message"]["content"])
    except (KeyError, IndexError, ValueError, TypeError):
        breakers["chat"].failure()
        raise DomainError("model_format", "模型未返回有效的结构化结果。", 503) from None


async def embed(texts, budget):
    if any(len(t) > 8192 for t in texts):
        raise DomainError("input_limit", "知识片段过长。", 422)
    data = await request("embedding", {"model": "ecnu-embedding-small", "input": texts}, budget)
    try:
        vectors = [x["embedding"] for x in sorted(data["data"], key=lambda x: x["index"])]
        if len(vectors) != len(texts) or any(len(v) != 1024 or not all(math.isfinite(x) for x in v) for v in vectors):
            raise ValueError()
        return vectors
    except (KeyError, TypeError, ValueError):
        breakers["embedding"].failure()
        raise DomainError("embedding_format", "向量服务返回异常。", 503) from None


async def rerank(query, documents, budget):
    data = await request("rerank", {"model": "ecnu-rerank", "query": query,
        "documents": documents, "top_n": min(5, len(documents)), "return_documents": False}, budget)
    rows, seen = [], set()
    for item in data.get("results", []):
        idx = item.get("index")
        score = item.get("relevance_score", item.get("score"))
        if type(idx) is int and 0 <= idx < len(documents) and idx not in seen:
            seen.add(idx)
            rows.append({"index": idx, "score": score if isinstance(score, (int, float)) and math.isfinite(score) else None})
    if not rows: raise DomainError("rerank_format", "重排未返回有效依据。", 503)
    return rows
