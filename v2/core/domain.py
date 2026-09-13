"""Authoritative v2 task and action state; models can propose, never submit."""
from datetime import timedelta
import time
from sqlalchemy import select, update, or_
from ..config import config
from ..db import now, uid
from ..domain import DomainError, order_view, action_view, preview_view
from ..events import emit
from ..incidents import PUBLIC_MESSAGE
from ..models import Consumer, Conversation, Order, Task, Message, Interaction, Preview, Action, Application, Feedback
from . import db as database, runtime, temporal
from .models import Turn, GenerationAttempt, KnowledgeVersion
from .settings import settings

GOALS = {"qa": "咨询规则", "query": "查询订单", "eligibility": "判断退货资格", "application": "准备退货申请"}
TERMINAL = {"completed", "cancelled"}

def event(db, name, c=None, task=None, **data):
    return emit(db, name, c, task, api_version=2, workflow_version="wf-2",
                prompt_version="intent-2_grounded-2", **data)

def touch(c):
    c.view_version = (c.view_version or 0) + 1

async def conversation(db, consumer, cid, lock=True):
    query = select(Conversation).where(Conversation.id == cid, Conversation.consumer_id == consumer)
    c = await db.scalar(query.with_for_update() if lock else query)
    if not c: raise DomainError("not_authorized", "无权访问该内容，或内容不存在。", 404)
    if c.api_version != 2: raise DomainError("api_version_mismatch", "这是 v1 会话，请返回原页面继续。")
    return c

async def task_scope(db, c, task_id):
    t = await db.get(Task, task_id)
    if not t or t.conversation_id != c.id or t.consumer_id != c.consumer_id or t.api_version != 2:
        raise DomainError("not_authorized", "无权访问该事项。", 404)
    return t

async def order_scope(db, consumer, oid, lock=False):
    query = select(Order).where(Order.id == oid, Order.consumer_id == consumer)
    o = await db.scalar(query.with_for_update() if lock else query)
    if not o: raise DomainError("not_authorized", "无权访问该订单，或订单不存在。", 404)
    return o

async def allow_new(order=None, write=False):
    cfg = config()
    if not settings()["enabled"] or not cfg["ai_enabled"] or \
       (order and order.merchant_id in cfg["disabled_merchants"]) or (write and not cfg["writes_enabled"]):
        raise DomainError("scope_disabled", "当前自动处理已暂停，原申请仍可查询。", 503)
    if write:
        async with database.Ledger() as ledger:
            if await runtime.cost_total(ledger) >= cfg["total_credits"]:
                raise DomainError("credit_budget", "当前自动处理费用预算已达到上限。", 503)

def check_turn(c, turn, t=None, expected_context=None):
    if (turn.status != "running" or c.inflight_id != turn.id or c.automation_state != "active"
            or turn.control_version != c.control_version or turn.deadline_at <= now()):
        raise DomainError("context_changed", "当前状态已变化，本轮结果不再展示。")
    if t and (t.turn_id != turn.id or t.status == "cancelled" or
              (expected_context is not None and t.context_version != expected_context)):
        raise DomainError("context_changed", "当前事项已变化，本轮结果不再展示。")

def message(db, c, t, text, cards=None, *, turn=None, evidence=None, role="assistant", display_state="visible"):
    if turn and role == "assistant": check_turn(c, turn, t)
    data = {"api_version": 2, "control_version": c.control_version,
            "order_id": t.order_id if t else None,
            "workflow_version": "wf-2", "prompt_version": "intent-2_grounded-2",
            "release_id": "2.0.0-candidate", **(evidence or {})}
    m = Message(id=uid(), conversation_id=c.id, task_id=t.id if t else None,
                turn_id=turn.id if turn else None, role=role, text=text, cards=cards or [],
                evidence=data, display_state=display_state)
    db.add(m); touch(c)
    event(db, "answer_generated" if role == "assistant" else "user_interaction_received", c, t,
          answer_ref=m.id, turn_id=m.turn_id, display_state=display_state)
    return m

async def create_conversation(consumer):
    await allow_new()
    async with database.Session.begin() as db:
        if not await db.get(Consumer, consumer): raise DomainError("not_authorized", "无效演示账号。", 404)
        c = Conversation(id=uid(), consumer_id=consumer, api_version=2, automation_state="active",
                         control_version=1, state_version=1, view_version=1)
        db.add(c); await db.flush()
        message(db, c, None, "你好，我是 YxWoof。你可以一次提出几件事，我会分别说明处理结果和下一步。")
        return c.id

def task_view(t):
    return {"id": t.id, "turn_id": t.turn_id, "goal": t.goal, "goal_label": GOALS.get(t.goal, "明确诉求"),
            "question": t.requested_goal, "order_id": t.order_id, "status": t.status,
            "active": t.active, "fields": t.fields, "dependencies": t.dependencies,
            "context_version": t.context_version, "result": t.result,
            "clarification": t.context.get("clarification"), "action_id": t.action_id,
            "created_at": t.created_at.isoformat()}

def feedback_view(f):
    return {"id": f.id, "helpful": f.helpful, "category": f.category, "comment": f.comment,
            "csat": f.csat, "ces": f.ces, "created_at": f.created_at.isoformat()}

async def get_state(consumer, cid):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        tasks = list((await db.scalars(select(Task).where(Task.conversation_id == cid, Task.api_version == 2)
                                      .order_by(Task.created_at, Task.id))).all())
        messages = list((await db.scalars(select(Message).where(Message.conversation_id == cid,
            Message.display_state == "visible").order_by(Message.created_at, Message.id))).all())
        feedback = list((await db.scalars(select(Feedback).outerjoin(Task, Task.id == Feedback.task_id)
            .outerjoin(Message, Message.id == Feedback.message_id)
            .where(or_(Task.conversation_id == cid, Message.conversation_id == cid), Feedback.consumer_id == consumer)
            .order_by(Feedback.created_at, Feedback.id))).all())
        by_message = {f.message_id: feedback_view(f) for f in feedback if f.message_id}
        ids = [t.id for t in tasks]
        previews = (await db.scalars(select(Preview).where(Preview.task_id.in_(ids)).order_by(Preview.created_at))).all() if ids else []
        actions = (await db.scalars(select(Action).where(Action.task_id.in_(ids)))).all() if ids else []
        turn = await db.get(Turn, c.inflight_id) if c.inflight_id else await db.scalar(
            select(Turn).where(Turn.conversation_id == cid).order_by(Turn.created_at.desc()).limit(1))
        consumer_row = await db.get(Consumer, consumer)
        order = await order_scope(db, consumer, c.order_id) if c.order_id else None
        return {"id": cid, "api_version": 2, "server_time": now().isoformat(),
                "consumer": {"id": consumer, "name": consumer_row.name},
                "automation_state": c.automation_state, "control_version": c.control_version,
                "state_version": c.state_version, "view_version": c.view_version,
                "accepting_requests": settings()["enabled"] and config()["ai_enabled"],
                "order": order_view(order) if order else None, "issues": [task_view(t) for t in tasks],
                "previews": {p.task_id: preview_view(p) for p in previews},
                "actions": {a.id: action_view(a) for a in actions},
                "turn": {"id": turn.id, "status": turn.status, "phase": turn.phase,
                         "deadline_at": turn.deadline_at.isoformat()} if turn else None,
                "messages": [{"id": m.id, "task_id": m.task_id, "turn_id": m.turn_id, "role": m.role,
                    "text": m.text, "cards": m.cards, "created_at": m.created_at.isoformat(),
                    "control_version": m.evidence.get("control_version", 1),
                    "source_ids": m.evidence.get("source_ids", []), "feedback": by_message.get(m.id)} for m in messages]}

async def stop_inflight(db, c, reason):
    if not c.inflight_id: return
    turn = await db.get(Turn, c.inflight_id)
    if turn and turn.status == "running":
        await suppress_unseen(db, c, turn.id)
        turn.status, turn.phase, turn.finished_at = "cancelled", reason, now()
        tasks = (await db.scalars(select(Task).where(Task.turn_id == turn.id,
                                 Task.status.in_(["pending", "running"])))).all()
        for t in tasks:
            t.status = "deferred"; t.context_version += 1
        event(db, "turn_cancelled", c, turn_id=turn.id, reason=reason)
    c.inflight_id = None; c.inflight_until = None; touch(c)


async def acknowledge_seen(db, c, ids):
    for mid in ids or []:
        m = await db.get(Message, mid)
        if not m or m.conversation_id != c.id or m.role != "assistant":
            raise DomainError("not_authorized", "无法确认该回复的展示状态。", 404)
        if m.display_state == "visible": m.delivery_state = "displayed"
    await db.flush()


async def suppress_unseen(db, c, turn_id=None):
    query = select(Message).where(Message.conversation_id == c.id, Message.role == "assistant",
        Message.turn_id.is_not(None), Message.display_state == "visible", Message.delivery_state != "displayed")
    if turn_id: query = query.where(Message.turn_id == turn_id)
    for m in (await db.scalars(query)).all():
        m.display_state = "suppressed"
        event(db, "reply_delivery_suppressed", c, message_id=m.id, turn_id=m.turn_id)

async def reserve_interaction(db, c, iid, kind):
    previous = await db.get(Interaction, iid)
    if previous:
        if previous.conversation_id != c.id or previous.kind != kind:
            raise DomainError("interaction_conflict", "请求编号已用于其他操作。")
        return False
    db.add(Interaction(id=iid, conversation_id=c.id, kind=kind))
    return True

async def begin_turn(consumer, cid, text, iid, resume_id=None, selected_goal=None, started_at=None):
    started_at = started_at if started_at is not None else time.monotonic()
    await allow_new()
    request_hash = temporal.digest({"text": text, "resume_id": resume_id, "goal": selected_goal})
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        previous = await db.get(Turn, iid)
        if previous:
            if previous.conversation_id != cid or previous.request_hash != request_hash:
                raise DomainError("interaction_conflict", "请求编号与原请求内容不一致。")
            return None
        if c.automation_state != "active": raise DomainError("ai_paused", "AI 已暂停，请先明确继续。")
        if c.inflight_id:
            if c.inflight_until and c.inflight_until > now():
                raise DomainError("turn_busy", "上一轮仍在处理，请查看状态或停止等待。")
            await stop_inflight(db, c, "expired")
        await reserve_interaction(db, c, iid, "v2_chat")
        turn = Turn(id=iid, conversation_id=cid, consumer_id=consumer, raw_text=text,
                    request_hash=request_hash, control_version=c.control_version,
                    status="running", deadline_at=now() + timedelta(seconds=max(0, settings()["turn_seconds"] - (time.monotonic() - started_at))))
        db.add(turn); await db.flush()
        c.inflight_id = iid; c.inflight_until = turn.deadline_at; c.state_version += 1
        resume = None
        if resume_id:
            t = await task_scope(db, c, resume_id)
            if t.status not in ("awaiting_input", "deferred", "awaiting_confirmation"):
                raise DomainError("task_closed", "该事项不能继续，请发起新的问题。")
            if await db.get(Action, t.action_id):
                raise DomainError("action_exists", "该申请已经派发，请查询原申请。")
            await db.execute(update(Preview).where(Preview.task_id == t.id).values(invalidated=True))
            t.turn_id = iid; t.context_version += 1; t.status = "running"
            resume = {**task_view(t), "context": t.context}
        message(db, c, None, text, turn=turn, role="user")
        event(db, "service_request_received", c, request_id=iid, turn_id=iid)
        return {"consumer": consumer, "cid": cid, "iid": iid, "order_id": c.order_id,
                "text": text, "resume": resume, "selected_goal": selected_goal,
                "control_version": c.control_version, "started_at": started_at}

async def finish_turn(consumer, cid, iid, error=None, timings=None):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        turn = await db.get(Turn, iid)
        if not turn or turn.conversation_id != cid: return
        if timings: turn.timings = timings
        if turn.status == "running":
            rows = (await db.scalars(select(Task).where(Task.turn_id == iid))).all()
            if error and not rows and c.inflight_id == iid:
                failed = Task(id=uid(), api_version=2, conversation_id=c.id, consumer_id=consumer, turn_id=iid,
                    order_id=c.order_id, requested_goal=turn.raw_text, goal="clarify", scenario="understanding_failure",
                    status="deferred", active=True, context={"raw_input": turn.raw_text, "failure_stage": turn.phase})
                db.add(failed); await db.flush(); rows = [failed]
            for t in rows:
                if t.status in ("running", "pending"):
                    t.status = "deferred"; t.context_version += 1
            incomplete = any(t.status not in TERMINAL for t in rows)
            turn.status = "deferred" if error else "partial" if incomplete else "completed"
            turn.phase = "failed" if error else "finished"
            turn.finished_at = now()
            if error and getattr(error, "code", "") not in ("context_changed", "turn_cancelled") and c.inflight_id == iid and c.automation_state == "active" and c.control_version == turn.control_version:
                message(db, c, rows[0] if rows else None, PUBLIC_MESSAGE, [{"type": "recovery"}],
                        evidence={"failed_turn_id": iid, "failure_code": getattr(error, "code", type(error).__name__)})
            event(db, "turn_finished", c, turn_id=iid, status=turn.status,
                  reason_code=getattr(error, "code", type(error).__name__) if error else None)
        if c.inflight_id == iid:
            c.inflight_id = None; c.inflight_until = None
        touch(c)

async def bind_order(consumer, cid, oid, iid, task_id=None, seen_ids=None):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        await order_scope(db, consumer, oid)
        if not await reserve_interaction(db, c, iid, "v2_bind"): return
        await acknowledge_seen(db, c, seen_ids)
        stopped_id = c.inflight_id
        await stop_inflight(db, c, "binding_changed")
        if task_id:
            t = await task_scope(db, c, task_id)
            if t.status in TERMINAL or await db.get(Action, t.action_id):
                raise DomainError("task_closed", "该事项已结束或已派发申请，不能更换对象。")
            t.order_id = oid; t.context_version += 1; t.status = "awaiting_input"
            t.context = {**t.context, "clarification": None}
            await db.execute(update(Preview).where(Preview.task_id == t.id).values(invalidated=True))
        c.order_id = oid; c.state_version += 1; touch(c)
        event(db, "object_bound", c, object_ref=oid, task_id=task_id)
        return stopped_id

async def control(consumer, cid, mode, iid, seen_ids=None):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        if not await reserve_interaction(db, c, iid, "v2_control_" + mode): return
        await acknowledge_seen(db, c, seen_ids)
        if mode == c.automation_state: return None
        stopped_id = c.inflight_id
        await stop_inflight(db, c, "consumer_" + mode)
        if mode != c.automation_state:
            if mode == "paused": await suppress_unseen(db, c)
            c.automation_state = mode; c.control_version += 1; c.state_version += 1
            ids = select(Task.id).where(Task.conversation_id == cid)
            await db.execute(update(Preview).where(Preview.task_id.in_(ids)).values(invalidated=True))
            message(db, c, None, "AI 已暂停。人工渠道尚未接入，你仍可查看原申请状态。" if mode == "paused"
                    else "已恢复 AI。之前未完成的事项需要你明确选择继续。")
        event(db, "automation_control_changed", c, automation_state=mode)
        touch(c)
        return stopped_id

async def cancel(consumer, cid, iid, task_id=None, turn_id=None, seen_ids=None):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        if not await reserve_interaction(db, c, iid, "v2_cancel"): return
        await acknowledge_seen(db, c, seen_ids)
        stopped_id = None
        if turn_id:
            turn = await db.get(Turn, turn_id)
            if not turn or turn.conversation_id != cid: raise DomainError("not_authorized", "无权访问该轮次。", 404)
            await suppress_unseen(db, c, turn_id)
            if c.inflight_id == turn_id:
                stopped_id = turn_id; await stop_inflight(db, c, "consumer_cancelled")
        elif task_id:
            t = await task_scope(db, c, task_id)
            if t.status in TERMINAL: return None
            if c.inflight_id == t.turn_id:
                stopped_id = t.turn_id; await stop_inflight(db, c, "task_cancelled")
            t.context_version += 1
            if not await db.get(Action, t.action_id):
                t.status = "cancelled"; t.active = False
            await db.execute(update(Preview).where(Preview.task_id == t.id).values(invalidated=True))
            message(db, c, t, "本事项已停止处理。已经派发的申请不会因此撤销，请查询原申请。"
                    if await db.get(Action, t.action_id) else "本事项已取消，尚未提交申请。")
        touch(c)
        return stopped_id

def business_fingerprint(o, snapshot):
    return temporal.digest({"order": temporal.order_facts(o), "status": o.status,
        "delivery_days": o.delivery_days, "version": o.version, "price_cents": o.price_cents,
        "product": o.product, "spec": o.spec, "policy": snapshot.fingerprint})

async def make_preview(consumer, cid, task_id, fields, iid, expected_turn=None, expected_context=None):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        t = await task_scope(db, c, task_id)
        if expected_turn:
            turn = await db.get(Turn, expected_turn); check_turn(c, turn, t, expected_context)
        if c.automation_state != "active": raise DomainError("ai_paused", "请先明确继续 AI。")
        if t.goal != "application" or t.status in TERMINAL or not t.order_id:
            raise DomainError("task_required", "请先明确要申请退货的订单。")
        o = await order_scope(db, consumer, t.order_id, True)
        await allow_new(o, True)
        if await db.get(Action, t.action_id): raise DomainError("action_exists", "该申请已经派发，请查询原申请。")
        if not await reserve_interaction(db, c, iid, "v2_preview"):
            return
        snapshot, policies = await temporal.policy_snapshot(db, o)
        allowed, explanation = temporal.eligible(o, snapshot.resolved_rules)
        if not allowed: raise DomainError("ineligible", explanation)
        data = {**t.fields, **fields}
        reason = str(data.get("reason", "")).strip()
        if not 2 <= len(reason) <= 300 or data.get("condition") != "unused_intact":
            raise DomainError("missing_fields", "请补充退货原因，并确认商品完好、未使用。", 422)
        if await db.scalar(select(Application.id).where(Application.order_id == o.id)):
            raise DomainError("application_exists", "该订单已有申请，请查询原记录。")
        await db.execute(update(Preview).where(Preview.task_id == t.id).values(invalidated=True))
        t.fields = {"reason": reason, "condition": "unused_intact"}; t.context_version += 1
        content = {"order_id": o.id, "merchant": o.merchant_name, "product": o.product, "spec": o.spec,
            "type": "标准退货售后申请", "reason": reason, "condition": "商品完好、未使用", "quantity": 1,
            "order_amount_cents": o.price_cents, "notice": "仅提交申请，受理不等于审核通过或退款到账。",
            "_binding": business_fingerprint(o, snapshot), "policy_snapshot_id": snapshot.id}
        p = Preview(id=uid(), task_id=t.id, consumer_id=consumer, order_id=o.id, action_id=t.action_id,
            content=content, content_hash=temporal.digest(content), policy_version=snapshot.fingerprint,
            fact_version=o.version, control_version=c.control_version, context_version=t.context_version,
            expires_at=now() + timedelta(seconds=config()["preview_seconds"]))
        db.add(p); t.status = "awaiting_confirmation"
        message(db, c, t, "申请预览已准备好。请核对并确认，现在尚未提交。", [{"type": "preview", "preview_id": p.id}],
                turn=turn if expected_turn else None,
                evidence={"source_ids": snapshot.version_ids, "policy_snapshot_id": snapshot.id,
                          "content_hashes": {v.id: v.content_hash for v in policies}})
        event(db, "application_preview_created", c, t, preview_id=p.id, source_ids=snapshot.version_ids)
        return preview_view(p)

async def confirm(consumer, cid, pid, content_hash, confirmation_id, fault=None):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        p = await db.get(Preview, pid)
        if not p or p.consumer_id != consumer: raise DomainError("not_authorized", "无权访问该预览。", 404)
        t = await task_scope(db, c, p.task_id)
        a = await db.get(Action, p.action_id)
        if a: return action_view(a)
        o = await order_scope(db, consumer, p.order_id, True)
        await allow_new(o, True)
        if not (c.automation_state == "active" and t.active and t.status == "awaiting_confirmation"
                and t.order_id == p.order_id and not p.invalidated and p.expires_at > now()
                and content_hash == p.content_hash and p.control_version == c.control_version
                and p.context_version == t.context_version and p.fact_version == o.version):
            raise DomainError("stale_confirmation", "预览已失效，请重新核对并确认。")
        snapshot, policies = await temporal.policy_snapshot(db, o)
        allowed, _ = temporal.eligible(o, snapshot.resolved_rules)
        if not allowed or p.content["_binding"] != business_fingerprint(o, snapshot):
            raise DomainError("facts_changed", "订单事实或适用规则已变化，请重新生成预览。")
        if await db.scalar(select(Application.id).where(Application.order_id == o.id)):
            raise DomainError("application_exists", "该订单已有申请，请查询原记录。")
        if not await reserve_interaction(db, c, confirmation_id, "v2_confirm"):
            raise DomainError("interaction_conflict", "确认编号已用于其他预览，请读取原申请状态。")
        app = Application(id="AS-" + uid(), action_id=t.action_id, consumer_id=consumer,
                          order_id=o.id, status="accepted", content=p.content)
        a = Action(id=t.action_id, task_id=t.id, consumer_id=consumer, order_id=o.id,
                   preview_id=p.id, confirmation_id=confirmation_id,
                   status="unknown" if fault == "lost_ack" else "accepted",
                   application_id=None if fault == "lost_ack" else app.id,
                   reconcile_started_at=now() if fault == "lost_ack" else None)
        db.add_all([a, app]); p.invalidated = True
        t.status = "deferred" if fault == "lost_ack" else "completed"; t.active = fault == "lost_ack"
        t.result = {"action_id": a.id, "application_id": a.application_id, "business_status": a.status}
        message(db, c, t, "提交结果尚未核实，请查询原申请，不要重复创建。" if fault == "lost_ack"
                else "申请已受理，等待业务处理；受理不代表审核通过或退款到账。",
                [{"type": "result", "action_id": a.id}], evidence={"policy_snapshot_id": snapshot.id,
                    "source_ids": snapshot.version_ids, "content_hashes": {v.id: v.content_hash for v in policies}})
        event(db, "action_dispatch_decided", c, t, decision="allow", action_id=a.id, confirmation_id=confirmation_id)
        event(db, "business_result_observed", c, t, action_id=a.id, business_status=a.status)
        await db.flush()
        return action_view(a)

async def reconcile(consumer, cid, aid, automatic=False):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        a = await db.get(Action, aid)
        if not a or a.consumer_id != consumer: raise DomainError("not_authorized", "无权访问该申请。", 404)
        t = await task_scope(db, c, a.task_id)
        await order_scope(db, consumer, a.order_id)
        if a.status != "unknown": return action_view(a)
        cfg = config()
        if automatic and (a.reconcile_count >= cfg["reconcile_limit"] or not a.reconcile_started_at or
                          (now() - a.reconcile_started_at).total_seconds() > cfg["reconcile_seconds"]):
            return action_view(a)
        a.reconcile_count += 1
        result = await db.scalar(select(Application).where(Application.action_id == aid, Application.consumer_id == consumer))
        if result:
            a.status = "accepted"; a.application_id = result.id
            t.status = "completed"; t.active = False
            t.result = {"action_id": a.id, "application_id": result.id, "business_status": "accepted"}
        # Update factual cards/state only. Recovery never appends a late chat answer.
        event(db, "reconciliation_finished", c, t, action_id=aid, result=a.status, automatic=automatic)
        touch(c)
        return action_view(a)

async def save_feedback(consumer, cid, target_id, data, target="message"):
    async with database.Session.begin() as db:
        c = await conversation(db, consumer, cid)
        m = await db.get(Message, target_id) if target == "message" else None
        if target == "message" and (not m or m.conversation_id != cid or m.role != "assistant" or
                                     m.display_state != "visible"):
            raise DomainError("not_authorized", "无法评价这条回复。", 404)
        t = await task_scope(db, c, m.task_id if m else target_id) if not m or m.task_id else None
        tid = t.id if t else None
        previous = await db.get(Feedback, data["id"])
        mid = m.id if m else None
        if previous:
            if previous.consumer_id != consumer or previous.task_id != tid or previous.message_id != mid:
                raise DomainError("not_authorized", "无权访问该反馈。", 404)
            if any(getattr(previous, k) != v for k, v in data.items() if k != "id"):
                raise DomainError("feedback_conflict", "修改评价请使用新的请求编号。")
            return feedback_view(previous)
        query = select(Feedback).where(Feedback.consumer_id == consumer, Feedback.task_id == tid,
                                      Feedback.message_id == mid).order_by(Feedback.created_at.desc(), Feedback.id.desc()).limit(1)
        latest = await db.scalar(query)
        row = Feedback(**data, consumer_id=consumer, task_id=tid, message_id=mid,
                       previous_id=latest.id if latest else None)
        db.add(row); await db.flush()
        event(db, "feedback_submitted", c, t, message_id=mid, feedback_id=row.id,
              helpful=row.helpful, category=row.category, previous_id=row.previous_id)
        touch(c)
        return feedback_view(row)

async def source(consumer, cid, source_id):
    async with database.Session() as db:
        c = await conversation(db, consumer, cid, False)
        m = await db.scalar(select(Message).where(Message.conversation_id == cid,
            Message.display_state == "visible", Message.evidence.contains({"source_ids": [source_id]})))
        v = await db.get(KnowledgeVersion, source_id)
        if not m or not v or not v.consumer_visible or not m.task_id:
            raise DomainError("not_authorized", "无权访问该依据。", 404)
        t = await task_scope(db, c, m.task_id)
        evidence_order = m.evidence.get("order_id") or m.evidence.get("order_facts", {}).get("order_id") or t.order_id
        o = await order_scope(db, consumer, evidence_order)
        if (v.merchant_id, v.sku) != (o.merchant_id, o.sku):
            raise DomainError("not_authorized", "无权访问该依据。", 404)
        return {**temporal.version_view(v), "historical_reference": True,
                "notice": "这是该回复当时引用的版本，适用范围和时间以该版本及订单记录为准。"}
