"""Authoritative consumer task state. No model may bypass these functions."""
import hashlib
import json
import re
from datetime import timedelta
from sqlalchemy import select, update
from .db import SessionLocal, uid, now
from .models import (Consumer, Order, Knowledge, Conversation, Task, Message, Preview,
                     Action, Application, Feedback, Interaction, Event)
from .events import emit
from .config import config
from .public_help import public_help
from .incidents import PUBLIC_MESSAGE

GOALS = {"qa": "咨询规则", "query": "查询订单", "eligibility": "判断申请资格", "application": "提交退货申请"}

class DomainError(Exception):
    def __init__(self, code, message, status=409):
        self.code, self.message, self.status = code, message, status
        super().__init__(message)

def reject_scope(resource):
    with SessionLocal.begin() as audit:
        emit(audit, "scope_check_decided", decision="deny", resource_type=resource, reason_code="not_authorized")
    raise DomainError("not_authorized", "无权访问该内容，或内容不存在。", 404)

def conversation(db, consumer, cid, lock=False):
    q = select(Conversation).where(Conversation.id == cid, Conversation.consumer_id == consumer)
    c = db.scalar(q.with_for_update() if lock else q)
    if not c:
        reject_scope("conversation")
    return c

def order_scope(db, consumer, oid, conv=None):
    o = db.scalar(select(Order).where(Order.id == oid, Order.consumer_id == consumer))
    if not o:
        reject_scope("order")
    emit(db, "scope_check_decided", conv, decision="allow", resource_type="order", order_ref=o.id,
         actor_pseudo_id=consumer, merchant_scope_id=o.merchant_id)
    return o

def current_task(db, c, include_last=False):
    t = db.scalar(select(Task).where(Task.conversation_id == c.id, Task.active.is_(True)))
    if not t and include_last:
        t = db.scalar(select(Task).where(Task.conversation_id == c.id).order_by(Task.created_at.desc()).limit(1))
    return t

def order_view(o):
    return {"id": o.id, "merchant_id": o.merchant_id, "merchant_name": o.merchant_name,
            "product": o.product, "spec": o.spec, "price_cents": o.price_cents,
            "status": o.status, "delivery_days": o.delivery_days, "ordered_date": o.ordered_date,
            "logistics": o.logistics, "updated_at": o.updated_at.isoformat(), "version": o.version}

def task_view(t):
    if not t:
        return None
    return {"id": t.id, "goal": t.goal, "goal_label": GOALS.get(t.goal, "明确当前事项"),
            "requested_goal": t.requested_goal, "status": t.status, "active": t.active,
            "fields": t.fields, "unhandled": t.unhandled, "created_at":t.created_at.isoformat(), "action_id": t.action_id,
            "result": t.result, "eligibility": t.eligibility}

def preview_view(p):
    return {"id": p.id, "action_id": p.action_id, "content": p.content, "content_hash": p.content_hash,
            "expires_at": p.expires_at.isoformat(), "invalidated": p.invalidated, "policy_version": p.policy_version}

def action_view(a):
    return {"id": a.id, "status": a.status, "application_id": a.application_id,
            "reconcile_count": a.reconcile_count, "updated_at": a.dispatched_at.isoformat()}

def add_message(db, c, t, text, cards=None, role="assistant"):
    m = Message(id=uid(), conversation_id=c.id, task_id=t.id if t else None,
                role=role, text=text, cards=cards or [])
    db.add(m)
    if role == "assistant":
        emit(db, "answer_generated", c, t, answer_ref=m.id, answer_type=cards[0]["type"] if cards else "text")
    return m

def change_status(db, c, t, status, **extra):
    old = t.status
    t.status = status
    t.updated_at = now()
    emit(db, "task_state_changed", c, t, from_state=old, to_state=status, **extra)

def invalidate(db, t):
    if t:
        db.execute(update(Preview).where(Preview.task_id == t.id, Preview.invalidated.is_(False)).values(invalidated=True))

def claim(db, c, t, message, milestone, **evidence):
    change_status(db, c, t, "completed")
    t.active = False
    t.result = {"milestone": milestone, "answer_ref": message.id, **evidence}
    emit(db, "task_milestone_claimed", c, t, milestone=milestone, answer_ref=message.id, **evidence)

def create_conversation(consumer):
    with SessionLocal.begin() as db:
        c = Conversation(id=uid(), consumer_id=consumer)
        db.add(c)
        db.flush()
        add_message(db, c, None, "你好，我是 YxWoof。查订单、了解规则，或准备一份售后申请，我会陪你一步步处理。", [{"type":"welcome"}])
        return c.id

def get_state(consumer, cid, resume=False):
    with SessionLocal.begin() as db:
        c = conversation(db, consumer, cid)
        if resume:
            emit(db, "resume_attempted", c, resume_reason="page_restore")
        t = current_task(db, c, True)
        o = order_scope(db, consumer, c.order_id, c) if c.order_id else None
        messages = db.scalars(select(Message).where(Message.conversation_id == c.id).order_by(Message.created_at, Message.id)).all()
        p = db.scalar(select(Preview).where(Preview.task_id == t.id).order_by(Preview.created_at.desc()).limit(1)) if t else None
        a = db.get(Action, t.action_id) if t else None
        if resume:
            emit(db, "resume_finished", c, t, restored_step=t.status if t else "ready",
                 automation_state=c.automation_state, result="allowed")
        return {"server_time":now().isoformat(),"id": c.id, "consumer": {"id":consumer,"name":db.get(Consumer, consumer).name},
                "automation_state": c.automation_state, "control_version":c.control_version,
                "state_version":c.state_version, "order":order_view(o) if o else None,
                "task":task_view(t), "preview":preview_view(p) if p else None,
                "action":action_view(a) if a else None,
                "actions":{x.id:action_view(x) for x in db.scalars(select(Action).join(Task,Task.id==Action.task_id).where(Task.conversation_id==cid,Action.consumer_id==consumer))},
                "inflight":bool(c.inflight_id and c.inflight_until and c.inflight_until > now()),
                "messages":[{"id":m.id,"task_id":m.task_id,"role":m.role,"text":consumer_message(m),"cards":m.cards,"created_at":m.created_at.isoformat()} for m in messages]}

def consumer_message(message):
    # Preserve historic records; normalize old system-failure replies only at the read boundary.
    if message.role=="assistant" and any(c.get("type")=="help_offer" for c in message.cards):
        if re.match(r"^(模型服务|模型未返回|向量服务|重排服务|本轮处理|本轮模型|模型预算|AI 服务当前已停用|AI 服务在等待期间)",message.text):
            return PUBLIC_MESSAGE
    return message.text

def list_orders(consumer):
    with SessionLocal() as db:
        return [order_view(o) for o in db.scalars(select(Order).where(Order.consumer_id == consumer).order_by(Order.id))]

def bind_order(consumer, cid, oid, interaction_id):
    with SessionLocal.begin() as db:
        c = conversation(db, consumer, cid, True)
        o = order_scope(db, consumer, oid, c)
        previous = db.get(Interaction, interaction_id)
        if previous:
            if previous.conversation_id != cid:
                reject_scope("interaction")
            return
        db.add(Interaction(id=interaction_id, conversation_id=cid, kind="bind"))
        old = c.order_id
        if old != oid:
            t = current_task(db, c)
            invalidate(db, t)
            # Changing the object cancels the old task, except an unbound clarification.
            if t and t.order_id:
                change_status(db, c, t, "cancelled", reason="object_switched")
                t.active = False
                emit(db, "task_cancelled", c, t, reason="object_switched")
            elif t:
                t.order_id = oid
                change_status(db, c, t, "processing")
            c.order_id = oid
            c.state_version += 1
            c.inflight_id = None
            emit(db, "context_changed", c, t, old_object_ref=old, new_object_ref=oid)
        emit(db, "object_bound", c, current_task(db,c), object_ref=oid, binding_result="allowed")
        emit(db, "user_interaction_received", c, interaction_id=interaction_id, interaction_type="bind")

def policy(db, o):
    items = db.scalars(select(Knowledge).where(Knowledge.merchant_id==o.merchant_id,
                      Knowledge.sku==o.sku, Knowledge.kind=="policy", Knowledge.enabled.is_(True),
                      Knowledge.consumer_visible.is_(True), Knowledge.valid_from<=now(), Knowledge.valid_until>now())).all()
    if not items:
        raise DomainError("evidence_missing", "当前没有有效且适用的退货规则，暂时无法判断或准备申请。")
    rules = {json.dumps(k.rules, sort_keys=True) for k in items}
    versions = {k.policy_version for k in items}
    if len(rules)!=1 or len(versions)!=1:
        raise DomainError("evidence_conflict", "当前适用规则存在冲突，暂时不能继续判断或办理。")
    return items[0]

def eligibility(o, k):
    if o.status != "delivered":
        return False, "这笔订单尚未签收，不符合本演示退货申请的订单状态条件。"
    days = k.rules["return_days"]
    if o.delivery_days is None or o.delivery_days < 0:
        raise DomainError("facts_missing","缺少可信签收时间，暂时无法判断。")
    if o.delivery_days > days:
        return False, f"按合成业务基准日，这笔订单已签收 {o.delivery_days} 天，超出本店 {days} 天的申请期限。"
    return True, f"按合成业务基准日，这笔订单已签收 {o.delivery_days} 天，处于本店 {days} 天的申请期限内。申请还需声明商品完好未使用，并由你确认提交；这不是审核通过或退款承诺。"

def enforce_runtime(o=None, write=False):
    cfg=config()
    if not cfg["ai_enabled"] or (o and o.merchant_id in cfg["disabled_merchants"]) or (write and not cfg["writes_enabled"]):
        raise DomainError("scope_disabled", "当前范围的自动办理已暂停；已有申请仍可查询。")
    from .llm import SessionLocal as Ledger, cost_total
    with Ledger() as ledger:
        if cost_total(ledger)>=cfg["total_credits"]:
            raise DomainError("credit_budget","本轮开发模型预算已用尽，暂不能新增自动办理；仍可查询原申请。",503)
    return cfg

def binding_fingerprint(o,k):
    facts={"consumer":o.consumer_id,"merchant":o.merchant_id,"sku":o.sku,"status":o.status,
           "delivery_days":o.delivery_days,"version":o.version,"amount":o.price_cents,
           "product":o.product,"spec":o.spec,"policy":k.policy_version,"rules":k.rules,
           "policy_content":k.content,"valid_from":k.valid_from.isoformat(),"valid_until":k.valid_until.isoformat()}
    return hashlib.sha256(json.dumps(facts,sort_keys=True,ensure_ascii=False).encode()).hexdigest()

def make_preview(consumer, cid, fields, expected_version=None):
    with SessionLocal.begin() as db:
        c = conversation(db, consumer, cid, True)
        if c.automation_state!="active":
            raise DomainError("ai_paused","AI 已暂停，请明确选择继续 AI 后再办理。")
        if expected_version is not None and c.state_version!=expected_version:
            raise DomainError("context_changed","当前事项已变化，请重新确认。")
        t = current_task(db,c)
        if not t or t.goal!="application" or not c.order_id:
            raise DomainError("task_required","请先选择订单并发起退货申请。")
        o=order_scope(db,consumer,c.order_id,c)
        enforce_runtime(o,True)
        if db.get(Action,t.action_id):
            raise DomainError("action_exists","该动作已经派发，请查询原申请。")
        k=policy(db,o)
        allowed, explanation=eligibility(o,k)
        if not allowed:
            raise DomainError("ineligible",explanation)
        data={**t.fields,**fields}
        reason=str(data.get("reason","")).strip()
        if not 2<=len(reason)<=300 or data.get("condition")!="unused_intact":
            raise DomainError("missing_fields","请填写退货原因，并确认商品完好、未使用。",422)
        invalidate(db,t)
        t.fields={"reason":reason,"condition":"unused_intact"}
        c.state_version+=1
        content={"order_id":o.id,"merchant":o.merchant_name,"product":o.product,"spec":o.spec,
                 "_binding":binding_fingerprint(o,k),"type":"标准退货售后申请","quantity":1,"reason":reason,"condition":"商品完好、未使用",
                 "order_amount_cents":o.price_cents,"notice":"仅提交申请，不执行退款；受理不等于审核通过或资金到账。"}
        digest=hashlib.sha256(json.dumps(content,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
        p=Preview(id=uid(),task_id=t.id,consumer_id=consumer,order_id=o.id,action_id=t.action_id,
                  content=content,content_hash=digest,policy_version=k.policy_version,fact_version=o.version,
                  control_version=c.control_version,context_version=c.state_version,
                  expires_at=now()+timedelta(seconds=config()["preview_seconds"]))
        db.add(p)
        change_status(db,c,t,"awaiting_confirmation")
        emit(db,"application_preview_created",c,t,preview_id=p.id,content_hash=digest,policy_version=k.policy_version,expires_at=p.expires_at.isoformat())
        m=add_message(db,c,t,"申请已经准备好，请核对后再确认。现在还没有提交。",[{"type":"preview","preview_id":p.id}])
        return preview_view(p)

def control(consumer,cid,mode,interaction_id):
    if mode not in ("paused","active"):
        raise DomainError("invalid_control","无效操作",422)
    with SessionLocal.begin() as db:
        c=conversation(db,consumer,cid,True)
        previous=db.get(Interaction,interaction_id)
        if previous:
            if previous.conversation_id!=cid: reject_scope("interaction")
            return
        db.add(Interaction(id=interaction_id,conversation_id=cid,kind="control"))
        t=current_task(db,c,True)
        if mode=="paused":
            emit(db,"human_requested",c,t,origin="consumer")
        if c.automation_state!=mode:
            c.automation_state=mode
            c.control_version+=1
            c.state_version+=1
            c.inflight_id=None
            invalidate(db,t)
            if t and mode=="paused":
                t.human_involvement="unknown"
            emit(db,"automation_control_changed",c,t,automation_state=mode,control_version=c.control_version)
            add_message(db,c,t,"AI 已暂停。你可以联系客服，或复制服务摘要。" if mode=="paused" else "已恢复 AI 服务。",
                        [{"type":"help"}] if mode=="paused" else [])
        emit(db,"user_interaction_received",c,t,interaction_id=interaction_id,interaction_type="control")

def cancel_task(consumer,cid):
    with SessionLocal.begin() as db:
        c=conversation(db,consumer,cid,True)
        t=current_task(db,c)
        if not t: return
        invalidate(db,t)
        c.state_version+=1
        c.inflight_id=None
        a=db.get(Action,t.action_id)
        if a:
            add_message(db,c,t,"申请已派发，结束当前交互不等于撤销申请。请查询原动作结果。",[{"type":"result","action_id":a.id}])
            return
        change_status(db,c,t,"cancelled")
        t.active=False
        emit(db,"task_cancelled",c,t,reason="consumer_cancel")
        add_message(db,c,t,"未提交的申请已取消。你可以选择其他事项。")

def confirm(consumer,cid,pid,content_hash,confirmation_id,fault=None):
    # fault is test-only Python injection, never accepted from HTTP inputs.
    with SessionLocal.begin() as db:
        c=conversation(db,consumer,cid,True)
        p=db.get(Preview,pid)
        if not p or p.consumer_id!=consumer:
            reject_scope("preview")
        t=db.get(Task,p.task_id)
        if not t or t.conversation_id!=cid:
            reject_scope("preview")
        existing=db.get(Action,p.action_id)
        if existing:
            existing.attempt_count+=1
            emit(db,"confirmation_validated",c,t,confirmation_id=confirmation_id,preview_id=pid,
                 validity="duplicate_original_action",action_id=existing.id)
            return action_view(existing)
        o=order_scope(db,consumer,p.order_id,c)
        enforce_runtime(o,True)
        valid=(t.active and t.goal=="application" and c.automation_state=="active" and not p.invalidated
               and p.expires_at>now() and c.order_id==p.order_id and t.order_id==p.order_id
               and p.content_hash==content_hash and p.control_version==c.control_version
               and p.context_version==c.state_version and p.fact_version==o.version)
        if not valid:
            raise DomainError("stale_confirmation","此预览已失效或当前事项已变化，请重新生成并确认。")
        k=policy(db,o)
        allowed,_=eligibility(o,k)
        if not allowed or p.content.get("_binding")!=binding_fingerprint(o,k) or k.policy_version!=p.policy_version or p.content["order_amount_cents"]!=o.price_cents or p.content["product"]!=o.product or p.content["spec"]!=o.spec:
            raise DomainError("facts_changed","适用规则或业务事实已变化，请重新核对预览。")
        if db.scalar(select(Application).where(Application.order_id==o.id)):
            raise DomainError("application_exists","该订单明细已有受理中的申请，请查询已有申请，不要重复创建。")
        emit(db,"confirmation_validated",c,t,confirmation_id=confirmation_id,preview_id=pid,validity="valid",action_id=p.action_id)
        emit(db,"action_dispatch_decided",c,t,action_id=p.action_id,confirmation_id=confirmation_id,
             control_version=c.control_version,decision="allow")
        emit(db,"tool_attempt_started",c,t,tool_name="submit_application",action_id=p.action_id,attempt_id=uid(),read_or_write="write")
        a=Action(id=p.action_id,task_id=t.id,consumer_id=consumer,order_id=o.id,preview_id=p.id,confirmation_id=confirmation_id)
        db.add(a)
        p.invalidated=True
        if fault=="not_created":
            a.status="not_created"
            change_status(db,c,t,"rejected")
            t.active=False
        else:
            app=Application(id="AS-"+uid(),action_id=a.id,consumer_id=consumer,order_id=o.id,content=p.content)
            db.add(app)
            if fault in ("lost_ack","query_unavailable"):
                a.status="unknown"
                a.reconcile_started_at=now()
                change_status(db,c,t,"unknown")
            else:
                a.status="accepted"
                a.application_id=app.id
        emit(db,"tool_attempt_finished",c,t,tool_name="submit_application",action_id=a.id,status=a.status,read_or_write="write")
        emit(db,"business_result_observed",c,t,producer="business_adapter",action_id=a.id,business_status=a.status,application_ref=a.application_id)
        text = "申请已受理，等待业务处理。审核与后续结果以业务状态为准。" if a.status=="accepted" else "暂未确认是否受理，请先查询原申请，避免重复提交。" if a.status=="unknown" else "本次申请明确未创建。你可以查看原因并寻求帮助。"
        m=add_message(db,c,t,text,[{"type":"result","action_id":a.id}])
        if a.status=="accepted":
            claim(db,c,t,m,"application_accepted",application_ref=a.application_id,action_id=a.id)
        db.flush()
        return action_view(a)

def reconcile(consumer,cid,aid,automatic=False,unavailable=False):
    with SessionLocal.begin() as db:
        c=conversation(db,consumer,cid,True)
        a=db.get(Action,aid)
        if not a or a.consumer_id!=consumer:
            reject_scope("action")
        t=db.get(Task,a.task_id)
        if t.conversation_id!=cid: reject_scope("action")
        order_scope(db,consumer,a.order_id,c)
        if a.status!="unknown": return action_view(a)
        cfg=config()
        if automatic and (a.reconcile_count>=cfg["reconcile_limit"] or not a.reconcile_started_at
                          or (now()-a.reconcile_started_at).total_seconds()>cfg["reconcile_seconds"]):
            return action_view(a)
        a.reconcile_count+=1
        emit(db,"reconciliation_started",c,t,action_id=a.id,trigger="automatic" if automatic else "consumer",count=a.reconcile_count)
        if unavailable:
            emit(db,"reconciliation_finished",c,t,action_id=a.id,result="unknown")
            return action_view(a)
        business=db.scalar(select(Application).where(Application.action_id==aid,Application.consumer_id==consumer))
        if business:
            a.status="accepted"
            a.application_id=business.id
            emit(db,"business_result_observed",c,t,producer="business_adapter",action_id=a.id,
                 application_ref=business.id,business_status="accepted",source_updated_at=business.created_at.isoformat())
            m=add_message(db,c,t,"已查到原申请：申请已受理，等待业务处理。这不代表审核通过或退款到账。",[{"type":"result","action_id":a.id}])
            claim(db,c,t,m,"application_accepted",application_ref=business.id,action_id=a.id)
        # Absence is not authoritative evidence of not_created.
        emit(db,"reconciliation_finished",c,t,action_id=a.id,result=a.status)
        return action_view(a)

def help_info(consumer,cid):
    with SessionLocal.begin() as db:
        c=conversation(db,consumer,cid)
        t=current_task(db,c,True)
        o=order_scope(db,consumer,c.order_id,c) if c.order_id else None
        entry=config()["help_entries"].get(o.merchant_id if o else "") or public_help()["entry"]
        lines=["YxWoof 服务摘要",f"AI 状态：{'已暂停' if c.automation_state=='paused' else '服务中'}"]
        if o: lines += [f"商户：{o.merchant_name}",f"订单：{o.id}",f"商品：{o.product} / {o.spec}"]
        if t:
            lines += [f"本次约定目标：{GOALS.get(t.goal,'待明确')}",f"当前状态：{t.status}"]
            a=db.get(Action,t.action_id)
            if a: lines += [f"原动作：{a.id}",f"受理状态：{a.status}",f"申请编号：{a.application_id or '尚未核实'}"]
        return {"availability":"demo_public" if entry.get("virtual") else "configured","entry":entry,"summary":"\n".join(lines),"pause_confirmed":c.automation_state=="paused"}

def save_feedback(consumer,cid,data,fail=False):
    with SessionLocal.begin() as db:
        c=conversation(db,consumer,cid)
        t=db.get(Task,data["task_id"])
        if not t or t.consumer_id!=consumer or t.conversation_id!=cid: reject_scope("feedback")
        previous=db.get(Feedback,data["id"])
        if previous:
            if previous.consumer_id!=consumer or previous.task_id!=t.id: reject_scope("feedback")
            return {"saved":True,"id":previous.id}
        if fail: raise DomainError("feedback_unavailable","反馈暂时未保存，请保留内容后重试。",503)
        f=Feedback(consumer_id=consumer,**data)
        db.add(f)
        emit(db,"feedback_submitted",c,t,feedback_id=f.id,helpful=f.helpful,category=f.category,
             rating=f.csat,ces=f.ces,question_version="feedback-1")
        return {"saved":True,"id":f.id}
