"""Free scheduled analysis, isolated human training, and reviewed regression artifacts."""
import asyncio
import json
import re
from datetime import timedelta
from sqlalchemy import select, text, or_, func
from ..db import now, uid
from ..domain import DomainError
from ..models import Task, Message, Feedback, Order
from . import db as database, temporal, retrieval
from .models import (AnalysisBatch, TrainingItem, RegressionCase, ReviewRecord, KnowledgeVersion, Turn,
                     DatasetVersion, Improvement, QueryVector, GenerationAttempt)
from .settings import settings

CATEGORIES = {"understanding", "retrieval", "rule_applicability", "generation", "workflow", "dependency", "business_expectation"}


def normalized(value):
    value = re.sub(r"[A-Za-z]{2,}(?:-[A-Za-z0-9]+)+", "[订单]", value)
    value = re.sub(r"\d{7,}", "[编号]", value)
    return re.sub(r"[\s，。！？、,.!?]", "", value.lower())[:500]


def deidentified(value):
    serialized = json.dumps(value, ensure_ascii=False)
    # Heuristics supplement mandatory human deidentification review, never claim full anonymization.
    return not re.search(r"(?<!\d)1[3-9]\d{9}(?!\d)|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|\bYX-[A-Z]{2}-\d+\b|\d{17}[\dXx]", serialized)


def review(db, target_type, target_id, actor, action, detail):
    row = ReviewRecord(id=uid(), target_type=target_type, target_id=target_id,
                      reviewer=actor["username"], action=action, detail=detail)
    db.add(row)
    return row


async def transcript(cid):
    async with database.Session() as db:
        tasks = (await db.scalars(select(Task).where(Task.conversation_id == cid).order_by(Task.created_at))).all()
        messages = (await db.scalars(select(Message).where(Message.conversation_id == cid).order_by(Message.created_at))).all()
        attempts = (await db.scalars(select(GenerationAttempt).join(Turn, Turn.id == GenerationAttempt.turn_id)
                                  .where(Turn.conversation_id == cid).order_by(GenerationAttempt.created_at))).all()
        return {"conversation_id": cid, "tasks": [{"id": t.id, "goal": t.goal, "question": t.requested_goal,
            "order_id": t.order_id, "status": t.status, "context": t.context, "result": t.result} for t in tasks],
            "messages": [{"id": m.id, "task_id": m.task_id, "role": m.role, "text": m.text, "evidence": m.evidence,
                "display_state": m.display_state, "delivery_state": m.delivery_state, "created_at": m.created_at.isoformat()} for m in messages],
            "diagnostics": [{"id": a.id, "kind": a.kind, "disposition": a.disposition, "reason": a.reason,
                "candidate": a.candidate, "evidence": a.evidence} for a in attempts]}


async def analyze(kind, period_end, *, refresh=False, after_chunk=None):
    """Session advisory lock spans small committed checkpoints, with zero paid model calls."""
    if kind not in ("daily", "weekly"): raise ValueError("Invalid report kind")
    start = period_end - timedelta(days=settings()["analysis_days"])
    batch_id = kind + "-" + period_end.astimezone(temporal.BUSINESS_TZ).strftime("%Y%m%d")
    connection = await database.engine.connect()
    connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
    locked = False
    try:
        locked = bool(await connection.scalar(text("SELECT pg_try_advisory_lock(87800023)")))
        if not locked: return None
        async with database.Session.begin() as db:
            batch = await db.get(AnalysisBatch, batch_id)
            if batch and batch.report.get("status") == "completed" and not refresh: return batch_id
            if not batch:
                batch = AnalysisBatch(id=batch_id, kind=kind, period_start=start, period_end=period_end,
                    report={"status": "running", "processed": [], "groups": [], "generation": 1})
                db.add(batch)
            elif refresh:
                batch.report = {"status": "running", "processed": [], "groups": [], "generation": batch.report.get("generation", 1) + 1}
            await db.flush()
            progress = dict(batch.report)
        async with database.Session() as db:
            # Include older tasks with feedback arriving in the window. Latest rating remains authoritative.
            late = select(Feedback.task_id).where(Feedback.created_at >= start, Feedback.created_at <= period_end)
            tasks = (await db.scalars(select(Task).where(Task.api_version == 2,
                or_((Task.created_at >= start) & (Task.created_at <= period_end), Task.id.in_(late)))
                .order_by(Task.created_at, Task.id))).all()
            ids = [t.id for t in tasks]
            orders = {o.id: o for o in (await db.scalars(select(Order).where(Order.id.in_([t.order_id for t in tasks if t.order_id])))).all()}
            messages = (await db.scalars(select(Message).where(Message.task_id.in_(ids), Message.role == "assistant",
                        Message.display_state == "visible", Message.created_at <= period_end).order_by(Message.created_at, Message.id))).all()
            ratings = (await db.scalars(select(Feedback).where(Feedback.task_id.in_(ids), Feedback.created_at <= period_end)
                                       .order_by(Feedback.created_at, Feedback.id))).all()
            latest = {r.message_id or "task:" + r.task_id: r for r in ratings}
            vectors = {v.id: list(v.embedding) for v in (await db.scalars(select(QueryVector))).all()}
        by_task = {}
        for m in messages: by_task.setdefault(m.task_id, []).append(m)
        processed = set(progress["processed"]); groups = progress["groups"]
        for offset in range(0, len(tasks), 100):
            for t in tasks[offset:offset + 100]:
                if t.id in processed: continue
                o = orders.get(t.order_id); replies = by_task.get(t.id, [])
                source_ids = sorted({key for m in replies for key in m.evidence.get("source_ids", [])})
                scope = {"merchant_id": o.merchant_id if o else None, "sku": o.sku if o else None,
                         "intent": t.goal, "source_ids": source_ids}
                key = temporal.digest(scope)
                query = t.context.get("raw_input", t.requested_goal)
                vector_id = retrieval.query_key(query)
                vector = vectors.get(vector_id); norm = normalized(t.requested_goal)
                group = next((g for g in groups if g["scope_key"] == key and (g["normalized"] == norm or
                    (vector is not None and g.get("vector_id") in vectors and
                     retrieval.cosine(vector, vectors[g["vector_id"]]) >= .92))), None)
                if not group:
                    group = {"id": uid(), "scope_key": key, "scope": scope, "normalized": norm,
                             "vector_id": vector_id if vector is not None else None, "question": t.requested_goal,
                             "task_ids": [], "conversation_ids": [], "message_ids": [], "rated": 0, "negative": 0,
                             "replies": 0, "incomplete": 0, "fallback": 0, "reason_candidates": []}
                    groups.append(group)
                group["task_ids"].append(t.id); group["conversation_ids"] = sorted(set(group["conversation_ids"] + [t.conversation_id]))
                group["message_ids"].extend(m.id for m in replies)
                group["replies"] += len(replies)
                valid_ratings = [latest[m.id] for m in replies if m.id in latest]
                group["rated"] += len(valid_ratings); group["negative"] += sum(not r.helpful for r in valid_ratings)
                group["incomplete"] += int(t.status not in ("completed", "cancelled"))
                group["fallback"] += sum(bool(m.evidence.get("failure_code")) or m.text.startswith("现有依据不足") for m in replies)
                group["reason_candidates"] = sorted(set(group["reason_candidates"] + [r.category for r in valid_ratings if not r.helpful]))
                processed.add(t.id)
            progress.update(processed=sorted(processed), groups=groups, status="running", updated_at=now().isoformat())
            async with database.Session.begin() as db: (await db.get(AnalysisBatch, batch_id)).report = json.loads(json.dumps(progress))
            if after_chunk: await after_chunk(offset)
        for g in groups:
            g["frequency"] = len(g["task_ids"]); g["unique_conversations"] = len(g["conversation_ids"])
            g["feedback_coverage"] = g["rated"] / g["replies"] if g["replies"] else None
            g["repeat_followups"] = max(0, g["frequency"] - g["unique_conversations"])
        groups.sort(key=lambda g: (-g["negative"], -g["frequency"], g["id"]))
        progress.update(status="completed", groups=groups, completed_at=now().isoformat(), paid_model_calls=0,
                        note="原因仅为待核实候选；标准答案必须由审核人员补充，低分不等于答错。")
        async with database.Session.begin() as db: (await db.get(AnalysisBatch, batch_id)).report = progress
        return batch_id
    finally:
        try:
            if locked: await connection.execute(text("SELECT pg_advisory_unlock(87800023)"))
        finally: await connection.close()


async def stale_reason(db, item, at=None):
    at = at or now()
    if item.purpose != "current_guidance": return None
    if item.valid_from and at < item.valid_from: return "not_yet_valid"
    if item.valid_until and at >= item.valid_until: return "expired"
    if not item.review_due_at or at >= item.review_due_at: return "review_due"
    if not item.source_ids: return "evidence_missing"
    for source_id in item.source_ids:
        source = await db.get(KnowledgeVersion, source_id)
        if not source or source.status != "published" or not source.consumer_visible: return "source_unavailable"
        if (source.merchant_id, source.sku) != (item.scope.get("merchant_id"), item.scope.get("sku")): return "source_scope_changed"
        if not source.valid_from <= at < source.valid_until: return "source_outside_validity"
        # Withdrawing a replacement does not silently reinstate the older guidance.
        replacement = await db.scalar(select(KnowledgeVersion.id).where(KnowledgeVersion.supersedes_id == source.id,
            KnowledgeVersion.valid_from <= at))
        if replacement: return "source_replaced"
    return None


async def refresh_training(db):
    rows = (await db.scalars(select(TrainingItem).where(TrainingItem.status == "published").with_for_update())).all()
    for row in rows:
        reason = await stale_reason(db, row)
        if reason:
            row.status = "pending_review"; row.stale_reason = reason
            review(db, "training", row.id, {"username": "system-expiry-check"}, "suspended", {"reason": reason})


def training_view(row):
    return {"id": row.id, "family_id": row.family_id, "revision": row.revision, "batch_id": row.batch_id,
        "purpose": row.purpose, "status": row.status, "content": row.content, "scope": row.scope, "source_ids": row.source_ids,
        "valid_from": row.valid_from.isoformat() if row.valid_from else None,
        "valid_until": row.valid_until.isoformat() if row.valid_until else None,
        "review_due_at": row.review_due_at.isoformat() if row.review_due_at else None,
        "stale_reason": row.stale_reason, "created_at": row.created_at.isoformat()}


async def list_training(view="current"):
    async with database.Session.begin() as db:
        await refresh_training(db)
        rows = (await db.scalars(select(TrainingItem).order_by(TrainingItem.created_at.desc()))).all()
        if view == "current": rows = [r for r in rows if r.status == "published" and r.purpose == "current_guidance"]
        elif view == "history": rows = [r for r in rows if r.purpose == "historical_case" and r.status == "published"]
        elif view == "invalid": rows = [r for r in rows if r.status in ("pending_review", "archived")]
        return [training_view(r) for r in rows]


async def create_training(data, actor, parent_id=None):
    async with database.Session.begin() as db:
        family, revision = uid(), 1
        if parent_id:
            parent = await db.get(TrainingItem, parent_id, with_for_update=True)
            if not parent: raise DomainError("not_found", "材料不存在。", 404)
            family = parent.family_id
            revision = 1 + (await db.scalar(select(func.max(TrainingItem.revision)).where(TrainingItem.family_id == family)))
        row = TrainingItem(id=uid(), family_id=family, revision=revision, **data)
        db.add(row); await db.flush()
        review(db, "training", row.id, actor, "draft_created", {"parent_id": parent_id})
        return training_view(row)


async def review_training(item_id, action, actor, note):
    async with database.Session.begin() as db:
        row = await db.get(TrainingItem, item_id, with_for_update=True)
        if not row: raise DomainError("not_found", "材料不存在。", 404)
        if action == "publish":
            if row.status != "draft": raise DomainError("revision_required", "已审核材料需要新建修订版本再发布。")
            if not row.content.get("answer") and not row.content.get("steps"):
                raise DomainError("answer_required", "请补充并核实标准答案或操作步骤。", 422)
            if not row.content.get("deidentification_reviewed") or not deidentified(row.content):
                raise DomainError("deidentification_required", "请完成人工脱敏复核。", 422)
            row.valid_from = row.valid_from or now()
            row.review_due_at = now() + timedelta(days=settings()["review_days"])
            reason = await stale_reason(db, row)
            if reason: raise DomainError("invalid_training", "材料当前不可发布为现行指引：" + reason)
            # Publishing a new version retires its previous current version.
            previous = (await db.scalars(select(TrainingItem).where(TrainingItem.family_id == row.family_id,
                        TrainingItem.id != row.id, TrainingItem.status == "published").with_for_update())).all()
            for old in previous:
                old.status = "archived"; old.stale_reason = "revised"
                review(db, "training", old.id, actor, "archived", {"replacement": row.id})
            row.status = "published"; row.stale_reason = None
        elif action in ("reject", "archive"):
            row.status = "rejected" if action == "reject" else "archived"
        else: raise DomainError("invalid_action", "无效审核操作。", 422)
        review(db, "training", row.id, actor, action, {"note": note, "content_hash": temporal.digest(row.content)})
        return training_view(row)


async def export_training(item_id, historical=False):
    async with database.Session.begin() as db:
        await refresh_training(db)
        row = await db.get(TrainingItem, item_id)
        if not row or row.status != "published" or (not historical and row.purpose != "current_guidance"):
            raise DomainError("training_unavailable", "该材料不可作为现行培训材料导出。")
        return {"generated_at": now().isoformat(), "usage": "人工培训专用；不用于在线答复、检索或模型训练。",
                "notice": "已下载文件无法撤回，请使用失效版本清单定期替换。", "item": training_view(row)}


async def create_case(data, actor):
    async with database.Session.begin() as db:
        origin = await db.get(Message, data["origin_message_id"], with_for_update=True)
        if not origin or origin.role != "assistant": raise DomainError("not_found", "原回复不存在。", 404)
        revision = 1 + (await db.scalar(select(func.max(RegressionCase.revision)).where(
            RegressionCase.origin_message_id == origin.id)) or 0)
        row = RegressionCase(id=uid(), revision=revision, **data)
        db.add(row); await db.flush()
        review(db, "regression", row.id, actor, "draft_created", {"origin_message_id": origin.id})
        return row.id


async def review_case(case_id, action, actor, note):
    async with database.Session.begin() as db:
        row = await db.get(RegressionCase, case_id, with_for_update=True)
        if not row: raise DomainError("not_found", "案例不存在。", 404)
        if row.status != "draft": raise DomainError("revision_required", "已审核案例需要创建新版本。")
        if action == "approve":
            if row.category not in CATEGORIES or any(not row.content.get(k) for k in (
                    "input_context", "fact_snapshot", "expected_behavior", "forbidden_behavior", "deidentification_reviewed")):
                raise DomainError("case_incomplete", "请补齐输入上下文、事实快照、预期行为、禁止行为和脱敏审核。", 422)
            if not deidentified(row.content): raise DomainError("deidentification_required", "仍有需要脱敏的标识。", 422)
            row.status = "approved"
        elif action == "reject": row.status = "rejected"
        else: raise DomainError("invalid_action", "无效审核操作。", 422)
        review(db, "regression", row.id, actor, action, {"note": note, "content_hash": temporal.digest(row.content)})


async def freeze_dataset(case_ids, actor):
    async with database.Session.begin() as db:
        rows = (await db.scalars(select(RegressionCase).where(RegressionCase.id.in_(case_ids)))).all()
        if not case_ids or len(rows) != len(set(case_ids)) or any(r.status != "approved" for r in rows):
            raise DomainError("unreviewed_cases", "只能冻结已审核通过的回归案例。")
        cases = [{"id": r.id, "revision": r.revision, "category": r.category, "content": r.content} for r in sorted(rows, key=lambda r: r.id)]
        row = DatasetVersion(id=uid(), content_hash=temporal.digest(cases), cases=cases)
        db.add(row); await db.flush()
        review(db, "dataset", row.id, actor, "frozen", {"content_hash": row.content_hash, "case_ids": case_ids})
        return {"id": row.id, "content_hash": row.content_hash, "cases": row.cases}


async def scheduled(at=None):
    local = (at or now()).astimezone(temporal.BUSINESS_TZ)
    cutoff = local.replace(hour=2, minute=0, second=0, microsecond=0)
    if local < cutoff: cutoff -= timedelta(days=1)
    await analyze("daily", cutoff)
    # The latest Monday is recoverable after a missed start/restart.
    monday = cutoff - timedelta(days=cutoff.weekday())
    await analyze("weekly", monday)
    async with database.Session.begin() as db: await refresh_training(db)
