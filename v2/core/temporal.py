"""One temporal resolver for retrieval, eligibility, previews and confirmations."""
import hashlib
import json
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from ..db import now, uid
from ..domain import DomainError
from ..models import Knowledge, Order
from .models import KnowledgeVersion, PolicySnapshot, OrderEntitlement

BUSINESS_TZ = ZoneInfo("Asia/Shanghai")

def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), default=str).encode()).hexdigest()

def window(order, basis, at):
    if basis == "application": return at, at, "instant"
    value = {"ordered": order.ordered_at, "paid": order.paid_at,
             "delivered": order.delivered_at}.get(basis)
    if value: return value, value, "instant"
    if basis == "ordered":
        try:
            day = datetime.fromisoformat(order.ordered_date).date()
            start = datetime.combine(day, time.min, BUSINESS_TZ).astimezone(timezone.utc)
            return start, start + timedelta(days=1), "day"
        except (TypeError, ValueError): pass
    raise DomainError("rule_time_missing", "缺少判断这条规则所需的订单时间，暂时无法确认适用版本。")

def overlaps(version, start, end, precision):
    if precision == "instant": return version.valid_from <= start < version.valid_until
    return version.valid_from < end and version.valid_until > start

def covers(version, start, end, precision):
    if precision == "instant": return version.valid_from <= start < version.valid_until
    return version.valid_from <= start and version.valid_until >= end

def member(version, order, informational=False):
    if informational: return True
    conditions = version.applicability or {}
    entitlements = order.entitlements or {}
    activity = conditions.get("activity_id")
    if activity:
        if "activities" not in entitlements:
            raise DomainError("activity_membership_unknown", "缺少这笔订单是否参与活动的记录，不能按活动规则判断权益。")
        if activity not in entitlements["activities"]: return False
    for key, expected in conditions.get("facts", {}).items():
        if key not in entitlements.get("facts", {}):
            raise DomainError("rule_condition_missing", "缺少适用规则要求的订单资格记录，请先核实。")
        if entitlements["facts"][key] != expected: return False
    return True

def choose_versions(versions, order, scope="order", at=None):
    """Date precision is an interval, never an invented midnight purchase time."""
    at = at or now()
    groups = {}
    precisions = {}
    for v in versions:
        if not v.consumer_visible: continue
        start, end, precision = (at, at, "instant") if scope == "current" else window(order, v.time_basis, at)
        if not overlaps(v, start, end, precision): continue
        if not member(v, order, scope == "current"): continue
        if not covers(v, start, end, precision):
            raise DomainError("rule_date_ambiguous", "订单仅有日期，而当天规则发生变化，暂时无法确定适用版本。")
        groups.setdefault(v.document_id, []).append(v)
        precisions[v.id] = precision
    selected = []
    for candidates in groups.values():
        # An explicitly superseding version wins only within its effective interval.
        by_id = {v.id: v for v in candidates}
        replaced = set()
        for v in candidates:
            parent = v.supersedes_id
            seen = {v.id}
            while parent in by_id:
                if parent in seen:
                    raise DomainError("rule_version_cycle", "资料版本关系存在冲突，暂时无法可靠判断。")
                seen.add(parent); replaced.add(parent); parent = by_id[parent].supersedes_id
        winners = [v for v in candidates if v.id not in replaced]
        if len(winners) != 1:
            raise DomainError("rule_version_conflict", "适用时段存在多个未明确替代关系的规则版本，暂时无法判断。")
        winner = winners[0]
        if winner.status != "published":
            raise DomainError("rule_revoked", "适用资料已撤回，不能继续据此判断；需要核实替代依据。")
        selected.append(winner)
    return sorted(selected, key=lambda v: (v.document_id, v.revision)), precisions

async def resolve_versions(db, order, scope="order", at=None):
    if scope not in ("order", "current"):
        raise DomainError("invalid_time_scope", "无效的规则查询范围。", 422)
    versions = list((await db.scalars(select(KnowledgeVersion).where(
        KnowledgeVersion.merchant_id == order.merchant_id,
        KnowledgeVersion.sku == order.sku))).all())
    selected, precision = [], {}
    fixed_facts = order_facts(order)
    fixed_hash = digest(fixed_facts)
    binding = await db.scalar(select(OrderEntitlement).where(OrderEntitlement.order_id == order.id,
                    OrderEntitlement.fingerprint == fixed_hash)) if scope == "order" else None
    explicit_ids = (order.entitlements or {}).get("rule_version_ids") if scope == "order" else None
    pinned_ids = binding.version_ids if binding else explicit_ids
    if pinned_ids is not None:
        pinned = [v for v in versions if v.id in pinned_ids and v.time_basis != "application"]
        if len(pinned) != len(set(pinned_ids)):
            raise DomainError("snapshot_scope_invalid", "订单权益快照引用了缺失或不适用的规则版本，需人工核实。")
        selected, precision = choose_versions(pinned, order, scope, at)
        if {v.id for v in selected} != set(pinned_ids):
            raise DomainError("snapshot_conditions_changed", "订单权益快照与订单条件不一致，不能继续自动判断。")
        dynamic, dynamic_precision = choose_versions([v for v in versions if v.time_basis == "application"], order, scope, at)
        selected += dynamic; precision.update(dynamic_precision)
    else:
        selected, precision = choose_versions(versions, order, scope, at)
    if not selected:
        raise DomainError("evidence_missing", "当前缺少该订单适用时段的有效资料，不能使用其他时段规则代替。")
    if scope == "order" and not binding:
        await db.execute(insert(OrderEntitlement).values(id=uid(), order_id=order.id, fingerprint=fixed_hash,
            version_ids=[v.id for v in selected if v.time_basis != "application"], facts=fixed_facts,
            created_at=now()).on_conflict_do_nothing(index_elements=["order_id", "fingerprint"]))
    return selected, precision

def merge_rules(versions):
    policies = [v for v in versions if v.kind == "policy"]
    if not policies:
        raise DomainError("evidence_missing", "缺少这笔订单的适用退货规则，暂时无法判断或准备申请。")
    graph = {v.id: set(v.overrides) for v in policies}
    def visit(key, path):
        if key in path: raise DomainError("rule_override_cycle", "规则覆盖关系存在循环，暂时无法判断。")
        for parent in graph.get(key, set()): visit(parent, path | {key})
    for key in graph: visit(key, set())
    values, owners = {}, {}
    for v in policies:
        for field, value in v.rules.items():
            previous = owners.get(field)
            if previous and values[field] != value:
                if previous.id in v.overrides:
                    pass
                elif v.id in previous.overrides:
                    continue
                else:
                    raise DomainError("rule_override_missing", "通用规则与活动规则存在冲突，且没有明确覆盖关系，暂时无法判断。")
            values[field], owners[field] = value, v
    if not isinstance(values.get("return_days"), int) or values["return_days"] < 0:
        raise DomainError("rule_invalid", "规则缺少有效的退货时限，暂时无法判断。")
    return values, policies

def order_facts(order):
    return {"order_id": order.id, "consumer_id": order.consumer_id, "merchant_id": order.merchant_id,
            "sku": order.sku, "ordered_date": order.ordered_date,
            "ordered_at": order.ordered_at.isoformat() if order.ordered_at else None,
            "paid_at": order.paid_at.isoformat() if order.paid_at else None,
            "delivered_at": order.delivered_at.isoformat() if order.delivered_at else None,
            "entitlements": order.entitlements or {}}

async def policy_snapshot(db, order, at=None):
    versions, precision = await resolve_versions(db, order, at=at)
    rules, policies = merge_rules(versions)
    facts = {**order_facts(order), "time_precision": {v.id: precision[v.id] for v in policies}}
    ids = sorted(v.id for v in policies)
    fingerprint = digest({"facts": facts, "versions": ids, "rules": rules})
    key = uid()
    await db.execute(insert(PolicySnapshot).values(id=key, order_id=order.id,
        fingerprint=fingerprint, version_ids=ids, facts=facts, resolved_rules=rules,
        created_at=now()).on_conflict_do_nothing(index_elements=["order_id", "fingerprint"]))
    snapshot = await db.scalar(select(PolicySnapshot).where(PolicySnapshot.order_id == order.id,
                                                          PolicySnapshot.fingerprint == fingerprint))
    return snapshot, policies

def eligible(order, rules):
    if order.status != "delivered": return False, "订单尚未签收，不符合本演示退货申请的订单状态条件。"
    delivery_days = order.delivery_days
    if order.delivered_at:
        elapsed = now() - order.delivered_at
        delivery_days = elapsed.days
    if delivery_days is None or delivery_days < 0:
        raise DomainError("order_fact_missing", "缺少可核实的签收时间信息，暂时无法判断退货时限。")
    days = rules["return_days"]
    if delivery_days > days:
        return False, f"这笔订单已签收 {delivery_days} 天，超过适用规则的 {days} 天退货申请期限。"
    return True, f"这笔订单已签收 {delivery_days} 天，处于适用规则的 {days} 天退货申请期限内；还需确认商品完好、未使用。"


async def publish_version(db, data):
    """Operator-only additive authoring; embeddings are a separate, explicit paid step."""
    data = dict(data)
    allowed = {"id", "document_id", "revision", "merchant_id", "sku", "title", "kind", "content", "rules",
               "applicability", "overrides", "time_basis", "policy_version", "valid_from", "valid_until", "supersedes_id", "consumer_visible"}
    if set(data) - allowed or data.get("time_basis", "ordered") not in ("ordered", "paid", "delivered", "application"):
        raise DomainError("invalid_version", "规则版本字段或时间基准无效。", 422)
    if any(not isinstance(data.get(key), str) or not data[key].strip()
           for key in ("document_id", "merchant_id", "sku", "title", "kind", "content")):
        raise DomainError("invalid_version", "规则需要文档编号、商户、商品、标题、类型与内容。", 422)
    for name in ("valid_from", "valid_until"):
        value = data.get(name)
        if isinstance(value, str): value = datetime.fromisoformat(value)
        if not isinstance(value, datetime) or value.tzinfo is None: raise DomainError("invalid_version", "规则需要完整且带时区的有效期。", 422)
        data[name] = value
    if data["valid_from"] >= data["valid_until"] or not data.get("content"):
        raise DomainError("invalid_version", "规则内容或有效期无效。", 422)
    existing = (await db.scalars(select(KnowledgeVersion).where(KnowledgeVersion.document_id == data["document_id"]).with_for_update())).all()
    revision = 1 + max((v.revision for v in existing), default=0)
    if data.get("revision", revision) != revision: raise DomainError("revision_conflict", "版本号必须递增。")
    if existing:
        parent = next((v for v in existing if v.id == data.get("supersedes_id")), None)
        if not parent or (parent.merchant_id, parent.sku, parent.kind, parent.time_basis) != (
                data["merchant_id"], data["sku"], data["kind"], data.get("time_basis", "ordered")):
            raise DomainError("version_scope_conflict", "替代关系必须属于相同文档、商户、商品和时间基准。")
    elif data.get("supersedes_id"): raise DomainError("version_scope_conflict", "首个版本不能替代其他文档。")
    data.setdefault("policy_version", data["document_id"] + "@" + str(revision))
    data.update(id=data.get("id", uid()), revision=revision, content_hash=hashlib.sha256(data["content"].encode()).hexdigest())
    row = KnowledgeVersion(**data); db.add(row); await db.flush()
    return version_view(row)

def version_view(v):
    return {"id": v.id, "document_id": v.document_id, "revision": v.revision,
            "title": v.title, "kind": v.kind, "content": v.content,
            "policy_version": v.policy_version, "content_hash": v.content_hash,
            "merchant_id": v.merchant_id, "sku": v.sku, "status": v.status,
            "valid_from": v.valid_from.isoformat(), "valid_until": v.valid_until.isoformat(),
            "time_basis": v.time_basis, "recorded_at": v.recorded_at.isoformat()}

async def import_legacy(db):
    """Explicit migration command only. Never silently synchronize mutable v1 content."""
    count = 0
    for k in (await db.scalars(select(Knowledge).order_by(Knowledge.id))).all():
        exists = await db.scalar(select(KnowledgeVersion.id).where(KnowledgeVersion.document_id == k.id))
        if exists: continue
        db.add(KnowledgeVersion(id=uid(), document_id=k.id, revision=1,
            merchant_id=k.merchant_id, sku=k.sku, title=k.title, kind=k.kind,
            content=k.content, rules=k.rules, applicability={}, overrides=[], time_basis="ordered",
            policy_version=k.policy_version, valid_from=k.valid_from, valid_until=k.valid_until,
            status="published" if k.enabled else "revoked", consumer_visible=k.consumer_visible,
            content_hash=k.content_hash, embedding=k.embedding))
        count += 1
    await db.flush()
    return count
