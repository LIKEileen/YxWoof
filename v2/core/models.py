"""Versioned evidence and review records; legacy business identities are retained."""
from sqlalchemy import Column, String, Text, Integer, Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector
from ..db import Base, now, uid


class KnowledgeVersion(Base):
    __tablename__ = "v2_knowledge_versions"
    id = Column(String, primary_key=True, default=uid)
    document_id = Column(String, nullable=False, index=True)
    revision = Column(Integer, nullable=False)
    merchant_id = Column(String, nullable=False, index=True)
    sku = Column(String, nullable=False, index=True)
    title = Column(String, nullable=False)
    kind = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    rules = Column(JSONB, default=dict, nullable=False)
    applicability = Column(JSONB, default=dict, nullable=False)
    overrides = Column(JSONB, default=list, nullable=False)
    time_basis = Column(String, default="ordered", nullable=False)
    policy_version = Column(String, nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    recorded_at = Column(DateTime(timezone=True), default=now, nullable=False)
    supersedes_id = Column(String, ForeignKey("v2_knowledge_versions.id"))
    status = Column(String, default="published", nullable=False)
    consumer_visible = Column(Boolean, default=True, nullable=False)
    content_hash = Column(String, nullable=False)
    embedding = Column(Vector(1024))
    title_embedding = Column(Vector(1024))
    __table_args__ = (UniqueConstraint("document_id", "revision"),)


class PolicySnapshot(Base):
    __tablename__ = "v2_policy_snapshots"
    id = Column(String, primary_key=True, default=uid)
    order_id = Column(String, ForeignKey("orders.id"), nullable=False, index=True)
    fingerprint = Column(String, nullable=False)
    version_ids = Column(JSONB, nullable=False)
    facts = Column(JSONB, nullable=False)
    resolved_rules = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    __table_args__ = (UniqueConstraint("order_id", "fingerprint"),)


class OrderEntitlement(Base):
    __tablename__ = "v2_order_entitlements"
    id = Column(String, primary_key=True, default=uid)
    order_id = Column(String, ForeignKey("orders.id"), nullable=False, index=True)
    fingerprint = Column(String, nullable=False)
    version_ids = Column(JSONB, nullable=False)
    facts = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    __table_args__ = (UniqueConstraint("order_id", "fingerprint"),)


class Turn(Base):
    __tablename__ = "v2_turns"
    id = Column(String, primary_key=True)
    conversation_id = Column(String, ForeignKey("conversations.id"), nullable=False, index=True)
    consumer_id = Column(String, nullable=False)
    raw_text = Column(Text, nullable=False)
    request_hash = Column(String, nullable=False)
    control_version = Column(Integer, nullable=False)
    status = Column(String, default="running", nullable=False)
    phase = Column(String, default="understanding", nullable=False)
    plan = Column(JSONB, default=dict, nullable=False)
    timings = Column(JSONB, default=dict, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    deadline_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))


class GenerationAttempt(Base):
    __tablename__ = "v2_generation_attempts"
    id = Column(String, primary_key=True, default=uid)
    turn_id = Column(String, ForeignKey("v2_turns.id"), nullable=False, index=True)
    task_id = Column(String, ForeignKey("tasks.id"))
    kind = Column(String, nullable=False)
    candidate = Column(JSONB, nullable=False)
    disposition = Column(String, nullable=False)
    reason = Column(String)
    evidence = Column(JSONB, default=dict, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)


class QueryVector(Base):
    __tablename__ = "v2_query_vectors"
    id = Column(String, primary_key=True)
    model = Column(String, nullable=False)
    embedding = Column(Vector(1024), nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)


class AdminSession(Base):
    __tablename__ = "v2_admin_sessions"
    token_hash = Column(String, primary_key=True)
    username = Column(String, nullable=False)
    role = Column(String, nullable=False)
    csrf_hash = Column(String, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)


class AnalysisBatch(Base):
    __tablename__ = "v2_analysis_batches"
    id = Column(String, primary_key=True)
    kind = Column(String, nullable=False)
    period_start = Column(DateTime(timezone=True), nullable=False)
    period_end = Column(DateTime(timezone=True), nullable=False)
    report = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)


class TrainingItem(Base):
    __tablename__ = "v2_training_items"
    id = Column(String, primary_key=True, default=uid)
    family_id = Column(String, nullable=False, index=True)
    revision = Column(Integer, nullable=False)
    batch_id = Column(String, ForeignKey("v2_analysis_batches.id"))
    purpose = Column(String, default="current_guidance", nullable=False)
    status = Column(String, default="draft", nullable=False)
    content = Column(JSONB, nullable=False)
    scope = Column(JSONB, nullable=False)
    source_ids = Column(JSONB, default=list, nullable=False)
    valid_from = Column(DateTime(timezone=True))
    valid_until = Column(DateTime(timezone=True))
    review_due_at = Column(DateTime(timezone=True))
    stale_reason = Column(String)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    __table_args__ = (UniqueConstraint("family_id", "revision"),)


class RegressionCase(Base):
    __tablename__ = "v2_regression_cases"
    id = Column(String, primary_key=True, default=uid)
    origin_message_id = Column(String, ForeignKey("messages.id"), nullable=False, index=True)
    batch_id = Column(String, ForeignKey("v2_analysis_batches.id"))
    revision = Column(Integer, default=1, nullable=False)
    status = Column(String, default="draft", nullable=False)
    category = Column(String, nullable=False)
    content = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    __table_args__ = (UniqueConstraint("origin_message_id", "revision"),)


class ReviewRecord(Base):
    __tablename__ = "v2_review_records"
    id = Column(String, primary_key=True, default=uid)
    target_type = Column(String, nullable=False)
    target_id = Column(String, nullable=False, index=True)
    reviewer = Column(String, nullable=False)
    action = Column(String, nullable=False)
    detail = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)


class DatasetVersion(Base):
    __tablename__ = "v2_dataset_versions"
    id = Column(String, primary_key=True, default=uid)
    content_hash = Column(String, nullable=False)
    cases = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)


class Improvement(Base):
    __tablename__ = "v2_improvements"
    id = Column(String, primary_key=True, default=uid)
    title = Column(String, nullable=False)
    case_ids = Column(JSONB, nullable=False)
    change_ref = Column(String, nullable=False)
    status = Column(String, default="reviewed", nullable=False)
    evaluation = Column(JSONB, default=dict, nullable=False)
    published_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
