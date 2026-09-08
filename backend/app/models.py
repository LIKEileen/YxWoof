from sqlalchemy import Column, String, Text, Integer, Boolean, DateTime, Float, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector
from .db import Base, now, uid

class Consumer(Base):
    __tablename__ = "consumers"
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

class Order(Base):
    __tablename__ = "orders"
    id = Column(String, primary_key=True)
    consumer_id = Column(String, ForeignKey("consumers.id"), nullable=False, index=True)
    merchant_id = Column(String, nullable=False, index=True)
    merchant_name = Column(String, nullable=False)
    sku = Column(String, nullable=False)
    product = Column(String, nullable=False)
    spec = Column(String, nullable=False)
    price_cents = Column(Integer, nullable=False)
    status = Column(String, nullable=False)
    delivery_days = Column(Integer)
    ordered_date = Column(String, nullable=False)
    version = Column(Integer, default=1, nullable=False)
    logistics = Column(JSONB, default=list, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Knowledge(Base):
    __tablename__ = "knowledge"
    id = Column(String, primary_key=True)
    merchant_id = Column(String, nullable=False, index=True)
    sku = Column(String, nullable=False)
    title = Column(String, nullable=False)
    kind = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    rules = Column(JSONB, default=dict, nullable=False)
    policy_version = Column(String, nullable=False)
    valid_from = Column(DateTime(timezone=True), nullable=False)
    valid_until = Column(DateTime(timezone=True), nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    consumer_visible = Column(Boolean, default=True, nullable=False)
    content_hash = Column(String, nullable=False)
    embedding = Column(Vector(1024))
    updated_at = Column(DateTime(timezone=True), default=now, nullable=False)

class BrowserSession(Base):
    __tablename__ = "browser_sessions"
    token_hash = Column(String, primary_key=True)
    consumer_id = Column(String, ForeignKey("consumers.id"), nullable=False)
    csrf_hash = Column(String, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)

class Conversation(Base):
    __tablename__ = "conversations"
    id = Column(String, primary_key=True, default=uid)
    consumer_id = Column(String, ForeignKey("consumers.id"), nullable=False, index=True)
    automation_state = Column(String, default="active", nullable=False)
    control_version = Column(Integer, default=1, nullable=False)
    state_version = Column(Integer, default=1, nullable=False)
    order_id = Column(String, ForeignKey("orders.id"))
    inflight_id = Column(String)
    inflight_until = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Task(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True, default=uid)
    conversation_id = Column(String, ForeignKey("conversations.id"), nullable=False, index=True)
    consumer_id = Column(String, nullable=False)
    order_id = Column(String, ForeignKey("orders.id"))
    requested_goal = Column(Text, nullable=False)
    goal = Column(String, nullable=False)
    scenario = Column(String, nullable=False)
    eligibility = Column(String, default="unknown", nullable=False)
    eligibility_reason = Column(String, default="unclassified", nullable=False)
    status = Column(String, default="processing", nullable=False)
    active = Column(Boolean, default=True, nullable=False)
    fields = Column(JSONB, default=dict, nullable=False)
    unhandled = Column(JSONB, default=list, nullable=False)
    human_involvement = Column(String, default="none_observed", nullable=False)
    action_id = Column(String, default=uid, nullable=False, unique=True)
    result = Column(JSONB, default=dict, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=now, nullable=False)
    __table_args__ = (Index("one_active_task_per_conversation", "conversation_id", unique=True, postgresql_where=active.is_(True)),)

class Message(Base):
    __tablename__ = "messages"
    id = Column(String, primary_key=True, default=uid)
    conversation_id = Column(String, ForeignKey("conversations.id"), nullable=False, index=True)
    task_id = Column(String, ForeignKey("tasks.id"))
    role = Column(String, nullable=False)
    text = Column(Text, nullable=False)
    cards = Column(JSONB, default=list, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Interaction(Base):
    __tablename__ = "interactions"
    id = Column(String, primary_key=True)
    conversation_id = Column(String, ForeignKey("conversations.id"), nullable=False)
    kind = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Preview(Base):
    __tablename__ = "previews"
    id = Column(String, primary_key=True, default=uid)
    task_id = Column(String, ForeignKey("tasks.id"), nullable=False, index=True)
    consumer_id = Column(String, nullable=False)
    order_id = Column(String, nullable=False)
    action_id = Column(String, nullable=False)
    content = Column(JSONB, nullable=False)
    content_hash = Column(String, nullable=False)
    policy_version = Column(String, nullable=False)
    fact_version = Column(Integer, nullable=False)
    control_version = Column(Integer, nullable=False)
    context_version = Column(Integer, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    invalidated = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Action(Base):
    __tablename__ = "actions"
    id = Column(String, primary_key=True)
    task_id = Column(String, ForeignKey("tasks.id"), nullable=False, unique=True)
    consumer_id = Column(String, nullable=False)
    order_id = Column(String, nullable=False)
    preview_id = Column(String, ForeignKey("previews.id"), nullable=False)
    confirmation_id = Column(String, nullable=False)
    status = Column(String, default="dispatched", nullable=False)
    application_id = Column(String)
    attempt_count = Column(Integer, default=1, nullable=False)
    reconcile_count = Column(Integer, default=0, nullable=False)
    reconcile_started_at = Column(DateTime(timezone=True))
    dispatched_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Application(Base):
    __tablename__ = "applications"
    id = Column(String, primary_key=True, default=uid)
    action_id = Column(String, nullable=False, unique=True)
    consumer_id = Column(String, nullable=False, index=True)
    order_id = Column(String, nullable=False, unique=True)
    status = Column(String, default="accepted", nullable=False)
    content = Column(JSONB, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Feedback(Base):
    __tablename__ = "feedback"
    id = Column(String, primary_key=True)
    task_id = Column(String, ForeignKey("tasks.id"), nullable=False)
    consumer_id = Column(String, nullable=False)
    helpful = Column(Boolean, nullable=False)
    category = Column(String, nullable=False)
    comment = Column(Text, nullable=False)
    csat = Column(Integer)
    ces = Column(Integer)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Event(Base):
    __tablename__ = "events"
    id = Column(String, primary_key=True, default=uid)
    name = Column(String, nullable=False, index=True)
    producer = Column(String, nullable=False)
    conversation_id = Column(String, index=True)
    task_id = Column(String, index=True)
    state_version = Column(Integer)
    payload = Column(JSONB, default=dict, nullable=False)
    occurred_at = Column(DateTime(timezone=True), default=now, nullable=False)
    received_at = Column(DateTime(timezone=True), default=now, nullable=False)

class Evaluation(Base):
    __tablename__ = "evaluations"
    id = Column(String, primary_key=True, default=uid)
    task_id = Column(String, nullable=False, index=True)
    labels = Column(JSONB, nullable=False)
    judge_type = Column(String, nullable=False)
    rubric_version = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)

class ModelRun(Base):
    __tablename__ = "model_runs"
    id = Column(String, primary_key=True, default=uid)
    interaction_id = Column(String, nullable=False, index=True)
    kind = Column(String, nullable=False)
    model = Column(String, nullable=False)
    status = Column(String, nullable=False)
    reserved = Column(Float, nullable=False)
    actual = Column(Float)
    usage = Column(JSONB, default=dict, nullable=False)
    latency_ms = Column(Integer)
    error_code = Column(String)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
