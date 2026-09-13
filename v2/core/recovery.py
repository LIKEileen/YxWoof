"""Idempotent outage bookkeeping and factual reconciliation; never replay chat."""
import asyncio
import json
import os
from pathlib import Path
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from ..db import now
from ..models import Conversation, Task, Action, Event
from ..incidents import atomic_json, record, identifier, token
from . import db as database, domain
from .models import Turn
from .settings import settings


def root():
    return Path(os.getenv("V2_RECOVERY_DIR", "/workspace/v2-recovery"))


def journal(s, error):
    """Only routing IDs and machine codes; no user text, SQL or credentials."""
    try:
        key = identifier(s.get("iid"))
        if not key: return
        atomic_json(root() / (key + ".json"), {"turn_id": key, "conversation_id": identifier(s.get("cid")),
            "code": token(getattr(error, "code", type(error).__name__)), "occurred_at": now().isoformat()})
    except Exception as exc: record(exc, "v2_recovery_journal")


async def scan():
    # Cross-process transaction lock. Each committed turn terminates once, even after a process restart.
    async with database.Session.begin() as db:
        if not await db.scalar(text("SELECT pg_try_advisory_xact_lock(87800022)")): return
        rows = (await db.scalars(select(Turn).where(Turn.status == "running", Turn.deadline_at <= now()))).all()
        for turn in rows:
            c = await domain.conversation(db, turn.consumer_id, turn.conversation_id)
            if c.inflight_id == turn.id: await domain.stop_inflight(db, c, "deadline_expired")
            else:
                turn.status = "cancelled"; turn.phase = "expired"; turn.finished_at = now()
                tasks = (await db.scalars(select(Task).where(Task.turn_id == turn.id, Task.status.in_(["pending", "running"])))).all()
                for t in tasks: t.status = "deferred"; t.context_version += 1
    paths = await asyncio.to_thread(lambda: sorted(root().glob("*.json"))[:100])
    for path in paths:
        try:
            data = await asyncio.to_thread(lambda: json.loads(path.read_text()))
            async with database.Session.begin() as db:
                await db.execute(insert(Event).values(id="v2-recovery-" + data["turn_id"], name="v2_outage_recovered",
                    producer="server", conversation_id=data["conversation_id"], payload=data)
                    .on_conflict_do_nothing(index_elements=["id"]))
            await asyncio.to_thread(path.unlink, missing_ok=True)
        except FileNotFoundError: pass
    async with database.Session() as db:
        actions = (await db.execute(select(Action.id, Action.consumer_id, Task.conversation_id)
            .join(Task, Task.id == Action.task_id).where(Task.api_version == 2, Action.status == "unknown"))).all()
    for aid, consumer, cid in actions:
        await domain.reconcile(consumer, cid, aid, True)


async def loop():
    while True:
        try:
            async with asyncio.timeout(8): await scan()
            if settings()["learning_enabled"]:
                from .learning import scheduled
                await scheduled()
        except asyncio.CancelledError: raise
        except Exception as exc: record(exc, "v2_recovery_scan")
        await asyncio.sleep(2)
