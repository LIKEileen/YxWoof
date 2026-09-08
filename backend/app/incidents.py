"""DB-independent, allowlisted diagnostics and durable notification outbox."""
import contextvars
import fcntl
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

PUBLIC_MESSAGE = "抱歉，服务暂时不可用，请稍后重试。"
request_context = contextvars.ContextVar("incident_request", default=None)
MAX_LOG_BYTES = 10 * 1024 * 1024

def root():
    return Path(os.getenv("INCIDENT_DIR", "/workspace/incidents"))

def token(value):
    # Callers must pass machine codes, never request/exception text.
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", value) else "unclassified"

def identifier(value):
    return value if isinstance(value, str) and re.fullmatch(r"[a-fA-F0-9-]{32,36}", value) else None

def stamp():
    return datetime.now(timezone.utc).isoformat()

def stdout(row):
    try:
        sys.stderr.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        sys.stderr.flush()
    except Exception:
        pass  # Last-resort sink unavailable; never break the consumer error exit.

def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if temporary.exists(): temporary.unlink()

def append_log(row):
    directory = root()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    with os.fdopen(os.open(directory / ".log.lock", os.O_WRONLY | os.O_CREAT, 0o600), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = directory / "exceptions.jsonl"
        if path.exists() and path.stat().st_size >= MAX_LOG_BYTES:
            oldest = directory / "exceptions.jsonl.5"
            if oldest.exists(): oldest.unlink()
            for number in range(4, 0, -1):
                old = directory / ("exceptions.jsonl." + str(number))
                if old.exists(): os.replace(old, directory / ("exceptions.jsonl." + str(number + 1)))
            os.replace(path, directory / "exceptions.jsonl.1")
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), "w") as stream:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

def record(error, component, *, code=None, interaction_id=None, severity="error"):
    existing = getattr(error, "_yx_incident_id", None)
    if existing: return existing
    iid = str(uuid4())
    row = {
        "schema_version": 1, "event": "incident_recorded", "incident_id": iid,
        "occurred_at": stamp(), "severity": token(severity), "component": token(component),
        "error_type": token(type(error).__name__), "error_code": token(code or getattr(error, "code", type(error).__name__)),
        "request_id": identifier(request_context.get()), "interaction_id": identifier(interaction_id),
        "release_id": token(os.getenv("APP_RELEASE", "public-0.1.0")),
        # No exception messages, source lines, locals, SQL, URLs, headers or customer content.
        "frames": [{"file": Path(f.filename).name, "line": f.lineno, "function": token(f.name)}
                   for f in traceback.extract_tb(error.__traceback__)[-12:]],
    }
    stdout(row)
    for sink, write in [
        ("file_log", lambda: append_log(row)),
        ("outbox", lambda: atomic_json(root() / "pending" / (iid + ".json"),
                {"incident": row, "status": "pending", "attempts": 0, "next_attempt_at": 0})),
    ]:
        try: write()
        except Exception as failure:
            stdout({"event": "incident_sink_failed", "incident_id": iid, "sink": sink,
                    "error_type": token(type(failure).__name__), "occurred_at": stamp()})
    try: error._yx_incident_id = iid
    except Exception: pass
    return iid

def is_service_error(error):
    return (not hasattr(error, "status") or error.status >= 500 or
            getattr(error, "code", "").startswith(("model_", "turn_", "embedding_", "rerank_")) or
            getattr(error, "code", "") in {"scope_disabled", "knowledge_not_indexed", "input_limit"})

def public_error(error, component="http"):
    if is_service_error(error):
        record(error, component)
        return {"error": "service_unavailable", "message": PUBLIC_MESSAGE}, 503
    return {"error": error.code, "message": error.message}, error.status
