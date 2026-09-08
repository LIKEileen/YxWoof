"""Opt-in notification delivery CLI. No public admin endpoint and no default network calls."""
import argparse
import fcntl
import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse
import httpx
from .incidents import root, atomic_json, stdout, stamp

def deliver_once(settings=None, client=None):
    if settings is None:
        settings = json.loads(Path(os.getenv("NOTIFICATION_CONFIG", "/workspace/config/notifications.json")).read_text())
    if not settings.get("enabled"):
        return {"enabled": False, "attempted": 0, "delivered": 0, "failed": 0}
    url = settings.get("endpoint", "")
    parsed = urlparse(url)
    if parsed.username or parsed.password or parsed.fragment or parsed.query or not parsed.hostname:
        raise ValueError("Invalid notification endpoint")
    if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "localhost")):
        raise ValueError("HTTPS endpoint required")
    directory = root()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    result = {"enabled": True, "attempted": 0, "delivered": 0, "failed": 0}
    own = client is None
    client = client or httpx.Client(timeout=5, follow_redirects=False, trust_env=False)
    try:
        with os.fdopen(os.open(directory / ".delivery.lock", os.O_WRONLY | os.O_CREAT, 0o600), "w") as lock:
            try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError: return {**result, "busy": True}
            for path in sorted((directory / "pending").glob("*.json"))[:100]:
                try:
                    row = json.loads(path.read_text())
                    if row["next_attempt_at"] > time.time(): continue
                    iid = row["incident"]["incident_id"]
                    if row["status"] == "delivered":
                        (directory / "delivered").mkdir(exist_ok=True, mode=0o700)
                        os.replace(path, directory / "delivered" / path.name)
                        continue
                    row["attempts"] += 1
                    row["next_attempt_at"] = time.time() + min(3600, 30 * 2 ** min(row["attempts"] - 1, 7))
                    atomic_json(path, row)  # Persist attempt before dispatch; crash cannot cause a tight loop.
                    result["attempted"] += 1
                    headers = {"Idempotency-Key": iid}
                    credential = os.getenv("YXWOOF_NOTIFY_TOKEN")
                    if credential: headers["Authorization"] = "Bearer " + credential
                    try:
                        response = client.post(url, json=row["incident"], headers=headers)
                        ack = response.json() if 200 <= response.status_code < 300 else {}
                        if ack.get("accepted") is not True or ack.get("incident_id") != iid:
                            raise ValueError("missing_ack")
                        row.update(status="delivered", delivered_at=stamp())
                        atomic_json(path, row)
                        (directory / "delivered").mkdir(exist_ok=True, mode=0o700)
                        os.replace(path, directory / "delivered" / path.name)
                        result["delivered"] += 1
                    except Exception as error:
                        row["last_error_type"] = type(error).__name__
                        result["failed"] += 1
                        if row["attempts"] >= 8:
                            row["status"] = "dead_letter"
                        atomic_json(path, row)
                        if row["status"] == "dead_letter":
                            (directory / "dead-letter").mkdir(exist_ok=True, mode=0o700)
                            os.replace(path, directory / "dead-letter" / path.name)
                        stdout({"event": "notification_failed", "incident_id": iid, "attempt": row["attempts"],
                                "error_type": type(error).__name__, "occurred_at": stamp()})
                except Exception as error:
                    stdout({"event": "notification_queue_error", "error_type": type(error).__name__, "occurred_at": stamp()})
    finally:
        if own: client.close()
    return result

if __name__ == "__main__":
    argparse.ArgumentParser(description=__doc__).parse_args()
    print(json.dumps(deliver_once()))
