"""Server maintenance and release evidence; no management interface."""
import argparse, hashlib, json
from datetime import timedelta
from pathlib import Path
from sqlalchemy import select
from .config import config
from .db import SessionLocal, now, uid
from .models import Event, Task
from .domain import conversation
from .events import emit

def sweep():
    cfg=config();fingerprint=hashlib.sha256(json.dumps(cfg,sort_keys=True).encode()).hexdigest()
    with SessionLocal.begin() as db:
        previous=db.scalar(select(Event).where(Event.name=="kill_switch_changed").order_by(Event.received_at.desc()).limit(1))
        if not previous or previous.payload.get("config_version")!=fingerprint:
            emit(db,"kill_switch_changed",affected_scope=cfg["disabled_merchants"],config_version=fingerprint,
                 ai_enabled=cfg["ai_enabled"],writes_enabled=cfg["writes_enabled"],reason_code="configuration_observed")
        matured=db.scalars(select(Task).where(Task.created_at<now()-timedelta(minutes=30))).all()
        for t in matured:
            if t.created_at+(timedelta(hours=24) if t.goal=="application" else timedelta(minutes=30))>now():continue
            event_id="window-"+t.id
            if not db.get(Event,event_id):
                c=conversation(db,t.consumer_id,t.conversation_id)
                emit(db,"task_window_elapsed",c,t,event_id=event_id,window_version="p0-1",original_task_id=t.id,reason="real_window_matured")

def release(report,kind):
    with SessionLocal.begin() as db:
        emit(db,"release_evaluation_finished",producer="evaluation",test_set_version=report.get("test_set_version","p0-1"),
             release_id=config()["release_id"],report_ref=report.get("report_ref"),result=report["result"],scope=kind,
             test_count=report.get("test_count"),interpretation=report.get("interpretation"))

def incident(severity,scope,reason):
    with SessionLocal.begin() as db:
        emit(db,"incident_recorded",incident_severity=severity,affected_scope=scope,reason_code=reason)
    # Stop configuration is a separate explicit operation; recording cannot pretend to stop a service.

if __name__=="__main__":
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("sweep")
    r=sub.add_parser("release");r.add_argument("file");r.add_argument("--scope",required=True)
    i=sub.add_parser("incident");i.add_argument("--severity",required=True);i.add_argument("--scope",required=True);i.add_argument("--reason",required=True)
    a=p.parse_args()
    if a.cmd=="sweep":sweep()
    elif a.cmd=="release":release(json.loads(Path(a.file).read_text()),a.scope)
    else:incident(a.severity,a.scope,a.reason)
    print("Audit evidence recorded.")
