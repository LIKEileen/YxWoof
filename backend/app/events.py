from .models import Event
from .db import uid
from .config import config

CLIENT_EVENTS = {
    "object_selection_shown", "object_selected", "answer_presented",
    "application_preview_presented", "confirmation_clicked", "feedback_invited",
    "user_interaction_recorded", "help_entry_presented", "help_entry_opened", "service_summary_copied",
}

def emit(db, name, conv=None, task=None, producer="server", event_id=None, **payload):
    cfg = config()
    e = Event(id=event_id or uid(), name=name, producer=producer,
              conversation_id=conv.id if conv else None,
              task_id=task.id if task else None,
              state_version=conv.state_version if conv else None,
              payload={"schema_version": 1, "environment": "demo", "data_mode": "synthetic",
                       "clock_mode": "real", "release_id": cfg["release_id"],
                       "workflow_version": cfg["workflow_version"], "prompt_version": cfg["prompt_version"],
                       "metric_version": cfg["metric_version"], "price_version": cfg["price_version"],
                       "platform_id": "yx-demo",
                       **({"actor_pseudo_id":conv.consumer_id,"auth_context_ref":conv.id,"interaction_id":conv.inflight_id} if conv else {}),
                       **({"agreed_task_goal":task.goal,"scenario_id":task.scenario,"eligibility":task.eligibility,
                           "eligibility_reason":task.eligibility_reason,"human_involvement":task.human_involvement} if task else {}), **payload})
    db.add(e)
    return e
