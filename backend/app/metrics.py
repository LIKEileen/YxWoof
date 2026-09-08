"""PRD v1.1 cohort metrics. Missing evidence is never converted to success."""
import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import select
from .db import SessionLocal, now
from .models import Task, Event, Evaluation, Action, Application, Feedback, Interaction, Message
from .llm import SessionLocal as LedgerSession, cost_total
from .config import config
from .models import ModelRun

EXPECTED={"qa":"question_answered","query":"order_queried","eligibility":"eligibility_explained","application":"application_accepted"}

def fraction(n,d,missing=0):
    return {"numerator":n,"denominator":d,"value":n/d if d and not missing else None,
            "status":"incomplete" if missing else "available" if d else "N/A","missing":missing}

def percentile(values,p):
    if not values:return None
    values=sorted(values)
    import math
    return values[max(0,min(len(values)-1,math.ceil(len(values)*p)-1))]

def evaluate_cohort(tasks,labels,visible,as_of,visible_times=None):
    n=d=unverified=inflight=0; verified_ids=[];reasons=Counter()
    for t in tasks:
        window=timedelta(hours=24) if t["goal"]=="application" else timedelta(minutes=30)
        if t["eligibility"]=="out_of_scope":reasons["excluded_out_of_scope"]+=1;continue
        if t["eligibility"]=="unknown":reasons["eligibility_unknown"]+=1;continue
        if t["created_at"]+window>as_of:inflight+=1;continue
        d+=1
        label=labels.get(t["id"]);result=t.get("result") or {};mid=result.get("answer_ref")
        displayed=mid in visible and (not visible_times or visible_times.get(mid,as_of)<=t["created_at"]+window)
        if not label or any(k not in label for k in ("correct","safe","independent")):
            unverified+=1;reasons["missing_independent_evaluation"]+=1;continue
        if (label["correct"] and label["safe"] and label["independent"] and t["human_involvement"]=="none_observed"
            and result.get("milestone")==EXPECTED.get(t["goal"]) and displayed):
            n+=1;verified_ids.append(t["id"])
        else:reasons["not_verified_complete"]+=1
    return {**fraction(n,d),"inflight":inflight,"unverified":unverified,"verified_ids":verified_ids,
            "interpretation":"conservative_verified_fraction; unreviewed tasks are not successes","reasons":dict(reasons)}

def build_report(as_of=None,task_ids=None):
    as_of=as_of or now()
    from .models import Order, Preview
    with SessionLocal() as db:
        tasks=db.scalars(select(Task)).all();events=db.scalars(select(Event)).all()
        evaluations=db.scalars(select(Evaluation).order_by(Evaluation.created_at)).all()
        actions=db.scalars(select(Action)).all();applications=db.scalars(select(Application)).all()
        feedback=db.scalars(select(Feedback)).all();interactions=db.scalars(select(Interaction)).all()
        messages=db.scalars(select(Message)).all();previews=db.scalars(select(Preview)).all()
        orders={t.id:db.get(Order,t.order_id) for t in tasks if t.order_id}
    if task_ids is not None:
        tasks=[t for t in tasks if t.id in task_ids];cids={t.conversation_id for t in tasks}
        events=[e for e in events if e.task_id in task_ids or e.conversation_id in cids]
        evaluations=[e for e in evaluations if e.task_id in task_ids]
        actions=[a for a in actions if a.task_id in task_ids];aids={a.id for a in actions}
        applications=[a for a in applications if a.action_id in aids]
        feedback=[f for f in feedback if f.task_id in task_ids]
        interactions=[i for i in interactions if i.conversation_id in cids]
        messages=[m for m in messages if m.conversation_id in cids]
        previews=[p for p in previews if p.task_id in task_ids]
        orders={k:v for k,v in orders.items() if k in task_ids}
    labels={}
    for e in evaluations:labels.setdefault(e.task_id,{}).update(e.labels)
    taskdict=[{"id":t.id,"goal":labels.get(t.id,{}).get("expected_goal",t.goal),
               "eligibility":"in_scope" if labels.get(t.id,{}).get("scope_in_scope") is True else t.eligibility,"created_at":t.created_at,
               "human_involvement":t.human_involvement,"result":t.result} for t in tasks]
    byname=defaultdict(list)
    for e in events:byname[e.name].append(e)
    shown=byname["answer_presented"];visible={e.payload.get("message_id") for e in shown};times={}
    for e in shown:
        mid=e.payload.get("message_id")
        if mid:times[mid]=min(times.get(mid,e.received_at),e.received_at)
    ns=evaluate_cohort(taskdict,labels,visible,as_of,times)
    def tids(name):return {e.task_id for e in byname[name] if e.task_id}
    def label_metric(key,candidates=None):
        candidates=tasks if candidates is None else candidates
        known=[labels[t.id][key] for t in candidates if labels.get(t.id,{}).get(key) is not None]
        return {**fraction(sum(bool(v) for v in known),len(candidates),len(candidates)-len(known)),"reviewed":len(known)}
    def mature(t):return t.created_at+(timedelta(hours=24) if t.goal=="application" else timedelta(minutes=30))<=as_of
    metrics={"NS-01":ns}
    # The Interaction table is an independent durable ingress object, not a count of E01 rows.
    ingress={i.id for i in interactions if i.kind=="chat"}
    ingress|={e.payload.get("request_id") for e in byname["service_request_received"] if e.payload.get("entry")=="human_request"}
    associated={e.payload.get("request_id") for e in byname["service_request_received"] if e.task_id}
    associated|={e.payload.get("request_id") for e in byname["request_dispositioned"] if e.payload.get("scope_reason")}
    metrics["PR-01"]={**fraction(len(ingress&associated),len(ingress)),"missing_request_ids":sorted(ingress-associated)}
    coverage=label_metric("scope_in_scope")
    metrics["PR-02"]={**coverage,"declared_in_scope":sum(t.eligibility=="in_scope" for t in tasks),
        "classification_unknown":sum(t.eligibility=="unknown" for t in tasks),
        "unhandled_subgoals":sum(len(t.unhandled) for t in tasks),
        "note":"Formal coverage needs independent atomic-goal classification; application declarations are shown separately."}
    metrics["PR-03"]=label_metric("binding_correct",[t for t in tasks if t.order_id])
    metrics["PR-04"]={**label_metric("evidence_sufficient",[t for t in tasks if labels.get(t.id,{}).get("evidence_exists") is True]),
                       "missing_evidence_truth":sum("evidence_exists" not in labels.get(t.id,{}) for t in tasks)}
    substantive=[t for t in tasks if t.result.get("milestone") in ("question_answered","order_queried","eligibility_explained")]
    metrics["PR-05"]={**label_metric("answer_grounded",substantive),"not_substantively_answered":sum(labels.get(t.id,{}).get("expected_goal",t.goal) in ("qa","query","eligibility") for t in tasks)-len(substantive),"not_applicable":sum(labels.get(t.id,{}).get("expected_goal",t.goal)=="application" for t in tasks)}
    query_ids={e.task_id for e in byname["tool_attempt_started"] if e.payload.get("tool_name")=="query_order"}
    metrics["PR-06"]=label_metric("query_valid",[t for t in tasks if t.id in query_ids])
    valid={e.task_id for e in byname["confirmation_validated"] if e.payload.get("validity")=="valid"}
    shown_preview={e.task_id for e in byname["application_preview_presented"] if e.payload.get("preview_valid") is True}
    mature_apps={t.id for t in tasks if t.goal=="application" and mature(t)}
    accepted={a.task_id for a in actions if a.status=="accepted"}
    accepted_shown={t.id for t in tasks if t.id in accepted and t.result.get("milestone")=="application_accepted"
                    and t.result.get("answer_ref") in visible}
    metrics["PR-07"]={"valid_confirmation":fraction(len(valid&shown_preview&mature_apps),len(shown_preview&mature_apps)),
        "accepted_and_presented":fraction(len(valid&accepted_shown&mature_apps),len(valid&mature_apps)),
        "accepted_without_result_display":len(accepted-accepted_shown),
        "inflight_application_tasks":sum(t.goal=="application" and not mature(t) for t in tasks)}
    unknown=[a for a in actions if a.reconcile_started_at and a.reconcile_started_at+timedelta(seconds=120)<=as_of]
    resolved={}
    for a in unknown:
        facts=[e for e in byname["business_result_observed"] if e.payload.get("action_id")==a.id
               and e.payload.get("business_status") in ("accepted","not_created","rejected")
               and a.reconcile_started_at<=e.received_at<=a.reconcile_started_at+timedelta(seconds=120)]
        if facts:resolved[a.id]=min(facts,key=lambda e:e.received_at).payload["business_status"]
    metrics["PR-08"]={**fraction(len(resolved),len(unknown)),"outcomes":dict(Counter(resolved.values()))}
    metrics["PR-11"]=label_metric("unnecessary_requestion")
    timed={}
    for e in shown:
        iid=e.payload.get("interaction_id");duration=e.payload.get("duration_ms")
        if iid and duration is not None:timed[iid]=min(timed.get(iid,duration),duration)
    expected={e.payload.get("interaction_id"):e for e in byname["user_interaction_received"]
              if e.payload.get("interaction_type") in ("send","preview","confirm") and e.payload.get("interaction_id")}
    matured_inputs={i:e for i,e in expected.items() if e.received_at+timedelta(seconds=30)<=as_of}
    durations=[timed[i] for i,e in expected.items() if e.payload.get("interaction_type")=="send" and i in timed]
    metrics["PR-12"]={"p50_ms":percentile(durations,.5),"p95_ms":percentile(durations,.95),"n":len(durations),
        "missing_display_evidence":len(set(matured_inputs)-set(timed)),"status":"available" if durations else "N/A",
        "source":"First meaningful client visibility, same monotonic clock; loader is excluded"}
    metrics["PR-13"]=label_metric("next_step_correct")
    requested=tids("human_requested")
    paused={e.task_id for e in byname["automation_control_changed"] if e.payload.get("automation_state")=="paused"}
    help_shown=tids("help_entry_presented")
    available={e.task_id for e in byname["help_entry_presented"] if e.payload.get("availability")=="configured"}
    exit_unit=lambda e:e.task_id or ("entry:"+e.conversation_id if e.conversation_id else e.id)
    exit_requests={exit_unit(e) for e in byname["human_requested"]}
    exit_paused={exit_unit(e) for e in byname["automation_control_changed"] if e.payload.get("automation_state")=="paused"}
    exit_shown={exit_unit(e) for e in byname["help_entry_presented"]}
    metrics["PR-14"]={"correct_exit":fraction(len(exit_requests&exit_paused&exit_shown),len(exit_requests)),
                     "opened":fraction(len(tids("help_entry_opened")&available),len(available)),"entry_without_task_included":True}
    calibrated={}
    for e in shown:
        mid=e.payload.get("message_id");elapsed=e.payload.get("task_elapsed_ms")
        if mid and elapsed is not None and e.payload.get("clock_uncertainty_ms") is not None:
            prior=calibrated.get(mid)
            if not prior or elapsed<prior[0]:calibrated[mid]=(elapsed,e.payload["clock_uncertainty_ms"])
    verified_elapsed=[calibrated[t.result["answer_ref"]] for t in tasks if t.id in ns["verified_ids"] and t.result.get("answer_ref") in calibrated]
    # Same-server receipt durations are explicitly a proxy, never mislabeled as calibrated display time.
    server_proxy=[(times[t.result["answer_ref"]]-t.created_at).total_seconds()*1000
                  for t in tasks if t.id in ns["verified_ids"] and t.result.get("answer_ref") in times]
    unfinished=[t for t in tasks if t.eligibility=="in_scope" and t.id not in ns["verified_ids"]]
    metrics["OUT-03"]={**ns,"end_to_end_display_ms":{"p50":percentile([x[0] for x in verified_elapsed],.5),"p95":percentile([x[0] for x in verified_elapsed],.95),
            "n":len(verified_elapsed),"missing":ns["numerator"]-len(verified_elapsed),"clock_mode":"RTT-bounded server/client calibration",
            "max_uncertainty_ms":max([x[1] for x in verified_elapsed],default=None)},
        "same_server_receipt_proxy_ms":{"p50":percentile(server_proxy,.5),"p95":percentile(server_proxy,.95),"n":len(server_proxy)},
        "unfinished":len(unfinished),"oldest_unfinished_seconds":max([(as_of-t.created_at).total_seconds() for t in unfinished],default=0),
        "overdue_unfinished":sum(mature(t) for t in unfinished)}
    operations=defaultdict(set)
    interaction_tasks={e.payload.get("request_id"):e.task_id for e in byname["service_request_received"] if e.task_id}
    interaction_tasks.update({e.payload.get("interaction_id"):e.task_id for e in byname["user_interaction_received"] if e.task_id})
    for e in byname["user_interaction_recorded"]+byname["object_selected"]+byname["confirmation_clicked"]:
        iid=e.payload.get("interaction_id")
        target=e.task_id or interaction_tasks.get(iid)
        if target and iid and e.payload.get("action_result")!="response_received":operations[target].add(iid)
    metrics["OUT-04"]={"all_tasks":{"operations":sum(len(operations[t.id]) for t in tasks),"n":len(tasks),
                         "mean":sum(len(operations[t.id]) for t in tasks)/len(tasks) if tasks else None},
        "verified_tasks":{"operations":sum(len(operations[i]) for i in ns["verified_ids"]),"n":ns["numerator"],
                          "mean":sum(len(operations[i]) for i in ns["verified_ids"])/ns["numerator"] if ns["numerator"] else None},
        "unassociated_client_operations":sum(not e.task_id and not interaction_tasks.get(e.payload.get("interaction_id")) for e in byname["user_interaction_recorded"] if e.payload.get("interaction_id")),
        "baseline_status":"not_collected","active_time_status":"not_collected"}
    with LedgerSession() as ledger:
        total=cost_total(ledger)
        interaction_ids={i.id for i in interactions}
        runs=ledger.scalars(select(ModelRun).where(ModelRun.interaction_id.in_(interaction_ids))).all() if interaction_ids else []
    known_cost=sum(r.actual or 0 for r in runs);missing_cost=sum(r.actual is None for r in runs)
    metrics["OUT-06"]={"value":None,"model_only_estimate_per_verified":known_cost/ns["numerator"] if ns["numerator"] and not missing_cost else None,
        "cohort_estimated_model_credits":known_cost,"missing_cost_runs":missing_cost,"total_reserved_or_estimated_credits":total,
        "currency":config().get("pricing",{}).get("unit","LOCAL_BUDGET_UNIT"),"unallocated_costs":["tools","runtime"],"status":"incomplete; not full technical cost or ROI"}
    ratings=[f.csat for f in feedback if f.csat is not None];ces=[f.ces for f in feedback if f.ces is not None]
    invited=tids("feedback_invited");should_invite={m.task_id for m in messages if m.role=="assistant" and m.task_id}
    metrics["OUT-07"]={"CSAT":fraction(sum(x>=4 for x in ratings),len(ratings)),
        "CSAT_distribution":dict(Counter(ratings)),"CES_mean":sum(ces)/len(ces) if ces else None,"CES_n":len(ces),
        "CES_distribution":dict(Counter(ces)),"invitation_coverage":fraction(len(invited&should_invite),len(should_invite)),
        "response_rate":fraction(len({f.task_id for f in feedback}&invited),len(invited))}
    metrics["GD-01"]=label_metric("unauthorized_disclosure")
    valid_confirm={e.payload.get("action_id"):e for e in byname["confirmation_validated"] if e.payload.get("validity")=="valid"}
    dispatch={e.payload.get("action_id"):e for e in byname["action_dispatch_decided"] if e.payload.get("decision")=="allow"}
    app_ids={a.action_id for a in applications};action_map={a.id:a for a in actions}
    missing_proof={aid for aid in app_ids if aid not in valid_confirm or aid not in dispatch}
    invalid={aid for aid in app_ids if aid in valid_confirm and aid in dispatch and
             (valid_confirm[aid].payload.get("confirmation_id")!=action_map[aid].confirmation_id
              or dispatch[aid].payload.get("confirmation_id")!=action_map[aid].confirmation_id)}
    metrics["GD-02"]={**fraction(len(invalid),len(app_ids),len(missing_proof)),"missing_confirmation_evidence":len(missing_proof)}
    counts=Counter(a.action_id for a in applications)
    metrics["GD-03"]={**fraction(sum(v>1 for v in counts.values()),sum(t.goal=="application" for t in tasks)),
                      "duplicate_records":sum(max(v-1,0) for v in counts.values())}
    critical_tasks={m.task_id for m in messages if m.id in visible and any(c.get("type") in ("result","existing_application") for c in m.cards)}
    metrics["GD-04"]=label_metric("false_success_claim",[t for t in tasks if t.id in critical_tasks])
    modes={};violations=set()
    for e in sorted(events,key=lambda e:e.received_at):
        if e.name=="automation_control_changed":modes[e.conversation_id]=e.payload.get("automation_state")
        forbidden=(e.name=="action_dispatch_decided" and e.payload.get("decision")=="allow") or (e.name=="answer_generated" and e.payload.get("answer_type")=="sources")
        if forbidden and modes.get(e.conversation_id)=="paused":violations.add(e.task_id)
    metrics["GD-05"]={**fraction(len(violations),len(paused)),"absolute_incidents":len(violations),
                      "excluded":"Necessary deterministic pause notifications and original-action reconciliation"}
    metrics["GD-06"]={"unsafe_continued":label_metric("unsafe_continued",[t for t in tasks if labels.get(t.id,{}).get("must_stop") is True]),
        "over_refusal":label_metric("over_refusal",[t for t in tasks if labels.get(t.id,{}).get("can_complete") is True]),
        "missing_stop_truth":sum("must_stop" not in labels.get(t.id,{}) for t in tasks)}
    reopen_matured={t.id for t in tasks if t.result and t.created_at+timedelta(hours=72)<=as_of}
    reopen={e.task_id for e in byname["task_reopened"] if e.task_id in reopen_matured and any(t.id==e.task_id and e.received_at<=t.created_at+timedelta(hours=72) for t in tasks)}
    metrics["GD-07"]={**fraction(len(reopen),len(reopen_matured)),"scope":"Same conversation and controlled evaluation only"}
    no_display={i for i in matured_inputs if i not in timed or timed[i]>30000}
    metrics["GD-08"]={"no_timely_display_evidence":fraction(len(no_display),len(matured_inputs)),
        "continued_after_budget":label_metric("budget_overrun"),"model_runs":len(runs),
        "budget_reached_events":len(byname["runtime_budget_reached"])}
    # Independently enumerate durable objects and check required event relations.
    audit_rows=[]
    def check(kind,identifier,requirements):
        missing=[label for label,okay in requirements.items() if not okay]
        audit_rows.append({"kind":kind,"id":identifier,"pass":not missing,"missing":missing})
    generated={e.payload.get("answer_ref") for e in byname["answer_generated"]}
    for t in tasks:check("task",t.id,{"task_registered":t.id in tids("task_registered")})
    for i in ingress:check("input",i,{"service_request_received":any(e.payload.get("request_id")==i for e in byname["service_request_received"]),
                                     "associated_or_dispositioned":i in associated})
    for m in messages:
        if m.role=="assistant":check("answer",m.id,{"answer_generated":m.id in generated})
    for p in previews:check("preview",p.id,{"application_preview_created":any(e.payload.get("preview_id")==p.id for e in byname["application_preview_created"])})
    for a in actions:
        check("action",a.id,{"valid_confirmation":a.id in valid_confirm,"dispatch_decision":a.id in dispatch,
            "tool_started":any(e.payload.get("action_id")==a.id and e.payload.get("read_or_write")=="write" for e in byname["tool_attempt_started"]),
            "tool_finished":any(e.payload.get("action_id")==a.id and e.payload.get("read_or_write")=="write" for e in byname["tool_attempt_finished"]),
            "business_observed":any(e.payload.get("action_id")==a.id for e in byname["business_result_observed"])})
    for f in feedback:check("feedback",f.id,{"feedback_submitted":any(e.payload.get("feedback_id")==f.id for e in byname["feedback_submitted"])})
    task_ids={t.id for t in tasks}
    orphan_refs=sum(bool(e.task_id and e.task_id not in task_ids) for e in events)
    consistent=sum(a.status=="unknown" or (a.status=="accepted" and any(p.action_id==a.id and p.id==a.application_id for p in applications)) or (a.status=="not_created" and a.id not in app_ids) for a in actions)
    metrics["GD-09"]={"key_objects":fraction(sum(x["pass"] for x in audit_rows),len(audit_rows)),
        "state_consistency":fraction(consistent,len(actions)),"missing_links":sum(len(x["missing"]) for x in audit_rows),
        "orphan_task_refs":orphan_refs,"accepted_without_display":len(accepted-accepted_shown),"missing_cost_runs":missing_cost,
        "failed_objects":[x for x in audit_rows if not x["pass"]],
        "client_completeness":"Compare independent Playwright receipts for actual display; absent events remain unverified"}
    metrics["GD-10"]={"privacy_incidents":label_metric("private_log_leak"),
                      "configuration_regression":label_metric("configuration_regression")}
    strata={}
    for merchant in sorted({o.merchant_id for o in orders.values() if o}):
        for goal in EXPECTED:
            subset=[t for t in taskdict if t["goal"]==goal and t["id"] in orders and orders[t["id"]].merchant_id==merchant]
            if subset:strata[merchant+"/"+goal]=evaluate_cohort(subset,labels,visible,as_of,times)
    return {"generated_at":now().isoformat(),"as_of":as_of.isoformat(),"metric_version":"prd-1.1","environment":"synthetic",
        "metrics":metrics,"strata":strata,"sample_n":len(tasks),"evaluation_coverage":len(labels),
        "safety_note":"Missing independent labels and client evidence are not successes. No human usability claim."}

def main():
    p=argparse.ArgumentParser();p.add_argument("--output",default="/workspace/docs/metrics-report.json")
    p.add_argument("--as-of");p.add_argument("--cohort");args=p.parse_args()
    ids={r["task_id"] for r in json.loads(Path(args.cohort).read_text())["records"] if r.get("task_id")} if args.cohort else None
    result=build_report(datetime.fromisoformat(args.as_of) if args.as_of else None,ids)
    Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps({"output":args.output,"NS-01":result["metrics"]["NS-01"],"budget":result["metrics"]["OUT-06"]["total_reserved_or_estimated_credits"]},ensure_ascii=False))

if __name__=="__main__":main()
