"""Public contract checks: rollback-only DB use, no live model credentials or datasets."""
import copy,json,secrets
from pathlib import Path
from uuid import uuid4
import httpx,pytest
from fastapi.testclient import TestClient
from sqlalchemy import select,func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from app import config as settings,main,domain,llm,notifications,import_data
from app.db import engine
from app.models import Consumer,BrowserSession,ModelRun
from app.incidents import PUBLIC_MESSAGE

@pytest.fixture(autouse=True)
def isolated(monkeypatch,tmp_path):
    monkeypatch.setenv("INCIDENT_DIR",str(tmp_path/"incidents"))
    monkeypatch.delenv("MODEL_API_KEY",raising=False)
    monkeypatch.delenv("MODEL_API_KEY_FILE",raising=False)
    for key in ("MODEL_BASE_URL","MODEL_CHAT_NAME","MODEL_EMBEDDING_NAME","MODEL_RERANK_NAME"):
        monkeypatch.delenv(key,raising=False)
    def forbidden():raise AssertionError("A live model transport must never be created by tests")
    monkeypatch.setattr(llm,"pooled_client",forbidden)

@pytest.fixture
def database(monkeypatch):
    with engine.connect() as connection:
        transaction=connection.begin()
        sessions=sessionmaker(bind=connection,join_transaction_mode="create_savepoint",expire_on_commit=False)
        for module in (main,domain,llm,import_data):
            monkeypatch.setattr(module,"SessionLocal",sessions)
        yield sessions
        transaction.rollback()

@pytest.fixture
def client(database):
    return TestClient(main.app,base_url="http://localhost",raise_server_exceptions=False)

@pytest.fixture
def configured(monkeypatch):
    cfg=copy.deepcopy(settings.config())
    cfg["ai_enabled"]=True
    cfg["model_api"].update(base_url="https://model.example/v1",chat_model="chat",embedding_model="embed",rerank_model="rank")
    cfg["pricing"].update(configured=True,input_per_million=1,cached_input_per_million=.5,output_per_million=2,
                          input_reservation_per_million=2,output_reservation_per_million=3,embedding_reserve=.1,rerank_reserve=.2)
    monkeypatch.setenv("MODEL_API_KEY",secrets.token_urlsafe(18))
    monkeypatch.setattr(settings,"config",lambda:cfg)
    monkeypatch.setattr(llm,"config",lambda:cfg)
    return cfg

def test_empty_profiles_and_avatar(client):
    assert client.get("/api/v1/demo-profiles").json()=={"profiles":[]}
    assert client.get("/api/v1/branding/avatar").content.startswith(b"\x89PNG")
def test_only_fixed_existing_profiles(client,database):
    with database() as db:
        db.add_all([Consumer(id="lin",name=uuid4().hex),Consumer(id=uuid4().hex,name=uuid4().hex)]);db.commit()
    assert client.get("/api/v1/demo-profiles").json()=={"profiles":["lin"]}
def test_missing_identity_does_not_create_session(client,database):
    response=client.post("/api/v1/demo-session",json={"profile":"lin"},headers={"Origin":"http://localhost"})
    assert response.status_code==409
    assert response.json()["error"]=="demo_profile_unavailable"
    assert "set-cookie" not in response.headers
    with database() as db:assert db.scalar(select(func.count()).select_from(BrowserSession))==0
def test_invalid_identity_cannot_enumerate(client):
    assert client.post("/api/v1/demo-session",json={"profile":uuid4().hex},headers={"Origin":"http://localhost"}).status_code==422
def test_mutation_requires_origin(client):
    assert client.post("/api/v1/demo-session",json={"profile":"lin"}).status_code==403
def test_private_orders_require_identity(client):
    assert client.get("/api/v1/orders").status_code==401
def test_help_has_no_dependency_on_db(client,monkeypatch):
    monkeypatch.setattr(main,"SessionLocal",lambda:(_ for _ in ()).throw(RuntimeError("unavailable")))
    assert client.get("/api/v1/public-help").json()["entry"]["contact"]=="support@yxwoof.example"
def test_database_fault_uniform_and_independently_logged(client,monkeypatch,tmp_path):
    marker=uuid4().hex
    def failed():raise OperationalError("private SQL "+marker,{},RuntimeError(marker))
    monkeypatch.setattr(main,"SessionLocal",failed)
    response=client.get("/api/v1/demo-profiles")
    assert response.status_code==503
    assert response.json()=={"error":"service_unavailable","message":PUBLIC_MESSAGE}
    log=(tmp_path/"incidents/exceptions.jsonl").read_text()
    assert "OperationalError" in log and marker not in log and "private SQL" not in log
    assert list((tmp_path/"incidents/pending").glob("*.json"))
def test_disabled_notifications_do_not_construct_client(monkeypatch):
    monkeypatch.setattr(notifications.httpx,"Client",lambda **kw:(_ for _ in ()).throw(AssertionError()))
    assert notifications.deliver_once({"enabled":False})["attempted"]==0

@pytest.mark.asyncio
async def test_disabled_model_never_dispatches():
    with pytest.raises(domain.DomainError):await llm.structured("","",{},"probe",llm.TurnBudget(str(uuid4())))
@pytest.mark.asyncio
async def test_missing_key_never_dispatches(configured,monkeypatch):
    monkeypatch.delenv("MODEL_API_KEY")
    with pytest.raises(domain.DomainError,match="模型配置"):await llm.structured("","",{},"probe",llm.TurnBudget(str(uuid4())))
@pytest.mark.parametrize("address",["http://remote.example/v1","https://user:pass@model.example","https://model.example?key=value",""])
def test_invalid_model_address(configured,address):
    configured["model_api"]["base_url"]=address
    with pytest.raises(domain.DomainError):settings.model_settings("chat")
@pytest.mark.parametrize("field",["messages","model","response_format","stream","max_tokens"])
def test_extension_cannot_replace_controlled_fields(configured,field):
    configured["model_api"]["chat_extra"]={field:"override"}
    with pytest.raises(domain.DomainError):settings.model_settings("chat")
@pytest.mark.parametrize("price",[None,-1,float("nan"),float("inf"),True])
def test_invalid_pricing_fails_closed(configured,price):
    configured["pricing"]["input_per_million"]=price
    with pytest.raises(domain.DomainError):settings.model_settings("chat")
def test_environment_overrides_model(configured,monkeypatch):
    monkeypatch.setenv("MODEL_CHAT_NAME","configured-chat")
    assert settings.model_settings("chat")["model"]=="configured-chat"
def test_under_reserved_pricing_fails_closed(configured):
    configured["pricing"]["input_reservation_per_million"]=0
    with pytest.raises(domain.DomainError):settings.model_settings("chat")
def test_unconfirmed_pricing_fails_closed(configured):
    configured["pricing"]["configured"]=False
    with pytest.raises(domain.DomainError):settings.model_settings("chat")

@pytest.mark.asyncio
@pytest.mark.parametrize("failure",["http","timeout","invalid_json"])
async def test_model_fault_logged(configured,database,monkeypatch,tmp_path,failure):
    marker=uuid4().hex
    def handler(request):
        if failure=="timeout":raise httpx.ReadTimeout(marker,request=request)
        if failure=="invalid_json":return httpx.Response(200,text=marker)
        return httpx.Response(503,text=marker)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as fake:
        monkeypatch.setattr(llm,"pooled_client",lambda:fake)
        with pytest.raises(domain.DomainError) as caught:
            await llm.structured("","",{},"probe",llm.TurnBudget(str(uuid4())))
    from app.incidents import public_error
    body,status=public_error(caught.value)
    assert status==503 and body["message"]==PUBLIC_MESSAGE
    log=(tmp_path/"incidents/exceptions.jsonl").read_text()
    assert "model_api" in log and marker not in log
    with database() as db:
        row=db.scalars(select(ModelRun)).one()
        assert row.status=="failed" and row.actual is None and row.reserved>0

@pytest.mark.asyncio
async def test_model_contracts_and_configured_cost(configured,database,monkeypatch):
    calls=[]
    configured["model_api"]["chat_extra"]={"temperature":0}
    def handler(request):
        payload=json.loads(request.content);calls.append(payload)
        if request.url.path.endswith("/chat/completions"):
            assert payload["model"]=="chat" and payload["temperature"]==0 and "thinking" not in payload
            return httpx.Response(200,json={"choices":[{"message":{"content":json.dumps({"ok":True})}}],"usage":{"prompt_tokens":12,"completion_tokens":6}})
        if request.url.path.endswith("/embeddings"):
            return httpx.Response(200,json={"data":[{"index":0,"embedding":[0.0]*1024}]})
        return httpx.Response(200,json={"results":[{"index":0}]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as fake:
        monkeypatch.setattr(llm,"pooled_client",lambda:fake)
        assert await llm.structured("","",{},"probe",llm.TurnBudget(str(uuid4())))=={"ok":True}
        assert len((await llm.embed([""],llm.TurnBudget(str(uuid4()))))[0])==1024
        assert await llm.rerank("",[""],llm.TurnBudget(str(uuid4())))==[0]
    assert len(calls)==3
    with database() as db:
        rows=db.scalars(select(ModelRun)).all()
        assert len(rows)==3
        assert next(r for r in rows if r.kind=="chat").actual==pytest.approx(.000024)
        assert all(r.actual is None for r in rows if r.kind!="chat")

@pytest.mark.asyncio
async def test_wrong_vector_dimension(configured,database,monkeypatch):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={"data":[{"index":0,"embedding":[0.0]}]}))) as fake:
        monkeypatch.setattr(llm,"pooled_client",lambda:fake)
        with pytest.raises(domain.DomainError,match="格式异常"):await llm.embed([""],llm.TurnBudget(str(uuid4())))
def test_import_requires_operator_file(database,tmp_path):
    path=tmp_path/'input.json'
    identity=uuid4().hex
    path.write_text(json.dumps({'consumers':[{'id':identity,'name':uuid4().hex}],'orders':[],'knowledge':[]}))
    assert import_data.import_bundle(path)=={'consumers':1,'orders':0,'knowledge':0}
    with pytest.raises(ValueError):import_data.import_bundle(path)
    with database() as db:assert db.scalar(select(func.count()).select_from(Consumer))==1

def test_import_rejects_runtime_records(tmp_path):
    path=tmp_path/'input.json'
    path.write_text(json.dumps({'consumers':[],'orders':[],'knowledge':[],'sessions':[]}))
    from pydantic import ValidationError
    with pytest.raises(ValidationError):import_data.import_bundle(path)

def test_import_missing_file_does_not_seed(database,tmp_path):
    with pytest.raises(FileNotFoundError):import_data.import_bundle(tmp_path/'missing.json')
    with database() as db:assert db.scalar(select(func.count()).select_from(Consumer))==0
