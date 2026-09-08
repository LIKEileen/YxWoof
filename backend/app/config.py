import json,math,os
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
CONFIG_PATH=Path(os.getenv("CONFIG_PATH",ROOT/"config/runtime.json"))
DATABASE_URL=os.environ["DATABASE_URL"]
AVATAR_PATH=Path(os.getenv("AVATAR_PATH",ROOT/"assets/avatar.png"))
RELEASE="public-0.1.0"

def config():
    value=json.loads(CONFIG_PATH.read_text())
    if not (0 < value["total_credits"] <= 1000 and 0 < value["turn_credits"] <= 10):
        raise ValueError("Invalid local budget limits")
    return value

def credential():
    key=os.getenv("MODEL_API_KEY","").strip()
    path=os.getenv("MODEL_API_KEY_FILE","")
    if not key and path:
        key=Path(path).read_text().strip()
    if not key:
        raise ValueError("Model credentials are not configured")
    return key

def model_settings(kind):
    from urllib.parse import urlsplit
    from .domain import DomainError
    cfg=config()
    if not cfg.get("ai_enabled"):
        raise DomainError("scope_disabled","AI 服务当前已停用",503)
    values=cfg.get("model_api",{})
    base=os.getenv("MODEL_BASE_URL") or values.get("base_url","")
    model=os.getenv("MODEL_"+kind.upper()+"_NAME") or values.get(kind+"_model","")
    parsed=urlsplit(base)
    allowed=parsed.scheme=="https" or (parsed.scheme=="http" and parsed.hostname in {"127.0.0.1","localhost"})
    if not allowed or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or not model:
        raise DomainError("model_configuration","模型配置不可用",503)
    extra=values.get(kind+"_extra",{})
    protected={"model","messages","response_format","stream","max_tokens","input","query","documents","top_n","return_documents"}
    if not isinstance(extra,dict) or protected.intersection(extra):
        raise DomainError("model_configuration","模型参数不可用",503)
    try:
        key=credential()
        prices=cfg["pricing"]
        fields=("input_per_million","cached_input_per_million","output_per_million",
                "input_reservation_per_million","output_reservation_per_million","embedding_reserve","rerank_reserve")
        if prices.get("configured") is not True:raise ValueError()
        for field in fields:
            number=prices[field]
            if isinstance(number,bool) or not isinstance(number,(int,float)) or not math.isfinite(number) or number<0:raise ValueError()
        if prices["input_reservation_per_million"]<max(prices["input_per_million"],prices["cached_input_per_million"]):raise ValueError()
        if prices["output_reservation_per_million"]<prices["output_per_million"]:raise ValueError()
        if prices["embedding_reserve"]<=0 or prices["rerank_reserve"]<=0:raise ValueError()
    except (KeyError,ValueError,OSError,TypeError):
        raise DomainError("model_configuration","模型配置不可用",503) from None
    return {"base_url":base.rstrip("/"),"model":model,"extra":extra,"key":key,"pricing":prices}
