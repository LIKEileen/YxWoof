"""Import operator-supplied business data; no bundled records or automatic seeding."""
import argparse,hashlib,json
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel,ConfigDict,Field,model_validator
from .db import SessionLocal
from .models import Consumer,Order,Knowledge

class Strict(BaseModel):
    model_config=ConfigDict(extra="forbid")
class ConsumerInput(Strict):
    id:str=Field(min_length=1,max_length=80)
    name:str=Field(min_length=1,max_length=100)
class OrderInput(Strict):
    id:str=Field(min_length=1,max_length=80)
    consumer_id:str
    merchant_id:str
    merchant_name:str
    sku:str
    product:str
    spec:str
    price_cents:int=Field(ge=0,strict=True)
    status:str
    delivery_days:int|None=Field(default=None,ge=0,strict=True)
    ordered_date:str
    version:int=Field(default=1,ge=1,strict=True)
    logistics:list[dict]=Field(default_factory=list)
class KnowledgeInput(Strict):
    id:str=Field(min_length=1,max_length=80)
    merchant_id:str
    sku:str
    title:str
    kind:str
    content:str=Field(min_length=1,max_length=8192)
    rules:dict=Field(default_factory=dict)
    policy_version:str
    valid_from:datetime
    valid_until:datetime
    enabled:bool=True
    consumer_visible:bool=True
    @model_validator(mode="after")
    def valid_period(self):
        if self.valid_from.tzinfo is None or self.valid_until.tzinfo is None or self.valid_from>=self.valid_until:
            raise ValueError("Knowledge validity must be timezone-aware and increasing")
        if self.kind=="policy":
            days=self.rules.get("return_days")
            if isinstance(days,bool) or not isinstance(days,int) or days<0:
                raise ValueError("Policy return_days must be a non-negative integer")
        return self
class Bundle(Strict):
    consumers:list[ConsumerInput]
    orders:list[OrderInput]
    knowledge:list[KnowledgeInput]

def import_bundle(path):
    data=Bundle.model_validate_json(Path(path).read_text())
    groups=((Consumer,data.consumers),(Order,data.orders),(Knowledge,data.knowledge))
    with SessionLocal.begin() as db:
        for model,items in groups:
            seen=set()
            for item in items:
                values=item.model_dump()
                if item.id in seen or db.get(model,item.id):
                    raise ValueError("An imported identifier already exists; no changes committed")
                seen.add(item.id)
                if model is Knowledge:
                    values["content_hash"]=hashlib.sha256(values["content"].encode()).hexdigest()
                db.add(model(**values))
            db.flush()
    return {name:len(getattr(data,name)) for name in ("consumers","orders","knowledge")}

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file",required=True,help="Operator-supplied JSON file; never commit it")
    args=parser.parse_args()
    print(json.dumps(import_bundle(args.file)))
