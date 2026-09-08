import asyncio
import json
import logging
import os
import tempfile
import jieba
from rank_bm25 import BM25Okapi
from sqlalchemy import select
from .db import SessionLocal, now, uid
from .models import Knowledge
from . import llm
from .domain import DomainError
jieba.setLogLevel(logging.ERROR)
jieba.dt.tmp_dir=os.getenv("JIEBA_CACHE_DIR",tempfile.gettempdir())

async def retrieve(merchant,sku,query,budget):
    budget.tool()
    with SessionLocal() as db:
        scoped=select(Knowledge).where(Knowledge.merchant_id==merchant,Knowledge.sku==sku,
                Knowledge.enabled.is_(True),Knowledge.consumer_visible.is_(True),
                Knowledge.valid_from<=now(),Knowledge.valid_until>now())
        docs=db.scalars(scoped).all()
        if not docs: raise DomainError("evidence_missing","当前缺少有效且适用的资料。")
        if any(d.embedding is None for d in docs):
            raise DomainError("knowledge_not_indexed","知识索引尚未就绪，暂不能提供有据回答。",503)
        copies=[{"id":d.id,"title":d.title,"content":d.content,"kind":d.kind,"policy_version":d.policy_version} for d in docs]
    budget.tool()
    v=(await llm.embed([query],budget))[0]
    with SessionLocal() as db:
        dense=db.scalars(scoped.order_by(Knowledge.embedding.cosine_distance(v)).limit(50)).all()
    tokenized=[list(jieba.cut(d["content"])) for d in copies]
    scores=BM25Okapi(tokenized).get_scores(list(jieba.cut(query)))
    lexical=sorted(range(len(copies)),key=lambda i:float(scores[i]),reverse=True)[:50]
    rrf={}
    for ranking in ([d.id for d in dense],[copies[i]["id"] for i in lexical]):
        for rank,key in enumerate(ranking,1): rrf[key]=rrf.get(key,0)+1/(60+rank)
    candidates=sorted(copies,key=lambda d:rrf.get(d["id"],0),reverse=True)[:10]
    budget.tool()
    indices=await llm.rerank(query,[d["content"] for d in candidates],budget)
    return [candidates[i] for i in indices]

async def index_knowledge():
    with SessionLocal() as db:
        docs=[(k.id,k.content,k.content_hash) for k in db.scalars(select(Knowledge).where(Knowledge.embedding.is_(None)))]
    for key,content,digest in docs:
        budget=llm.TurnBudget("index-"+uid())
        vectors=await llm.embed([content],budget)
        with SessionLocal.begin() as db:
            k=db.get(Knowledge,key)
            if k.content_hash==digest: k.embedding=vectors[0]
    print(json.dumps({"indexed":len(docs)},ensure_ascii=False))

if __name__=="__main__":
    asyncio.run(index_knowledge())
