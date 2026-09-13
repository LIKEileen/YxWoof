"""Scoped multi-route retrieval over KnowledgeVersion only, never TrainingItem."""
import asyncio
import math
import re
import time
import jieba
from rank_bm25 import BM25Okapi
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from ..domain import DomainError
from ..config import config
from ..models import Order
from . import db as database, runtime, temporal
from .models import KnowledgeVersion, QueryVector
from .settings import settings

EMBED_MODEL = "ecnu-embedding-small"

def query_key(query):
    return temporal.digest({"model": EMBED_MODEL, "text": query})

async def query_vectors(queries, budget):
    queries = list(dict.fromkeys(queries))
    ids = [query_key(q) for q in queries]
    async with database.Session() as db:
        found = {v.id: list(v.embedding) for v in (await db.scalars(select(QueryVector).where(QueryVector.id.in_(ids)))).all()}
    missing = [q for q in queries if query_key(q) not in found]
    if missing:
        budget.tool()
        vectors = await runtime.embed(missing, budget)
        async with database.Session.begin() as db:
            for query, vector in zip(missing, vectors):
                key = query_key(query)
                await db.execute(insert(QueryVector).values(id=key, model=EMBED_MODEL, embedding=vector)
                                 .on_conflict_do_nothing(index_elements=["id"]))
                found[key] = vector
    return {q: found[query_key(q)] for q in queries}

def cosine(a, b):
    norm = math.sqrt(sum(float(x) ** 2 for x in a) * sum(float(x) ** 2 for x in b))
    return sum(float(x) * float(y) for x, y in zip(a, b)) / norm if norm else 0.0

def tokens(text):
    return [x.lower() for x in jieba.lcut(text) if x.strip() and re.search(r"\w", x)]

def lexical_rank(docs, query):
    explicit = bool(re.search(r"(?<![A-Za-z0-9])[A-Za-z][A-Za-z0-9_-]*\d[A-Za-z0-9_-]*(?![A-Za-z0-9])", query))
    explicit = explicit or any(len(d["title"]) >= 4 and d["title"] in query for d in docs)
    explicit = explicit or any(d.get("activity_id") and d["activity_id"] in query for d in docs)
    if not explicit: return []
    scores = BM25Okapi([tokens(d["title"] + " " + d["content"]) or ["_"] for d in docs]).get_scores(tokens(query))
    return [{"id": d["id"], "score": float(score)} for d, score in sorted(zip(docs, scores), key=lambda p: p[1], reverse=True)
            if float(score) > 0][:10]

def rankings(docs, original, rewritten, subqueries, vectors, mode):
    def dense(query, field="embedding", top=10):
        return [{"id": d["id"], "score": score} for d, score in sorted(
            [(d, cosine(vectors[query], d[field])) for d in docs if d.get(field) is not None],
            key=lambda pair: (-pair[1], pair[0]["id"]))[:top]]
    routes = {"original": [dense(original, top=20)]}
    if mode in ("rewrite", "subquestions", "adaptive") and rewritten and rewritten != original:
        routes["rewrite"] = [dense(rewritten)]
    if mode in ("subquestions", "adaptive"):
        distinct = [q for q in dict.fromkeys(subqueries) if q not in (original, rewritten)]
        if distinct: routes["subquestions"] = [dense(q) for q in distinct]
    if mode == "adaptive":
        title = dense(rewritten or original, "title_embedding")
        if title: routes["title"] = [title]
        lexical = lexical_rank(docs, rewritten or original)
        if lexical: routes["lexical"] = [lexical]
    weights = {"original": 2, "rewrite": 1, "subquestions": 1, "title": .5, "lexical": .5}
    fused = {}
    for name, lists in routes.items():
        for ranking in lists:
            for rank, item in enumerate(ranking, 1):
                fused[item["id"]] = fused.get(item["id"], 0) + weights[name] / len(lists) / (60 + rank)
    protected = [x["id"] for x in routes["original"][0]]
    extra = sorted((key for key in fused if key not in protected), key=lambda key: (-fused[key], key))
    candidates = protected + extra[:max(0, 40 - len(protected))]
    candidates.sort(key=lambda key: (-fused[key], key))
    return routes, candidates

async def retrieve(consumer, order_id, original, rewritten, subqueries, scope, budget, mode=None):
    started = time.monotonic(); budget.tool()
    mode = mode or ("adaptive" if settings()["adaptive_retrieval"] else "dense")
    if mode not in ("dense", "dense_rerank", "rewrite", "subquestions", "adaptive"):
        raise ValueError("Unknown retrieval experiment")
    async with database.Session.begin() as db:
        order = await db.get(Order, order_id)
        if not order or order.consumer_id != consumer:
            raise DomainError("not_authorized", "无权访问该订单。", 404)
        if order.merchant_id in config()["disabled_merchants"]:
            raise DomainError("scope_disabled", "该商户的自动处理当前暂停。", 503)
        versions, precision = await temporal.resolve_versions(db, order, scope)
        docs = [{**temporal.version_view(v), "embedding": list(v.embedding) if v.embedding is not None else None,
                 "title_embedding": list(v.title_embedding) if v.title_embedding is not None else None,
                 "activity_id": (v.applicability or {}).get("activity_id")} for v in versions]
    queries = [original]
    if mode in ("rewrite", "subquestions", "adaptive") and rewritten: queries.append(rewritten)
    if mode in ("subquestions", "adaptive"): queries.extend(subqueries)
    trace = {"mode": mode, "scope": scope, "order_id": order_id, "original_query": original,
             "rewritten_query": rewritten, "subqueries": subqueries, "time_precision": precision}
    try:
        if any(d["embedding"] is None for d in docs):
            raise DomainError("knowledge_not_indexed", "适用资料尚未完成索引。", 503)
        vectors = await query_vectors(queries, budget)
        routes, candidates = await asyncio.to_thread(rankings, docs, original, rewritten, subqueries, vectors, mode)
        trace["query_vector_ids"] = [query_key(q) for q in dict.fromkeys(queries)]
    except DomainError as error:
        if error.code in ("turn_timeout", "turn_cancelled", "turn_budget", "credit_budget", "model_budget"): raise
        lexical = await asyncio.to_thread(lexical_rank, docs, rewritten or original)
        if not lexical: raise
        routes, candidates = {"lexical": [lexical]}, [r["id"] for r in lexical]
        trace["degraded"] = error.code
    trace["rankings"], trace["candidate_ids"] = routes, candidates
    by_id = {d["id"]: d for d in docs}
    if mode != "dense" and "original" in routes:
        budget.tool()
        try:
            reranked = await runtime.rerank(rewritten or original, [by_id[k]["content"] for k in candidates], budget)
            chosen = [candidates[r["index"]] for r in reranked]
            trace["reranked"] = [{"id": candidates[r["index"]], "score": r["score"]} for r in reranked]
        except DomainError as error:
            if error.code in ("turn_timeout", "turn_cancelled", "turn_budget", "credit_budget", "model_budget"): raise
            chosen = [r["id"] for r in routes["original"][0]][:5]
            trace["rerank_degraded"] = error.code
    else:
        chosen = candidates[:5]
    trace["selected_ids"] = chosen
    trace["retrieval_ms"] = int((time.monotonic() - started) * 1000)
    return [{k: v for k, v in by_id[key].items() if k not in ("embedding", "title_embedding", "activity_id")}
            for key in chosen], trace

async def index_versions():
    async with database.Session() as db:
        rows = [(v.id, v.content, v.title + " " + json_fields(v.rules), v.embedding is None, v.title_embedding is None)
                for v in (await db.scalars(select(KnowledgeVersion).where(KnowledgeVersion.status == "published"))).all()
                if v.embedding is None or v.title_embedding is None]
    for key, content, title, needs_content, needs_title in rows:
        texts = ([content] if needs_content else []) + ([title] if needs_title else [])
        vectors = await runtime.embed(texts, runtime.Budget("index-v2-" + key))
        async with database.Session.begin() as db:
            row = await db.get(KnowledgeVersion, key)
            if needs_content: row.embedding = vectors.pop(0)
            if needs_title: row.title_embedding = vectors.pop(0)
    return len(rows)

def json_fields(rules):
    return " ".join(str(k) + " " + str(v) for k, v in (rules or {}).items())
