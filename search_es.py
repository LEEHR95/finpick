"""
ES 의미검색 + Redis 캐시 통합.

semantic_search(query, index, k, use_cache) 한 번이면:
  1) 질문 임베딩 (Redis 캐시)
  2) ES kNN 검색  (Redis 캐시)
캐시를 끄면(use_cache=False) 매번 새로 계산 → 비교 측정에 사용.

연결: ES_URL(기본 localhost:9200). 캐시 정책은 cache.py 참고.
"""

import os

from elasticsearch import Elasticsearch

import cache

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
_es = Elasticsearch(ES_URL, request_timeout=60)


def semantic_search(query, index="fss-products", k=5, use_cache=True):
    """질문(자연어)으로 의미검색. (결과리스트, 캐시히트여부) 반환."""
    if not use_cache:
        import rag_core
        qv = rag_core.embed_query(query).tolist()
        return _knn(index, qv, k), False

    # 1) 임베딩 캐시
    qv = cache.cached_embed_query(query)
    # 2) 검색 결과 캐시 (인덱스+질문+k 조합 키)
    result, hit = cache.cached_call(
        "search", cache.TTL_SEARCH,
        lambda: _knn(index, qv, k),
        index, query, k,
    )
    return result, hit


def _knn(index, query_vector, k):
    r = _es.search(index=index, size=k,
                   knn={"field": "embedding", "query_vector": query_vector,
                        "k": k, "num_candidates": 100},
                   source_excludes=["embedding"])
    out = []
    for h in r["hits"]["hits"]:
        s = h["_source"]
        out.append({
            "score": round(h["_score"], 4),
            "name": s.get("fin_prdt_nm") or s.get("keystat_name"),
            "owner": s.get("kor_co_nm") or s.get("class_name"),
            "product_type": s.get("product_type") or s.get("source"),
        })
    return out
