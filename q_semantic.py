# -*- coding: utf-8 -*-
"""의미검색(kNN) 동작 확인."""
import rag_core
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", request_timeout=60)


def knn(index, query, k=5):
    qv = rag_core.embed_query(query).tolist()
    r = es.search(index=index, size=k,
                  knn={"field": "embedding", "query_vector": qv,
                       "k": k, "num_candidates": 100},
                  source_excludes=["embedding"])
    print(f"\n[의미검색] index={index}  질문: \"{query}\"")
    for h in r["hits"]["hits"]:
        s = h["_source"]
        name = s.get("fin_prdt_nm") or s.get("keystat_name")
        extra = s.get("kor_co_nm", s.get("class_name", ""))
        print(f"  ({h['_score']:.3f}) {extra} / {name}")


knn("fss-products", "안전하게 목돈 굴리기 좋은 예금")
knn("fss-products", "내 집 마련 위한 주택 대출")
knn("bok-keystat", "대출 금리의 기준이 되는 한국은행 정책금리")
knn("bok-keystat", "달러 원화 환율")
