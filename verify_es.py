# -*- coding: utf-8 -*-
"""ES 적재 검증용 임시 스크립트."""
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", request_timeout=60)
print("총 문서:", es.count(index="fss-products")["count"])

# 1) 한글 키워드 검색
r = es.search(index="fss-products", size=3, query={"match": {"fin_prdt_nm": "정기예금"}})
print(f"\n[검색] 상품명에 '정기예금' (총 {r['hits']['total']['value']}건) 상위 3:")
for h in r["hits"]["hits"]:
    s = h["_source"]
    print(f"  - {s['kor_co_nm']} / {s['fin_prdt_nm']} ({s['product_type']})")

# 2) nested 금리: 12개월 정기예금 최고금리 TOP5
r = es.search(index="fss-products", size=5,
    query={"term": {"product_type": "정기예금"}},
    sort=[{"options.intr_rate2": {"order": "desc", "mode": "max",
        "nested": {"path": "options", "filter": {"term": {"options.save_trm": "12"}}}}}])
print("\n[검색] 12개월 정기예금 최고금리 TOP5:")
for h in r["hits"]["hits"]:
    s = h["_source"]
    best = max((o.get("intr_rate2") or 0) for o in s["options"] if o.get("save_trm") == "12")
    print(f"  - {s['kor_co_nm']} / {s['fin_prdt_nm']} : {best}%")
