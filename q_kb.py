# -*- coding: utf-8 -*-
"""국민은행 예금 상품 조회 (ES 작동 확인용)."""
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", request_timeout=60)

# 회사명에 '국민'이 들어가는 정기예금/적금 상품 조회
r = es.search(index="fss-products", size=50, query={
    "bool": {
        "must": [{"match_phrase": {"kor_co_nm": "국민은행"}}],
        "filter": [{"terms": {"product_type": ["정기예금", "적금"]}}],
    }
})

total = r["hits"]["total"]["value"]
print(f"국민은행 예금/적금 상품: 총 {total}건\n")
for i, h in enumerate(r["hits"]["hits"], 1):
    s = h["_source"]
    # 12개월 금리(있으면)
    rates12 = [o.get("intr_rate2") for o in s.get("options", [])
               if o.get("save_trm") == "12" and o.get("intr_rate2") is not None]
    r12 = f" / 12개월 {max(rates12)}%" if rates12 else ""
    print(f"{i:2d}. [{s['product_type']}] {s['fin_prdt_nm']}{r12}")
