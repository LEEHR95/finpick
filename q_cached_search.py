# -*- coding: utf-8 -*-
"""캐시 적용 의미검색 정확성 확인."""
import search_es, cache
cache.flush()
for q in ["주택 구입 자금 대출", "이자 높은 적금"]:
    res, hit = search_es.semantic_search(q, k=3)
    print(f"\n질문: {q}  (캐시히트={hit})")
    for x in res:
        print(f"  - {x['owner']} / {x['name']} [{x['product_type']}]")
