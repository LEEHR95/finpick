# -*- coding: utf-8 -*-
"""지금 넣은 데이터로 알 수 있는 것들 — 실제 ES 질의 예시."""
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", request_timeout=60)


def fetch(product_type, size=500):
    r = es.search(index="fss-products", size=size,
                  query={"term": {"product_type": product_type}},
                  source_excludes=["embedding"])
    return [h["_source"] for h in r["hits"]["hits"]]


def best_deposit_rate(d, term="12"):
    rates = [o.get("intr_rate2") for o in d.get("options", [])
             if o.get("save_trm") == term and o.get("intr_rate2") is not None]
    return max(rates) if rates else None


def lowest_loan_rate(d):
    vals = []
    for o in d.get("options", []):
        for k in ("lend_rate_min", "lend_rate_avg"):
            v = o.get(k)
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                pass
    return min(vals) if vals else None


print("=" * 60)
print("1) 12개월 정기예금 최고금리 TOP5")
deps = [(d, best_deposit_rate(d)) for d in fetch("정기예금")]
for d, r in sorted([x for x in deps if x[1]], key=lambda x: -x[1])[:5]:
    print(f"   {r:.2f}%  {d['kor_co_nm']} / {d['fin_prdt_nm']}")

print("\n2) 12개월 적금 최고금리 TOP5")
savs = [(d, best_deposit_rate(d)) for d in fetch("적금")]
for d, r in sorted([x for x in savs if x[1]], key=lambda x: -x[1])[:5]:
    print(f"   {r:.2f}%  {d['kor_co_nm']} / {d['fin_prdt_nm']}")

print("\n3) 주택담보대출 최저금리 TOP5")
mort = [(d, lowest_loan_rate(d)) for d in fetch("주택담보대출")]
for d, r in sorted([x for x in mort if x[1]], key=lambda x: x[1])[:5]:
    print(f"   {r:.2f}%  {d['kor_co_nm']} / {d['fin_prdt_nm']}")

print("\n4) 한국은행 핵심 지표")
r = es.search(index="bok-keystat", size=200, query={"match_all": {}},
              source_excludes=["embedding"])
want = ["한국은행 기준금리", "원/달러 환율(종가)", "예금은행 대출금리",
        "예금은행 수신금리", "소비자물가지수"]
by_name = {h["_source"]["keystat_name"]: h["_source"] for h in r["hits"]["hits"]}
base_rate = None
for name in want:
    s = by_name.get(name)
    if s:
        print(f"   {name}: {s['data_value']} {s['unit_name']} ({s['cycle']})")
        if name == "한국은행 기준금리":
            base_rate = float(s["data_value"])

print("\n5) 인사이트: 기준금리 대비 예금 최고금리")
if base_rate and deps:
    top = max(r for _, r in deps if r)
    print(f"   한국은행 기준금리 {base_rate}% 인데, 정기예금 최고는 {top:.2f}%")
    print(f"   → 기준금리보다 약 {top - base_rate:.2f}%p 높은 예금이 존재")
