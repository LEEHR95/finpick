# -*- coding: utf-8 -*-
"""
측정 기반 기능 추가 실험 하네스.

같은 데이터(fss-products)로 4가지 구성을 비교한다.
  A. 인메모리 numpy 코사인 (원조 RAG 방식)   — 임베딩 필요, 캐시 없음
  B. ES 키워드 검색(BM25)                    — 임베딩 불필요, 캐시 없음
  C. ES 의미검색(kNN)                        — 임베딩 필요, 캐시 없음
  D. ES 의미검색 + Redis 캐시                 — 임베딩 필요(히트 시 생략)

측정 축: ① 응답시간 ② 유지비(메모리/저장) ③ 질문당 임베딩 호출 ④ 검색 품질(recall@5)
결과는 REPORT_experiment.md 로 저장.
"""
import time, json, subprocess
import statistics as st
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch

import rag_core, cache, search_es

es = Elasticsearch("http://localhost:9200", request_timeout=120)

QUERIES = [
    "안전하게 목돈 굴리기 좋은 예금",
    "이자 높은 적금",
    "주택 구입 자금 대출",
    "전세 보증금 마련 대출",
    "노후 대비 연금 상품",
]
LABELED = [
    ("목돈 안전하게 굴리는 예금", {"정기예금", "적금"}),
    ("이자 높은 적금", {"적금"}),
    ("내 집 마련 주택담보대출", {"주택담보대출"}),
    ("전세 보증금 마련 대출", {"전세자금대출"}),
    ("노후 대비 연금저축", {"연금저축"}),
    ("급할 때 신용대출", {"개인신용대출"}),
]


# ---------- 인메모리 베이스라인(A): ES에서 임베딩 끌어와 numpy 매트릭스 구성 ----------
print("인메모리 매트릭스 적재 중...")
res = es.search(index="fss-products", size=2000, query={"match_all": {}},
                source=["fin_prdt_nm", "kor_co_nm", "product_type", "embedding"])
DOCS, vecs = [], []
for h in res["hits"]["hits"]:
    s = h["_source"]
    DOCS.append({"name": s["fin_prdt_nm"], "co": s["kor_co_nm"], "type": s["product_type"]})
    vecs.append(s["embedding"])
MAT = np.array(vecs, dtype=np.float32)
MAT = MAT / (np.linalg.norm(MAT, axis=1, keepdims=True) + 1e-9)
print(f"  메모리 매트릭스: {MAT.shape}  ({MAT.nbytes/1024/1024:.1f} MB)")


def search_numpy(qvec, k=5):
    q = np.asarray(qvec, dtype=np.float32); q = q / (np.linalg.norm(q) + 1e-9)
    sims = MAT @ q
    return [DOCS[i] for i in np.argsort(-sims)[:k]]

def search_keyword(query, k=5):
    return es.search(index="fss-products", size=k,
                     query={"multi_match": {"query": query,
                            "fields": ["search_text", "fin_prdt_nm"]}},
                     source_excludes=["embedding"])["hits"]["hits"]

def search_knn(qvec, k=5):
    return es.search(index="fss-products", size=k,
                     knn={"field": "embedding", "query_vector": qvec,
                          "k": k, "num_candidates": 100},
                     source_excludes=["embedding"])["hits"]["hits"]


# --------------------------- ① 응답시간 ---------------------------
def t_ms(fn):
    t = time.perf_counter(); fn(); return (time.perf_counter() - t) * 1000

rows, emb_costs = [], []
for q in QUERIES:
    # A: (cold)임베딩 + numpy
    rag_core.embed_query.cache_clear()
    t = time.perf_counter(); v = rag_core.embed_query(q); emb = (time.perf_counter() - t) * 1000
    emb_costs.append(emb)
    a = emb + t_ms(lambda: search_numpy(v.tolist()))
    # B: 키워드(임베딩 없음)
    b = t_ms(lambda: search_keyword(q))
    # C: (cold)임베딩 + kNN
    rag_core.embed_query.cache_clear()
    t = time.perf_counter(); v = rag_core.embed_query(q); embc = (time.perf_counter() - t) * 1000
    c = embc + t_ms(lambda: search_knn(v.tolist()))
    # D: 캐시 히트
    cache.flush()
    search_es.semantic_search(q, use_cache=True)             # 미스(저장)
    d = t_ms(lambda: search_es.semantic_search(q, use_cache=True))  # 히트
    rows.append({"질문": q[:14], "A 메모리numpy": round(a, 1), "B ES키워드": round(b, 1),
                 "C ES의미검색": round(c, 1), "D +캐시(히트)": round(d, 1)})

df_lat = pd.DataFrame(rows)
avg = {"질문": "평균",
       "A 메모리numpy": round(df_lat["A 메모리numpy"].mean(), 1),
       "B ES키워드": round(df_lat["B ES키워드"].mean(), 1),
       "C ES의미검색": round(df_lat["C ES의미검색"].mean(), 1),
       "D +캐시(히트)": round(df_lat["D +캐시(히트)"].mean(), 1)}
df_lat = pd.concat([df_lat, pd.DataFrame([avg])], ignore_index=True)
emb_avg = st.mean(emb_costs)

print("\n① 응답시간 (ms, 질문별 end-to-end)")
print(df_lat.to_string(index=False))
print(f"\n   ※ 이 중 '질문 임베딩(Upstage 호출)'만 평균 {emb_avg:.1f} ms — A·C의 대부분을 차지(병목)")


# --------------------------- ② 유지비(리소스) ---------------------------
stats = es.indices.stats(index="fss-products")["indices"]["fss-products"]["total"]
idx_bytes = stats["store"]["size_in_bytes"]
rinfo = cache._r.info()

def docker_mem(name):
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name],
                             capture_output=True, text=True, timeout=30)
        return out.stdout.strip() or "n/a"
    except Exception:
        return "n/a"

df_cost = pd.DataFrame([
    {"항목": "ES 인덱스 크기(fss-products)", "값": f"{idx_bytes/1024/1024:.1f} MB"},
    {"항목": "ES 컨테이너 메모리", "값": docker_mem("es-bank")},
    {"항목": "Redis 사용 메모리", "값": rinfo.get("used_memory_human")},
    {"항목": "Redis 컨테이너 메모리", "값": docker_mem("redis-bank")},
    {"항목": "인메모리 매트릭스(A)", "값": f"{MAT.nbytes/1024/1024:.1f} MB ({MAT.shape[0]}×{MAT.shape[1]})"},
])
print("\n② 유지비 (리소스)")
print(df_cost.to_string(index=False))


# --------------------------- ③ 질문당 임베딩 호출 ---------------------------
df_call = pd.DataFrame([
    {"구성": "A 메모리numpy", "질문당 임베딩 호출": 1, "확장성": "낮음(전체 메모리 적재)"},
    {"구성": "B ES키워드", "질문당 임베딩 호출": 0, "확장성": "높음"},
    {"구성": "C ES의미검색", "질문당 임베딩 호출": 1, "확장성": "높음"},
    {"구성": "D +캐시", "질문당 임베딩 호출": "0~1(히트시 0)", "확장성": "높음"},
])
print("\n③ 질문당 임베딩 API 호출(=비용)")
print(df_call.to_string(index=False))


# --------------------------- ④ 검색 품질 (recall@5) ---------------------------
def recall_at5(search_fn_types):
    hit = 0
    for q, expected in LABELED:
        types = search_fn_types(q)
        if any(t in expected for t in types[:5]):
            hit += 1
    return hit / len(LABELED) * 100

def kw_types(q):
    return [h["_source"]["product_type"] for h in search_keyword(q, k=5)]
def knn_types(q):
    v = rag_core.embed_query(q).tolist()
    return [h["_source"]["product_type"] for h in search_knn(v, k=5)]

r_kw = recall_at5(kw_types)
r_knn = recall_at5(knn_types)
df_q = pd.DataFrame([
    {"검색방식": "B 키워드(BM25)", "recall@5": f"{r_kw:.0f}%"},
    {"검색방식": "C/D 의미검색(kNN)", "recall@5": f"{r_knn:.0f}%"},
])
print("\n④ 검색 품질 (라벨 질문 6개 기준)")
print(df_q.to_string(index=False))


# --------------------------- 리포트 저장 ---------------------------
def md_table(df):
    try:
        return df.to_markdown(index=False)
    except Exception:
        return "```\n" + df.to_string(index=False) + "\n```"

lines = ["# 측정 기반 기능 추가 실험 리포트\n",
         "같은 데이터(fss-products, 1,384건)로 구성 A~D를 비교한다.\n",
         "## ① 응답시간 (ms)\n", md_table(df_lat),
         f"\n> 질문 임베딩(Upstage)만 평균 **{emb_avg:.1f} ms** — A·C 응답시간의 대부분(병목).\n",
         "## ② 유지비(리소스)\n", md_table(df_cost),
         "\n## ③ 질문당 임베딩 호출(비용)\n", md_table(df_call),
         "\n## ④ 검색 품질(recall@5)\n", md_table(df_q),
         "\n## 해석 — 기능 추가의 양면\n",
         "- **B(키워드)**: 임베딩이 없어 가장 가볍고 비용 0. 단, 의미검색 불가 → 품질↓.",
         "- **C(의미검색)**: 품질↑(자연어 이해). 단 질문마다 임베딩 호출 → 응답시간·비용↑, 벡터 저장으로 인덱스 용량↑.",
         "- **A vs C**: 같은 임베딩을 쓰지만 검색 백엔드만 다름. 데이터가 작으면 numpy도 비슷, 커지면 ES가 유리(메모리 전체적재 불필요).",
         "- **D(+캐시)**: 반복 질문에서 임베딩·검색을 건너뛰어 응답시간 급감. 단 Redis 메모리·캐시 무효화 관리 비용 추가.",
         "\n**결론**: 기능을 추가하면 한 지표(품질/속도)는 좋아지지만 다른 지표(비용/유지비/복잡도)는 올라간다. "
         "병목(여기선 임베딩 호출)을 측정으로 먼저 찾고, 그걸 겨냥한 기능(캐시)을 붙일 때 효과가 가장 크다."]
with open("REPORT_experiment.md", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print("\n리포트 저장: REPORT_experiment.md")
