# -*- coding: utf-8 -*-
"""
데이터/인프라 테스트 — Elasticsearch & Redis.

ES : 클러스터 상태, 문서수 정합, 매핑, 임베딩 차원, 데이터 품질(누락·이상치),
     집계 정합, 키워드·의미검색 품질, 검색 성능.
Redis: 연결, 메모리 정책, TTL/만료, 직렬화 정합, 네임스페이스, 히트/미스, 성능.
"""
import json, time
from elasticsearch import Elasticsearch
import cache

if __name__ != "__main__":
    import pytest
    pytest.skip("script-style ES/Redis data audit; run directly with python test_data.py", allow_module_level=True)

es = Elasticsearch("http://localhost:9200", request_timeout=60)

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    cond = bool(cond)
    print(("  ✅" if cond else "  ❌") + f" {name}" + (f"  — {detail}" if detail else ""))
    PASS += cond; FAIL += (not cond)
def info(name, val):
    print(f"  ℹ️  {name}: {val}")


print("=" * 64)
print("[ Elasticsearch ]")

# --- 클러스터/인덱스 상태 ---
health = es.cluster.health()
check("클러스터 상태 not red", health["status"] != "red", health["status"])
for idx in ["fss-products", "bok-keystat"]:
    check(f"인덱스 '{idx}' 존재", es.indices.exists(index=idx))

# --- 문서 수 정합 (적재 소스 대비) ---
fss = json.load(open("data/fss/fss_all.json", encoding="utf-8"))
uniq = len({d["doc_id"] for d in fss})
es_cnt = es.count(index="fss-products")["count"]
check("fss 문서수 = 소스 고유 doc_id 수", es_cnt == uniq, f"ES {es_cnt} / 고유 {uniq} (원본 {len(fss)})")
bok = json.load(open("data/bok/keystat.json", encoding="utf-8"))
check("bok 문서수 = 소스 수", es.count(index="bok-keystat")["count"] == len(bok), f"{len(bok)}건")

# --- 매핑 검증 ---
m = es.indices.get_mapping(index="fss-products")["fss-products"]["mappings"]["properties"]
check("embedding = dense_vector", m["embedding"]["type"] == "dense_vector")
check("embedding 차원 = 4096", m["embedding"].get("dims") == 4096, str(m["embedding"].get("dims")))
check("options = nested", m["options"]["type"] == "nested")
check("kor_co_nm = keyword", m["kor_co_nm"]["type"] == "keyword")

# --- 임베딩 차원 일관성 (실제 문서) ---
one = es.search(index="fss-products", size=1, source=["embedding"])["hits"]["hits"][0]
check("실제 벡터 길이 = 4096", len(one["_source"]["embedding"]) == 4096, str(len(one["_source"]["embedding"])))

# --- 데이터 품질: 필수필드 누락 ---
missing = es.count(index="fss-products", query={"bool": {"should": [
    {"bool": {"must_not": {"exists": {"field": "kor_co_nm"}}}},
    {"bool": {"must_not": {"exists": {"field": "product_type"}}}},
], "minimum_should_match": 1}})["count"]
check("필수필드(회사·종류) 누락 0건", missing == 0, f"{missing}건")

# --- 데이터 품질: 금리 이상치 (예적금 0~20% 범위 밖) ---
outliers = []
for ptype in ["정기예금", "적금"]:
    r = es.search(index="fss-products", size=500, query={"term": {"product_type": ptype}},
                  source_excludes=["embedding"])
    for h in r["hits"]["hits"]:
        for o in h["_source"].get("options", []):
            v = o.get("intr_rate2")
            if v is not None and (v < 0 or v > 20):
                outliers.append((h["_source"]["fin_prdt_nm"], v))
check("예적금 금리 이상치(0~20% 밖) 0건", len(outliers) == 0, f"{len(outliers)}건")

# --- 집계 정합: 종류별 합 = 전체 ---
agg = es.search(index="fss-products", size=0,
                aggs={"t": {"terms": {"field": "product_type", "size": 20}}})
sumb = sum(b["doc_count"] for b in agg["aggregations"]["t"]["buckets"])
check("종류별 합 = 전체 문서수", sumb == es_cnt, f"{sumb}/{es_cnt}")

# --- 검색 품질: 키워드 ---
kw = es.search(index="fss-products", query={"match": {"fin_prdt_nm": "정기예금"}})
check("키워드검색 '정기예금' 결과>0", kw["hits"]["total"]["value"] > 0,
      f"{kw['hits']['total']['value']}건")

# --- 검색 품질: 의미검색 top-1이 기대 종류와 일치 ---
import rag_core
def knn_top(query, index="fss-products"):
    qv = rag_core.embed_query(query).tolist()
    r = es.search(index=index, size=1, knn={"field": "embedding", "query_vector": qv,
                  "k": 1, "num_candidates": 50}, source_excludes=["embedding"])
    return r["hits"]["hits"][0]["_source"]
t1 = knn_top("목돈 안전하게 굴리는 예금")
check("의미검색 '예금' → 예적금류", t1["product_type"] in ("정기예금", "적금"), t1["product_type"])
t2 = knn_top("내 집 마련 주택 대출")
check("의미검색 '주택대출' → 주택담보대출", t2["product_type"] == "주택담보대출", t2["product_type"])

# --- 검색 성능 ---
def ms(fn, n=5):
    ts = []
    for _ in range(n):
        s = time.perf_counter(); fn(); ts.append((time.perf_counter()-s)*1000)
    return sum(ts)/len(ts)
qv = rag_core.embed_query("이자 높은 적금").tolist()
lat_kw = ms(lambda: es.search(index="fss-products", query={"match": {"fin_prdt_nm": "적금"}}, size=5))
lat_knn = ms(lambda: es.search(index="fss-products", size=5,
             knn={"field": "embedding", "query_vector": qv, "k": 5, "num_candidates": 100}))
info("키워드 검색 평균 지연", f"{lat_kw:.1f} ms")
info("의미검색(kNN) 평균 지연", f"{lat_knn:.1f} ms")

print("\n[ Redis ]")
check("PING", cache.ping() is True)
cfg = {k: v for k, v in zip(["mp"], [None])}
# 메모리 정책
import redis as _redis
r = _redis.Redis.from_url(cache.REDIS_URL, decode_responses=True)
mm = r.config_get("maxmemory")["maxmemory"]
mp = r.config_get("maxmemory-policy")["maxmemory-policy"]
check("maxmemory = 256MB", mm == str(256*1024*1024), mm)
check("eviction = allkeys-lru", mp == "allkeys-lru", mp)

# 직렬화 정합 (한글·중첩 구조 round-trip)
sample = {"회사": "테스트은행", "금리": [2.5, 3.1], "옵션": {"기간": 12}}
cache.cache_set("test", sample, 60, "rt")
check("직렬화 round-trip 동일", cache.cache_get("test", "rt") == sample)

# TTL 설정 확인
key = cache._key("test", "ttlcheck")
r.set(key, "x", ex=50)
check("TTL 설정됨(0<ttl<=50)", 0 < r.ttl(key) <= 50, f"{r.ttl(key)}s")

# 만료 동작
r.set(cache._key("test", "exp"), "x", ex=1)
time.sleep(1.3)
check("1초 TTL 후 만료", cache.cache_get("test", "exp") is None)

# 네임스페이스
check("키 네임스페이스 'bankrag:' 사용", key.startswith("bankrag:"), key.split(":")[0])

# 히트/미스 카운터
cache.flush()
cache.cache_set("test", {"v": 1}, 60, "hm")
before = r.info()["keyspace_hits"]
cache.cache_get("test", "hm")            # 히트 1회
after = r.info()["keyspace_hits"]
check("히트 카운터 증가", after > before, f"{before}→{after}")

# 성능: 캐시 get 지연
cache.cache_set("test", {"v": list(range(100))}, 60, "perf")
lat_get = ms(lambda: cache.cache_get("test", "perf"), n=20)
info("Redis 캐시 get 평균 지연", f"{lat_get:.3f} ms")

cache.flush()
print("\n" + "=" * 64)
print(f"결과: {PASS} PASS / {FAIL} FAIL")
