"""
금융감독원(상품) + 한국은행(지표) 데이터를 임베딩하여 Elasticsearch에 적재.

각 문서에 대해:
  - search_text : 사람이 읽을 수 있는 요약 문장(임베딩 입력 + 키워드 검색용)
  - embedding   : Upstage 임베딩 벡터(dense_vector) → 의미검색(kNN) 가능

인덱스:
  - fss-products : 금융상품 (nested options + embedding)
  - bok-keystat  : 한국은행 100대 지표 (+ embedding)

키는 환경변수에서만: UPSTAGE_API_KEY(임베딩), ES_URL(기본 localhost:9200).
실행: python embed_index.py
"""

import os
import json

from elasticsearch import Elasticsearch, helpers

import rag_core  # Upstage 임베딩 재사용 (embed, EMBED_PASSAGE_MODEL, client)

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

if rag_core.client is None:
    raise RuntimeError("UPSTAGE_API_KEY 환경변수가 없어 임베딩할 수 없습니다.")


# --------------------------- 요약 문장 만들기 ---------------------------
def fss_text(d):
    """금융상품 1건을 임베딩용 요약 문장으로."""
    rate_parts, seen = [], set()
    for o in d.get("options", []):
        trm = o.get("save_trm")
        rate = o.get("intr_rate2") if o.get("intr_rate2") is not None else o.get("intr_rate")
        if trm and rate is not None and trm not in seen:
            rate_parts.append(f"{trm}개월 {rate}%")
            seen.add(trm)
    parts = [
        f"{d.get('kor_co_nm','')} {d.get('fin_prdt_nm','')}".strip(),
        f"종류 {d.get('product_type','')} ({d.get('fin_grp_nm','')})",
    ]
    for label, key in [("가입방법", "join_way"), ("가입대상", "join_member"),
                       ("우대조건", "spcl_cnd"), ("기타", "etc_note")]:
        v = (d.get(key) or "").strip().replace("\n", " ")
        if v:
            parts.append(f"{label}: {v}")
    if rate_parts:
        parts.append("금리: " + ", ".join(rate_parts))
    return ". ".join(parts)


def bok_text(d):
    """한국은행 지표 1건을 임베딩용 요약 문장으로."""
    return (f"{d.get('class_name','')} {d.get('keystat_name','')}: "
            f"{d.get('data_value','')} {d.get('unit_name','')} "
            f"(기준시점 {d.get('cycle','')}). 한국은행 통계지표.")


# --------------------------- 인덱스 매핑 ---------------------------
def fss_mapping(dim):
    return {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic_templates": [
                {"opt_int": {"path_match": "options.*", "match_mapping_type": "long",
                             "mapping": {"type": "double"}}},
                {"opt_float": {"path_match": "options.*", "match_mapping_type": "double",
                               "mapping": {"type": "double"}}},
            ],
            "properties": {
                "doc_id": {"type": "keyword"},
                "product_type": {"type": "keyword"},
                "fin_grp_nm": {"type": "keyword"},
                "fin_grp_no": {"type": "keyword"},
                "kor_co_nm": {"type": "keyword"},
                "fin_co_no": {"type": "keyword"},
                "fin_prdt_cd": {"type": "keyword"},
                "fin_prdt_nm": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "dcls_month": {"type": "keyword"},
                "join_way": {"type": "text"}, "spcl_cnd": {"type": "text"},
                "etc_note": {"type": "text"}, "mtrt_int": {"type": "text"},
                "join_member": {"type": "text"}, "max_limit": {"type": "double"},
                "options": {"type": "nested"},
                "search_text": {"type": "text"},
                "embedding": {"type": "dense_vector", "dims": dim,
                              "index": True, "similarity": "cosine"},
            },
        },
    }


def bok_mapping(dim):
    return {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"properties": {
            "doc_id": {"type": "keyword"},
            "source": {"type": "keyword"},
            "class_name": {"type": "keyword"},
            "keystat_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "data_value": {"type": "keyword"},
            "unit_name": {"type": "keyword"},
            "cycle": {"type": "keyword"},
            "search_text": {"type": "text"},
            "embedding": {"type": "dense_vector", "dims": dim,
                          "index": True, "similarity": "cosine"},
        }},
    }


# --------------------------- 적재 ---------------------------
def embed_docs(docs, text_fn):
    """docs에 search_text/embedding 필드를 채운다 (배치 임베딩)."""
    texts = [text_fn(d) for d in docs]
    vecs = rag_core.embed(texts, rag_core.EMBED_PASSAGE_MODEL)  # 96개씩 자동 배치
    for d, t, v in zip(docs, texts, vecs):
        d["search_text"] = t
        d["embedding"] = v.tolist()
    return len(vecs[0]) if vecs else 0


def index_dataset(es, index, mapping_fn, docs, text_fn):
    print(f"\n[{index}] 임베딩 중... ({len(docs)}건)")
    dim = embed_docs(docs, text_fn)
    print(f"  임베딩 차원: {dim}")
    if es.indices.exists(index=index):
        es.indices.delete(index=index)
    es.indices.create(index=index, **mapping_fn(dim))
    actions = ({"_index": index, "_id": d["doc_id"], "_source": d} for d in docs)
    ok, errors = helpers.bulk(es, actions, chunk_size=200, raise_on_error=False)
    es.indices.refresh(index=index)
    total = es.count(index=index)["count"]
    print(f"  적재 완료: 성공 {ok}, 실패 {len(errors)} → 문서 {total}건")
    if errors:
        print("  실패 예시:", json.dumps(errors[0], ensure_ascii=False)[:300])


def main():
    es = Elasticsearch(ES_URL, request_timeout=120)
    print("ES:", ES_URL, "/ 버전", es.info()["version"]["number"])

    fss = json.load(open(os.path.join(DATA_DIR, "fss", "fss_all.json"), encoding="utf-8"))
    bok = json.load(open(os.path.join(DATA_DIR, "bok", "keystat.json"), encoding="utf-8"))

    index_dataset(es, "fss-products", fss_mapping, fss, fss_text)
    index_dataset(es, "bok-keystat", bok_mapping, bok, bok_text)
    print("\n전체 완료. 의미검색(kNN) 사용 가능.")


if __name__ == "__main__":
    main()
