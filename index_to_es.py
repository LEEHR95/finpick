"""
수집한 금융감독원 데이터(data/fss/fss_all.json)를 Elasticsearch에 적재한다.

  - 인덱스: fss-products
  - 문서 _id: doc_id (재실행해도 중복 없이 덮어씀)
  - options(기간별 금리)는 nested 매핑 → 기간·금리 조건 검색 가능

연결 주소는 환경변수 ES_URL(기본 http://localhost:9200)에서 읽는다.
실행: python index_to_es.py
"""

import os
import json

from elasticsearch import Elasticsearch, helpers

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
INDEX = "fss-products"
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "data", "fss", "fss_all.json")

MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        # options 안의 숫자는 문서마다 정수/소수가 섞여 와 충돌 → 전부 double로 통일
        "dynamic_templates": [
            {"options_ints_as_double": {
                "path_match": "options.*",
                "match_mapping_type": "long",
                "mapping": {"type": "double"},
            }},
            {"options_floats_as_double": {
                "path_match": "options.*",
                "match_mapping_type": "double",
                "mapping": {"type": "double"},
            }},
        ],
        "properties": {
            "doc_id": {"type": "keyword"},
            "product_type": {"type": "keyword"},
            "fin_grp_no": {"type": "keyword"},
            "fin_grp_nm": {"type": "keyword"},
            "fin_co_no": {"type": "keyword"},
            "kor_co_nm": {"type": "keyword"},
            "fin_prdt_cd": {"type": "keyword"},
            "fin_prdt_nm": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "dcls_month": {"type": "keyword"},
            "join_way": {"type": "text"},
            "spcl_cnd": {"type": "text"},
            "etc_note": {"type": "text"},
            "mtrt_int": {"type": "text"},
            "join_member": {"type": "text"},
            "max_limit": {"type": "double"},
            # 기간별 금리/대출금리 옵션 → 중첩 문서로 보관
            "options": {"type": "nested"},
        }
    },
}


def main():
    es = Elasticsearch(ES_URL, request_timeout=60)
    info = es.info()
    print(f"ES 연결: {ES_URL} (버전 {info['version']['number']})")

    with open(DATA_PATH, encoding="utf-8") as f:
        docs = json.load(f)
    print(f"적재 대상: {len(docs)}건  ({DATA_PATH})")

    # 인덱스 재생성 (재실행 시 깨끗하게)
    if es.indices.exists(index=INDEX):
        es.indices.delete(index=INDEX)
    es.indices.create(index=INDEX, **MAPPING)

    actions = ({"_index": INDEX, "_id": d["doc_id"], "_source": d} for d in docs)
    ok, errors = helpers.bulk(es, actions, chunk_size=500, raise_on_error=False)
    print(f"적재 완료: 성공 {ok}건, 실패 {len(errors)}건")
    if errors:
        print("실패 예시:", json.dumps(errors[0], ensure_ascii=False)[:300])

    es.indices.refresh(index=INDEX)
    total = es.count(index=INDEX)["count"]
    print(f"인덱스 '{INDEX}' 문서 수: {total}")

    # 상품종류별 집계 확인
    agg = es.search(index=INDEX, size=0, aggs={
        "by_type": {"terms": {"field": "product_type", "size": 20}}
    })
    print("종류별:")
    for b in agg["aggregations"]["by_type"]["buckets"]:
        print(f"  {b['key']:8s}: {b['doc_count']}건")


if __name__ == "__main__":
    main()
