"""
금융감독원 금융상품통합비교공시 API 수집기 (finlife.fss.or.kr)

전 상품종류 × 전 금융권을 받아와, Elasticsearch에 넣기 좋은 형태로 저장한다.
  - 문서 1개 = 금융상품 1건 (기본정보 + 금리옵션 배열 중첩)
  - 결과: data/fss/<상품종류>.json (배열), data/fss/fss_all.json (전체),
          data/fss/fss_bulk.ndjson (ES _bulk 인덱싱용)

API 키는 코드에 적지 않는다. 환경변수 FSS_API_KEY 에서만 읽는다.
실행: python fetch_fss.py
"""

import os
import json
import time

import httpx

API_KEY = os.environ.get("FSS_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "환경변수 FSS_API_KEY 가 없습니다.\n"
        "  PowerShell(임시):  $env:FSS_API_KEY=\"여기에_키\"\n"
        "  PowerShell(영구):  setx FSS_API_KEY \"여기에_키\"  (새 창부터 적용)"
    )

BASE_URL = "http://finlife.fss.or.kr/finlifeapi"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "fss")
os.makedirs(OUT_DIR, exist_ok=True)

# 상품종류: (이름, API 엔드포인트)
PRODUCTS = [
    ("정기예금", "depositProductsSearch"),
    ("적금", "savingProductsSearch"),
    ("연금저축", "annuitySavingProductsSearch"),
    ("주택담보대출", "mortgageLoanProductsSearch"),
    ("전세자금대출", "rentHouseLoanProductsSearch"),
    ("개인신용대출", "creditLoanProductsSearch"),
]

# 금융권 코드(topFinGrpNo)
FIN_GROUPS = {
    "020000": "은행",
    "030300": "저축은행",
    "030200": "여신전문",
    "050000": "보험",
    "060000": "금융투자",
}


def fetch_pages(endpoint, top_grp):
    """한 엔드포인트×금융권의 모든 페이지를 받아 (baseList, optionList) 누적 반환."""
    base_all, opt_all = [], []
    page = 1
    while True:
        url = (f"{BASE_URL}/{endpoint}.json"
               f"?auth={API_KEY}&topFinGrpNo={top_grp}&pageNo={page}")
        r = httpx.get(url, timeout=30.0, follow_redirects=True)
        r.raise_for_status()
        result = (r.json() or {}).get("result", {})
        err = str(result.get("err_cd", ""))
        if err and err != "000":
            # 해당 금융권에 그 상품이 없으면 에러가 날 수 있음 → 조용히 건너뜀
            break
        base_all.extend(result.get("baseList") or [])
        opt_all.extend(result.get("optionList") or [])
        max_page = int(result.get("max_page_no") or 1)
        if page >= max_page:
            break
        page += 1
        time.sleep(0.1)  # 과도한 호출 방지
    return base_all, opt_all


def build_documents(prod_name, grp_no, grp_name, base_list, opt_list):
    """baseList + optionList 를 (회사+상품코드)로 합쳐 ES 문서 리스트로."""
    opts_by_key = {}
    for o in opt_list:
        key = (o.get("fin_co_no"), o.get("fin_prdt_cd"))
        opts_by_key.setdefault(key, []).append(o)

    docs = []
    for b in base_list:
        key = (b.get("fin_co_no"), b.get("fin_prdt_cd"))
        doc = dict(b)  # 원본 기본정보 필드 전부 보존
        doc["product_type"] = prod_name
        doc["fin_grp_no"] = grp_no
        doc["fin_grp_nm"] = grp_name
        doc["doc_id"] = f"{prod_name}_{b.get('fin_co_no')}_{b.get('fin_prdt_cd')}"
        doc["options"] = opts_by_key.get(key, [])
        docs.append(doc)
    return docs


def main():
    all_docs = []
    summary = []
    for prod_name, endpoint in PRODUCTS:
        prod_docs = []
        for grp_no, grp_name in FIN_GROUPS.items():
            try:
                base_list, opt_list = fetch_pages(endpoint, grp_no)
            except Exception as e:
                print(f"  [실패] {prod_name}/{grp_name}: {e}", flush=True)
                continue
            if not base_list:
                continue
            docs = build_documents(prod_name, grp_no, grp_name, base_list, opt_list)
            prod_docs.extend(docs)
            print(f"  {prod_name:8s} / {grp_name:6s} : 상품 {len(docs):4d}건", flush=True)
        # 상품종류별 파일
        path = os.path.join(OUT_DIR, f"{prod_name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(prod_docs, f, ensure_ascii=False, indent=2)
        all_docs.extend(prod_docs)
        summary.append((prod_name, len(prod_docs)))

    # 전체 합본
    with open(os.path.join(OUT_DIR, "fss_all.json"), "w", encoding="utf-8") as f:
        json.dump(all_docs, f, ensure_ascii=False, indent=2)

    # ES _bulk 인덱싱용 NDJSON (인덱스명: fss-products)
    with open(os.path.join(OUT_DIR, "fss_bulk.ndjson"), "w", encoding="utf-8") as f:
        for d in all_docs:
            f.write(json.dumps({"index": {"_index": "fss-products", "_id": d["doc_id"]}},
                               ensure_ascii=False) + "\n")
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    print("\n=== 수집 요약 ===")
    for name, cnt in summary:
        print(f"  {name:8s}: {cnt}건")
    print(f"  총 {len(all_docs)}건 → {OUT_DIR}")


if __name__ == "__main__":
    main()
