"""
한국은행 ECOS API 수집기 — 100대 통계지표(KeyStatisticList)

기준금리·시장금리·환율·물가·통화량 등 거시경제 핵심 지표를 받아온다.
결과: data/bok/keystat.json (ES 적재/임베딩용 문서 배열)

API 키는 코드에 적지 않는다. 환경변수 BOK_API_KEY 에서만 읽는다.
실행: python fetch_bok.py
"""

import os
import json

import httpx

API_KEY = os.environ.get("BOK_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "환경변수 BOK_API_KEY 가 없습니다.\n"
        "  PowerShell(임시):  $env:BOK_API_KEY=\"여기에_키\"\n"
        "  PowerShell(영구):  setx BOK_API_KEY \"여기에_키\"  (새 창부터 적용)"
    )

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "bok")
os.makedirs(OUT_DIR, exist_ok=True)


def fetch_keystat():
    """100대 통계지표 전체를 받아 문서 리스트로 반환."""
    url = f"http://ecos.bok.or.kr/api/KeyStatisticList/{API_KEY}/json/kr/1/200"
    r = httpx.get(url, timeout=30.0, follow_redirects=True)
    r.raise_for_status()
    j = r.json()
    if "RESULT" in j:  # 에러 응답
        raise RuntimeError(f"ECOS 오류: {j['RESULT']}")
    rows = j["KeyStatisticList"]["row"]

    docs = []
    for i, row in enumerate(rows, 1):
        cls = (row.get("CLASS_NAME") or "").strip()
        name = (row.get("KEYSTAT_NAME") or "").strip()
        docs.append({
            "doc_id": f"bok_{i:03d}_{name}",
            "source": "한국은행 ECOS",
            "class_name": cls,
            "keystat_name": name,
            "data_value": row.get("DATA_VALUE"),
            "unit_name": (row.get("UNIT_NAME") or "").strip(),
            "cycle": row.get("CYCLE"),
        })
    return docs


def main():
    docs = fetch_keystat()
    path = os.path.join(OUT_DIR, "keystat.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
    print(f"한국은행 100대 지표: {len(docs)}건 → {path}")
    print("예시:")
    for d in docs[:5]:
        print(f"  {d['class_name']} / {d['keystat_name']}: {d['data_value']} {d['unit_name']} ({d['cycle']})")


if __name__ == "__main__":
    main()
