# -*- coding: utf-8 -*-
"""
캐시 적용 전/후 응답속도 비교 벤치마크 + 리포트(REPORT_cache.md) 자동 생성.

각 질문에 대해 semantic_search를 3가지 모드로 측정:
  - 캐시 미적용 : 매번 Upstage 임베딩 + ES 검색
  - 캐시 미스   : 캐시 적용 첫 호출(저장)
  - 캐시 히트   : 캐시 적용 두번째 호출(Redis에서 반환)
"""
import time
import statistics as st
from datetime import datetime

import search_es
import cache

QUERIES = [
    "안전하게 목돈 굴리기 좋은 예금",
    "이자 높은 적금",
    "주택 구입 자금 대출",
    "전세 보증금 마련 대출",
    "노후 대비 연금 상품",
]


def timed(fn):
    t0 = time.perf_counter()
    fn()
    return (time.perf_counter() - t0) * 1000  # ms


def main():
    print("Redis:", cache.ping(), "/ 벤치 시작")
    cache.flush()

    rows = []
    for q in QUERIES:
        nocache = timed(lambda: search_es.semantic_search(q, use_cache=False))
        miss = timed(lambda: search_es.semantic_search(q, use_cache=True))   # 첫 호출=미스
        hit = timed(lambda: search_es.semantic_search(q, use_cache=True))    # 둘째=히트
        rows.append((q, nocache, miss, hit))
        print(f"  {q[:18]:20s} 미적용 {nocache:7.1f} / 미스 {miss:7.1f} / 히트 {hit:6.1f} ms")

    avg_nc = st.mean(r[1] for r in rows)
    avg_miss = st.mean(r[2] for r in rows)
    avg_hit = st.mean(r[3] for r in rows)
    speedup = avg_nc / avg_hit
    saved = avg_nc - avg_hit

    # ---------------- 리포트 작성 ----------------
    lines = []
    lines.append("# Redis 캐시 적용 전/후 비교 리포트\n")
    lines.append(f"- 작성: {datetime.now():%Y-%m-%d %H:%M}")
    lines.append(f"- 측정 대상: ES 의미검색(질문 임베딩 + kNN) end-to-end 응답시간")
    lines.append(f"- 질문 수: {len(QUERIES)}개 / 인덱스: fss-products\n")

    lines.append("## 측정 결과 (질문별, ms)\n")
    lines.append("| 질문 | 캐시 미적용 | 캐시 미스(첫호출) | 캐시 히트 |")
    lines.append("|------|-----------:|----------------:|---------:|")
    for q, nc, ms, ht in rows:
        lines.append(f"| {q} | {nc:,.1f} | {ms:,.1f} | {ht:,.1f} |")
    lines.append(f"| **평균** | **{avg_nc:,.1f}** | **{avg_miss:,.1f}** | **{avg_hit:,.1f}** |\n")

    lines.append("## 요약\n")
    lines.append(f"- 캐시 미적용 평균: **{avg_nc:,.1f} ms**")
    lines.append(f"- 캐시 히트 평균  : **{avg_hit:,.1f} ms**")
    lines.append(f"- **속도 향상: 약 {speedup:,.0f}배** (질문당 평균 {saved:,.1f} ms 절약)")
    lines.append(f"- 캐시 미스(첫 호출)는 {avg_miss:,.1f} ms로 미적용과 비슷 — "
                 "저장 비용은 미미하고, 재요청부터 효과 발생\n")

    lines.append("## 캐시 정책\n")
    lines.append("| 항목 | 값 | 근거 |")
    lines.append("|------|-----|------|")
    lines.append(f"| 질문 임베딩 TTL | {cache.TTL_EMBED//86400}일 | 같은 질문의 의미 벡터는 변하지 않음 |")
    lines.append(f"| 검색결과 TTL | {cache.TTL_SEARCH//3600}시간 | 금리 데이터는 일 단위 갱신 → 신선도·속도 균형 |")
    lines.append("| 메모리 정책 | 256MB + allkeys-lru | 한도 초과 시 오래된 캐시부터 자동 삭제 |")
    lines.append(f"| 키 네임스페이스 | `{cache.KEY_PREFIX}:종류:해시` | 다른 앱과 충돌 방지 |")
    lines.append("| 무효화 | 데이터 재적재 시 `flush_kind('search')` | 금리 갱신분 즉시 반영 |\n")

    lines.append("## 결론\n")
    lines.append(f"질문 임베딩(Upstage API 호출)이 응답시간의 대부분을 차지하는데, "
                 f"Redis 캐시로 이를 제거하면 반복 질문에서 약 **{speedup:,.0f}배** 빨라진다. "
                 "사용자가 같은/유사 질문을 반복하는 챗봇 특성상 효과가 크며, "
                 "Upstage API 호출 비용도 절감된다.")

    report = "\n".join(lines)
    with open("REPORT_cache.md", "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n평균: 미적용 {avg_nc:,.1f} / 히트 {avg_hit:,.1f} ms → {speedup:,.0f}배")
    print("리포트 저장: REPORT_cache.md")


if __name__ == "__main__":
    main()
