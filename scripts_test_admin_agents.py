# -*- coding: utf-8 -*-
"""관리자 에이전트 스모크/간이 성능 테스트."""

from __future__ import annotations

import os
import statistics
import time
from typing import Iterable

from elasticsearch import Elasticsearch

import bank_agents
import bank_db
import scripts_seed_sim

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
LOAN_PRODUCTS = [
    {"code": "FP-MTG", "kind": "대출", "type": "주택담보대출", "name": "핀픽 내집마련대출", "rate": 3.40, "min": 1_000_000},
    {"code": "FP-RENT", "kind": "대출", "type": "전세자금대출", "name": "핀픽 전세자금대출", "rate": 3.50, "min": 1_000_000},
    {"code": "FP-CREDIT", "kind": "대출", "type": "개인신용대출", "name": "핀픽 직장인신용대출", "rate": 5.40, "min": 500_000},
]


def configure_agents() -> None:
    bank_agents.configure(
        es=Elasticsearch(ES_URL, request_timeout=30),
        market_stats=lambda _ptype: {},
        rate_ranking=lambda _n=5: [],
        base_rate=lambda: None,
        finpick_products=LOAN_PRODUCTS,
    )


def build_cases(targets: dict, admin_user_id: int) -> list[tuple[str, str, str]]:
    return [
        (
            "fraud",
            "고액·속도·연속실패 관점에서 가장 의심스러운 사용자인지 평가해줘.",
            f"user {targets['velocity']['user_id']}의 최근 이체 패턴을 분석해서 위험도와 근거를 알려줘.",
        ),
        (
            "fraud",
            "연속 실패가 계정탈취 시도처럼 보이는지 판단해줘.",
            f"user {targets['failures']['user_id']}의 실패 이체 패턴을 보고 이상거래 여부를 설명해줘.",
        ),
        (
            "compliance",
            "AML 관점에서 구조화 의심 여부를 판단해줘.",
            f"user {targets['structuring']['user_id']}의 AML 스크리닝 결과와 신고 필요성을 평가해줘.",
        ),
        (
            "compliance",
            "고액 현금성 이체 보고 후보로 볼 수 있는지 판단해줘.",
            f"user {targets['high_value']['user_id']}의 고액 이체를 컴플라이언스 관점에서 요약해줘.",
        ),
        (
            "credit",
            "승인 가능성이 높은 신청자 예시로 적합한지 평가해줘.",
            f"user {targets['credit_good']['user_id']}의 대출 심사 가능성과 적합 상품을 평가해줘.",
        ),
        (
            "credit",
            "보수적으로 봐야 하는 신청자 예시인지 평가해줘.",
            f"user {targets['credit_risky']['user_id']}의 대출 심사 리스크와 보수적 판단 근거를 설명해줘.",
        ),
    ]


def run_cases(cases: Iterable[tuple[str, str, str]], admin_user_id: int) -> list[dict]:
    results = []
    for domain, label, message in cases:
        started = time.perf_counter()
        try:
            answer = bank_agents.run_admin(domain, message, admin_user_id=admin_user_id)
            elapsed = time.perf_counter() - started
            bank_db.add_agent_log(
                domain, route="admin", source="test", user_id=admin_user_id, ok=True,
                elapsed_ms=elapsed * 1000, question=message, scenario_label=label,
                answer_chars=len(answer or ""))
            results.append({
                "domain": domain,
                "label": label,
                "seconds": elapsed,
                "message": message,
                "answer": answer,
                "ok": True,
            })
        except Exception as e:
            elapsed = time.perf_counter() - started
            bank_db.add_agent_log(
                domain, route="admin", source="test", user_id=admin_user_id, ok=False,
                elapsed_ms=elapsed * 1000, question=message, scenario_label=label,
                error=str(e))
            results.append({
                "domain": domain,
                "label": label,
                "seconds": elapsed,
                "message": message,
                "answer": str(e),
                "ok": False,
            })
    return results


def main() -> None:
    summary = scripts_seed_sim.seed()
    admin = scripts_seed_sim.ensure_sim_admin()
    configure_agents()

    print(f"[seed] sim users={summary['user_count']} events={summary['event_count']} es_sim_events={summary['sim_event_count']}")
    print(f"[admin] user_id={admin['id']} username={summary['admin']['username']}")

    cases = build_cases(summary["anomaly_targets"], admin["id"])
    results = run_cases(cases, admin["id"])

    for idx, row in enumerate(results, start=1):
        print(f"\n=== CASE {idx} | {row['domain']} | {row['label']} | {row['seconds']:.2f}s ===")
        print(f"Q: {row['message']}")
        print(f"A: {row['answer']}")

    latencies = [row["seconds"] for row in results]
    print("\n=== SUMMARY ===")
    print(f"cases={len(results)} avg={statistics.mean(latencies):.2f}s p95={max(latencies):.2f}s")


if __name__ == "__main__":
    main()
