# -*- coding: utf-8 -*-
"""
ES 의미검색 기반 RAG 답변 파이프라인 (+ Phoenix 추적).

흐름: 질문 → 질문 임베딩 → ES kNN 검색 → 근거(search_text) → Upstage LLM 답변
각 단계가 Phoenix(http://localhost:6006)에 트레이스로 기록된다.

실행: python rag_answer.py   (샘플 질문 몇 개를 흘려보냄)
"""
import os

import observability
observability.init_tracing()  # 반드시 OpenAI 호출 전에

import rag_core
from elasticsearch import Elasticsearch
from opentelemetry import trace

es = Elasticsearch(os.environ.get("ES_URL", "http://localhost:9200"), request_timeout=60)
tracer = trace.get_tracer("bank-rag")


def retrieve(question, index="fss-products", k=5):
    """질문 임베딩 → ES kNN → 근거 문맥과 문서들 반환."""
    with tracer.start_as_current_span("retrieve") as sp:
        sp.set_attribute("index", index)
        sp.set_attribute("k", k)
        qv = rag_core.embed_query(question).tolist()
        r = es.search(index=index, size=k,
                      knn={"field": "embedding", "query_vector": qv,
                           "k": k, "num_candidates": 100},
                      source_excludes=["embedding"])
        docs = [h["_source"] for h in r["hits"]["hits"]]
        context = "\n\n".join(d.get("search_text", "") for d in docs)
        sp.set_attribute("retrieved", len(docs))
        return context, docs


def answer(question, index="fss-products", k=5):
    """RAG 답변 생성 (추적됨)."""
    with tracer.start_as_current_span("rag-answer") as sp:
        sp.set_attribute("input.value", question)
        context, docs = retrieve(question, index, k)
        messages = [
            {"role": "system", "content": rag_core.SYSTEM_PROMPT_DEFAULT},
            {"role": "user", "content": f"참고 자료:\n{context}\n\n질문: {question}"},
        ]
        resp = rag_core.client.chat.completions.create(
            model=rag_core.CHAT_MODEL, messages=messages, temperature=0.2)
        out = resp.choices[0].message.content
        sp.set_attribute("output.value", out)
        return out, docs


if __name__ == "__main__":
    samples = [
        ("fss-products", "안전하게 목돈 굴리기 좋은 예금 추천해줘"),
        ("fss-products", "전세 보증금 마련 대출 금리 낮은 거 알려줘"),
        ("bok-keystat", "지금 한국은행 기준금리가 얼마야?"),
    ]
    for idx, q in samples:
        print("\n" + "=" * 60)
        print("질문:", q)
        ans, docs = answer(q, index=idx, k=5)
        print("답변:", ans)

    # 배치 트레이스 전송 보장
    observability.init_tracing().force_flush()
    print("\n[완료] Phoenix UI에서 확인: http://localhost:6006")
