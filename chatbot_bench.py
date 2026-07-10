# -*- coding: utf-8 -*-
"""
챗봇 전체 경로 측정 — 임베딩 / 검색 / LLM 답변 생성 단계별 + 답변 캐싱 효과.

검색창 실험(experiment.py)은 임베딩+검색까지였다.
여기선 그 뒤 'LLM이 문장 생성'까지 포함한 전체 체감 시간을 단계별로 분해한다.
"""
import time
import statistics as st
import pandas as pd
from elasticsearch import Elasticsearch

import rag_core, cache

es = Elasticsearch("http://localhost:9200", request_timeout=120)

QUESTIONS = [
    "안전하게 목돈 굴리기 좋은 예금 추천해줘",
    "이자 높은 적금 알려줘",
    "내 집 마련 주택담보대출 어떤 게 좋아?",
]


def answer_staged(question, k=5):
    """단계별 시간을 재며 RAG 답변 생성. (답변, {embed,search,llm,total} ms)."""
    t = time.perf_counter()
    qv = rag_core.embed_query(question).tolist()
    t_embed = (time.perf_counter() - t) * 1000

    t = time.perf_counter()
    r = es.search(index="fss-products", size=k,
                  knn={"field": "embedding", "query_vector": qv, "k": k, "num_candidates": 100},
                  source_excludes=["embedding"])
    docs = [h["_source"] for h in r["hits"]["hits"]]
    context = "\n\n".join(d.get("search_text", "") for d in docs)
    t_search = (time.perf_counter() - t) * 1000

    t = time.perf_counter()
    resp = rag_core.client.chat.completions.create(
        model=rag_core.CHAT_MODEL, temperature=0.2,
        messages=[{"role": "system", "content": rag_core.SYSTEM_PROMPT_DEFAULT},
                  {"role": "user", "content": f"참고 자료:\n{context}\n\n질문: {question}"}])
    out = resp.choices[0].message.content
    t_llm = (time.perf_counter() - t) * 1000

    return out, {"임베딩(ms)": round(t_embed, 1), "검색(ms)": round(t_search, 1),
                 "LLM 답변(ms)": round(t_llm, 1),
                 "합계(ms)": round(t_embed + t_search + t_llm, 1)}


# 연결 워밍업(첫 호출 TLS 비용 제거)
rag_core.embed_query("워밍업")
cache.flush()

print("① 챗봇 전체 경로 단계별 분해 (cold)\n")
rows, answers = [], {}
for q in QUESTIONS:
    rag_core.embed_query.cache_clear()
    ans, timing = answer_staged(q)
    answers[q] = ans
    rows.append({"질문": q[:18], **timing})

df = pd.DataFrame(rows)
avg = {"질문": "평균"}
for col in ["임베딩(ms)", "검색(ms)", "LLM 답변(ms)", "합계(ms)"]:
    avg[col] = round(df[col].mean(), 1)
df = pd.concat([df, pd.DataFrame([avg])], ignore_index=True)
print(df.to_string(index=False))

emb, sea, llm = avg["임베딩(ms)"], avg["검색(ms)"], avg["LLM 답변(ms)"]
tot = avg["합계(ms)"]
print(f"\n   단계별 비중:  임베딩 {emb/tot*100:.0f}%  /  검색 {sea/tot*100:.0f}%  /  LLM 답변 {llm/tot*100:.0f}%")
print(f"   → 챗봇 체감 시간의 대부분({llm/tot*100:.0f}%)은 'LLM이 문장 만드는 단계'")


print("\n② 답변 통째 캐싱 효과 (같은 질문 재요청)\n")
# 답변을 Redis에 캐싱 → 같은 질문은 임베딩·검색·LLM 전부 건너뜀
def answer_cached(question):
    hit = cache.cache_get("answer", question)
    if hit is not None:
        return hit, True
    ans, _ = answer_staged(question)
    cache.cache_set("answer", ans, cache.TTL_SEARCH, question)
    return ans, False

q = QUESTIONS[0]
cache.flush()
t = time.perf_counter(); _, h1 = answer_cached(q); cold = (time.perf_counter() - t) * 1000
t = time.perf_counter(); _, h2 = answer_cached(q); warm = (time.perf_counter() - t) * 1000
print(f"   1차(미스, 전체 실행): {cold:8.1f} ms  (캐시히트={h1})")
print(f"   2차(히트, 답변 재사용): {warm:8.1f} ms  (캐시히트={h2})")
print(f"   → 답변 캐싱 시 약 {cold/warm:,.0f}배 빠름 (LLM 호출까지 통째로 생략, 비용도 절감)")

print("\n[참고] 생성된 답변 예시:")
print(" Q:", q)
print(" A:", answers[QUESTIONS[0]][:160], "...")
