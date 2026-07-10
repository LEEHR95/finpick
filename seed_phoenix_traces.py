# -*- coding: utf-8 -*-
"""
피닉스(Phoenix)에 실제 예시 트레이스(호출기록) 100건을 채우는 스크립트.

observability.py의 추적은 bank_web.py 프로세스가 뜰 때만 자동으로 켜진다. 이 스크립트는
독립 실행이라 시작할 때 직접 init_tracing("bank-web")을 호출해 같은 프로젝트로 보낸다
(관리자 대시보드 "피닉스 열기" 버튼으로 들어가면 bank-web 프로젝트에서 그대로 보임).

seed_consult_kb.py의 FAQ 100건 주제를 "~가 궁금해요" 같은 자연스러운 고객 질문으로
바꿔 상담봇(bank_consult_bot.generate_reply)에 실제로 100번 물어본다. 그 호출들이
그대로 임베딩 검색 + LLM 응답 트레이스로 피닉스에 쌓인다.

동시에 여러 개를 보내 시간을 줄인다(스레드풀). Upstage API 실제 호출이라 시간·비용이
든다 — 재실행하면 100건이 또 쌓이니 필요할 때만 실행할 것.

실행: python seed_phoenix_traces.py
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import observability
observability.init_tracing("bank-web")

import bank_consult_bot as bot
from seed_consult_kb import FAQ

WORKERS = 6


def to_question(source):
    """FAQ 문서 제목을 자연스러운 고객 질문 형태로."""
    if source.endswith(("법", "방법", "여부", "차이", "기준", "안내", "혜택")):
        return f"{source}이 궁금해요"
    return f"{source}에 대해 알려주세요"


def ask_one(idx, category, source):
    question = to_question(source)
    history = [{"user": "고객", "text": question}]
    t0 = time.time()
    reply = bot.generate_reply(history)
    ms = int((time.time() - t0) * 1000)
    ok = "OK" if reply else "실패"
    return idx, category, question, ok, ms


def main():
    print(f"총 {len(FAQ)}건 질문을 상담봇에 보내 피닉스 트레이스를 채웁니다 (워커 {WORKERS}개)...")
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(ask_one, i, cat, source) for i, (cat, source, _kw, _text) in enumerate(FAQ, 1)]
        for fut in as_completed(futs):
            idx, category, question, ok, ms = fut.result()
            done += 1
            print(f"[{done}/{len(FAQ)}] ({category}) {question} -> {ok} ({ms}ms)")
    print("완료. 관리자 페이지 > 피닉스(LLM 추적) > '피닉스 열기'에서 bank-web 프로젝트를 확인하세요.")


if __name__ == "__main__":
    main()
