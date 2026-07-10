# -*- coding: utf-8 -*-
"""
실시간 상담봇 — 업스테이지 Solar Pro 2 (kafChat 의 solar.ts 를 파이썬으로 옮김).

채널의 최근 대화 맥락을 바탕으로 금융 상담원 말투로 답한다.
Upstage 클라이언트는 rag_core 가 이미 만들어 둔 것을 그대로 재사용한다(키 중복 설정 불필요).
"""
import os
import re

import bank_db  # 프롬프트 매니저(관리자 화면에서 시스템 프롬프트를 저장/수정)
import rag_core  # 이미 설정된 Upstage(OpenAI 호환) 클라이언트 재사용

BOT_NAME = os.environ.get("CONSULT_BOT_NAME", "상담봇")
# 봇 켜기: 명시적으로 끄지 않았고 API 키가 있으면 동작
BOT_ENABLED = (os.environ.get("CONSULT_BOT_ENABLED", "true").lower() == "true"
               and rag_core.client is not None)

KB_FOLDER = "__all__"  # seed_consult_kb.py가 카테고리별(예금/적금/대출 등)로 나눠 색인하므로
                        # 전체 폴더를 가로질러 검색한다 (rag_core.search의 특수값)

# 상담 종류별 프롬프트. 질문에 담긴 키워드로 종류를 고르고, 없으면 일반(consult_general)을 쓴다.
# 각 기본값은 {{BOT_NAME}}/{{CENTER_PHONE}} 같은 변수를 담고 있고, 실제 값은 관리자 화면의
# "프롬프트 변수"에서 코드 수정 없이 바꿀 수 있다.
_COMMON_RULES = (
    "- '참고 자료'가 주어지면 그 내용을 우선 근거로 답하고, 자료에 없는 내용은 확실하지 않다고 "
    "솔직히 말하며 고객센터({{CENTER_PHONE}}) 안내를 덧붙입니다.\n"
    "- 참고 자료의 문서 제목이나 '[예금자보호 한도]' 같은 대괄호 출처 표기를 답변에 그대로 "
    "옮기지 말고, 내용만 자연스러운 상담 말투로 풀어서 전달합니다.\n"
    "- 채팅이므로 답변은 너무 길지 않게 핵심 위주로 합니다.\n"
    "- 마크다운 기호(**, ##, - 등)와 이모지는 쓰지 말고 깔끔한 평문으로 답합니다.\n"
    "- 특정 상품 매수·매도를 단정적으로 권유하지 않고, 일반적인 정보와 유의사항을 안내합니다."
)

CONSULT_CATEGORIES = [
    # (프롬프트 key, 관리자 화면 표시명, 이 종류로 분류할 키워드들)
    ("consult_deposit", "예금 상담", ["예금", "정기예금", "예치"]),
    ("consult_savings", "적금 상담", ["적금", "저축"]),
    ("consult_loan", "대출 상담", ["대출", "신용대출", "담보대출", "이자율"]),
    ("consult_fx", "환전 상담", ["환전", "환율", "외화", "달러"]),
]
DEFAULT_CATEGORY = "consult_general"


def _default_prompt(topic_line):
    return (
        f'당신은 FinPick 은행의 친절한 실시간 상담원 "{{{{BOT_NAME}}}}"입니다.\n'
        f"{topic_line}\n" + _COMMON_RULES
    )


CONSULT_PROMPT_DEFAULTS = {
    DEFAULT_CATEGORY: _default_prompt(
        "- 예금·적금·대출·펀드·환전·포인트 등 금융 질문에 정확하고 간결하게, 존댓말로 답합니다."),
    "consult_deposit": _default_prompt(
        "- 예금(정기예금·입출금통장) 상담을 전문으로 합니다. 금리·가입기간·중도해지 불이익을 중심으로 안내합니다."),
    "consult_savings": _default_prompt(
        "- 적금 상담을 전문으로 합니다. 자유적립/정기적립 차이, 우대금리 조건을 중심으로 안내합니다."),
    "consult_loan": _default_prompt(
        "- 대출 상담을 전문으로 합니다. 금리·한도·상환방식·연체 시 유의사항을 신중하고 정확하게 안내합니다."),
    "consult_fx": _default_prompt(
        "- 환전·환율 상담을 전문으로 합니다. 환율 변동은 예측하지 않고, 환전 수수료 우대·외화예금 정보를 중심으로 안내합니다."),
}


def detect_category(question):
    """질문 키워드로 상담 종류를 고른다. 매치되는 게 없으면 일반 상담으로."""
    q = question or ""
    for key, _label, keywords in CONSULT_CATEGORIES:
        if any(kw in q for kw in keywords):
            return key
    return DEFAULT_CATEGORY


def _category_label(key):
    for k, label, _kw in CONSULT_CATEGORIES:
        if k == key:
            return label
    return "일반 상담"


def _seed_default_variables():
    bank_db.ensure_variable("BOT_NAME", BOT_NAME)
    bank_db.ensure_variable("CENTER_PHONE", "1599-0000")


def _retrieve_context(question):
    """최신 질문으로 상담FAQ 지식베이스를 검색. 문서가 없거나 실패하면 조용히 None."""
    try:
        context, _sources = rag_core.search(question, folder=KB_FOLDER, top_k=4)
        return context
    except Exception:
        return None


_EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF]+")


def _strip_markdown(text):
    """채팅창은 평문이라 마크다운 기호가 그대로 보인다 → 흔한 기호를 정리."""
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)   # **굵게** → 굵게
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)\*", r"\1", text)  # *기울임* → 기울임
    text = re.sub(r"`(.+?)`", r"\1", text)          # `코드` → 코드
    text = re.sub(r"^\s{0,3}#{1,6}\s*", "", text, flags=re.MULTILINE)  # 헤더 기호
    text = re.sub(r"^\s{0,3}[-*]\s+", "· ", text, flags=re.MULTILINE)  # 리스트 → ·
    text = re.sub(r"\[[^\[\]]{1,40}\]", "", text)   # [문서 제목] 형태의 출처 표기 제거
    text = _EMOJI_RE.sub("", text)                  # 이모지 제거(상담원 말투 지침 보강)
    text = re.sub(r"[ \t]{2,}", " ", text)          # 위 치환으로 생긴 중복 공백 정리
    return text.strip()


def generate_reply(history):
    """채널 최근 대화(history: [{user, text}, ...] 오래된→최신)로 봇 답변 생성. 실패 시 None."""
    if rag_core.client is None:
        return None
    recent = history[-12:]  # 최근 12개만 맥락으로 (토큰/비용 절약)

    last_user_msg = next((m.get("text", "") for m in reversed(recent)
                          if m.get("user") != BOT_NAME), "")
    context = _retrieve_context(last_user_msg) if last_user_msg else None

    _seed_default_variables()
    category = detect_category(last_user_msg)
    raw_prompt = bank_db.ensure_prompt(
        category, _category_label(category), CONSULT_PROMPT_DEFAULTS[category])
    system = bank_db.render_prompt(raw_prompt)
    if context:
        system += f"\n\n참고 자료:\n{context}"

    messages = [{"role": "system", "content": system}]
    for m in recent:
        if m.get("user") == BOT_NAME:
            messages.append({"role": "assistant", "content": m.get("text", "")})
        else:
            messages.append({"role": "user", "content": f'{m.get("user","고객")}: {m.get("text","")}'})
    try:
        res = rag_core.client.chat.completions.create(
            model=rag_core.CHAT_MODEL,
            messages=messages,
            max_tokens=1024,
            temperature=0.5,
        )
        text = (res.choices[0].message.content or "").strip()
        return _strip_markdown(text) or None
    except Exception as e:
        print(f"[consult-bot] 응답 생성 실패: {e}")
        return None
