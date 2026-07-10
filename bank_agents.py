# -*- coding: utf-8 -*-
"""
FinPick 은행업무 LangGraph 에이전트.

5개 도메인:
  - 고객:   계좌거래(조회+이체·포인트전환·주식매수, 실행 전 확인) + 상담(지식베이스·금리)  → 단일 고객 에이전트
  - 관리자: 이상거래탐지 / 여신심사 / 컴플라이언스                                         → 도메인별 에이전트

설계:
  - LLM: Upstage Solar Pro 2 (OpenAI 호환) via langchain-openai ChatOpenAI.
  - 각 에이전트 = langgraph.prebuilt.create_react_agent (도메인 툴 바인딩).
  - 툴은 대부분 기존 bank_db.* 함수를 얇게 감싼다.
  - 행위 주체(user_id)는 LLM이 못 정한다 — 웹 계층이 contextvar(set_actor)로 주입.
  - 돈 움직이는 작업은 prepare_* 로 요약만 만들고, 사용자가 확인하면 confirm_action(토큰)으로 실행.
  - 대화 메모리는 MemorySaver + thread_id(고객=cust-{uid}) 로 멀티턴 유지.

순환참조 방지: bank_web 의 헬퍼(es, market_stats 등)는 configure()로 주입받는다.
"""
import os
import uuid
import contextvars

from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver

import bank_db
import bank_kafka
import rag_core

POINT_TO_WON = 10
TRANSFER_INDEX = "bank-transfers"

# --------------------------- 주입 의존성 (bank_web.configure로 채움) ---------------------------
_deps = {
    "es": None,                 # Elasticsearch 클라이언트
    "market_stats": None,       # def market_stats(ptype)
    "rate_ranking": None,       # def rate_ranking(n)
    "base_rate": None,          # def base_rate()
    "finpick_products": [],     # FINPICK_PRODUCTS 리스트
}


def configure(**kwargs):
    _deps.update(kwargs)


# --------------------------- 행위 주체(actor) 컨텍스트 ---------------------------
_actor = contextvars.ContextVar("actor", default=None)


def set_actor(user_id=None, thread_id=None, is_admin=False):
    """웹 요청마다 호출: 이 요청에서 에이전트가 대신 행동할 주체를 고정한다."""
    _actor.set({"user_id": user_id, "thread_id": thread_id or "default", "is_admin": is_admin})


def _uid():
    a = _actor.get()
    return a["user_id"] if a else None


def _thread():
    a = _actor.get()
    return a["thread_id"] if a else "default"


# 확인 대기 액션: thread_id -> {"token":..., "type":"transfer|convert|buy", ...}
_pending = {}


def _make_pending(action):
    token = uuid.uuid4().hex[:8]
    action["token"] = token
    _pending[_thread()] = action
    return token


# --------------------------- LLM ---------------------------
def make_llm(temperature=0):
    return ChatOpenAI(model=rag_core.CHAT_MODEL, base_url="https://api.upstage.ai/v1",
                      api_key=os.environ.get("UPSTAGE_API_KEY"), temperature=temperature)


# =========================================================================
# 고객 툴 — 계좌거래 + 상담
# =========================================================================
@tool
def get_balance() -> str:
    """로그인한 본인의 모든 계좌와 잔액을 조회한다."""
    accts = bank_db.get_accounts(_uid())
    if not accts:
        return "개설된 계좌가 없습니다."
    lines = []
    for a in accts:
        label = "픽앤업 통장" if a["acct_type"] == "point" else "입출금통장"
        lines.append(f"{label} {a['account_no']}: {a['balance']:,}원")
    return "\n".join(lines)


@tool
def get_recent_transactions(limit: int = 10) -> str:
    """본인 주계좌의 최근 거래내역을 조회한다."""
    accts = bank_db.get_accounts(_uid())
    if not accts:
        return "계좌가 없습니다."
    txns = bank_db.get_transactions(accts[0]["id"], limit=limit)
    if not txns:
        return "거래내역이 없습니다."
    return "\n".join(
        f"{t['created_at'][5:16]} {t['kind']} {t['counterpart'] or ''} "
        f"{'+' if t['kind']=='입금' else '-'}{t['amount']:,}원 (잔액 {t['balance_after']:,})"
        for t in txns)


@tool
def get_my_products() -> str:
    """본인이 가입한 예적금·대출 상품을 조회한다."""
    subs = bank_db.get_subscriptions(_uid())
    if not subs:
        return "가입한 상품이 없습니다."
    return "\n".join(f"{s['product_name']} ({s['ptype']}) 금리 {s['rate']:.2f}% "
                     f"원금 {s['principal']:,}원" for s in subs)


@tool
def get_my_points() -> str:
    """본인의 픽앤업 포인트와 포인트머니 통장 잔액을 조회한다."""
    pts = bank_db.get_points(_uid())
    acct = bank_db.get_point_account(_uid())
    bal = acct["balance"] if acct else 0
    return f"보유 포인트 {pts:,}P, 포인트머니 통장 잔액 {bal:,}원"


@tool
def lookup_recipient(account_no: str) -> str:
    """이체 전, FinPick 계좌번호의 예금주 이름을 확인한다."""
    name = bank_db.lookup_account_name(account_no)
    return f"{account_no} 예금주: {name}" if name else "해당 FinPick 계좌를 찾을 수 없습니다."


@tool
def prepare_transfer(to_account: str, amount: int, bank: str = "FinPick", memo: str = "") -> str:
    """이체를 '준비'만 한다(실제 실행 안 함). 금액·받는분을 검증하고 확인 요약과 확인 토큰을 돌려준다.
    사용자에게 요약을 보여주고 '네'라고 확인받은 뒤에 confirm_action(토큰)으로 실행해야 한다."""
    if amount <= 0:
        return "이체 금액을 확인하세요."
    recipient = None
    if bank == "FinPick":
        recipient = bank_db.lookup_account_name(to_account)
        if not recipient:
            return "FinPick에서 해당 계좌를 찾을 수 없습니다. 계좌번호를 확인하세요."
    token = _make_pending({"type": "transfer", "to_account": to_account, "amount": amount,
                           "bank": bank, "memo": memo, "recipient": recipient})
    who = f"{recipient}님({to_account})" if recipient else f"{bank} {to_account}"
    return (f"[확인 필요] {who}에게 {amount:,}원을 이체합니다"
            + (f" (메모: {memo})" if memo else "") + f".\n확인 토큰: {token}\n"
            "사용자가 '네'라고 확인하면 confirm_action('" + token + "')로 실행하세요.")


@tool
def prepare_point_convert(points: int) -> str:
    """포인트 → 포인트머니(원화) 전환을 준비한다(1P=10원). 확인 후 confirm_action으로 실행."""
    if points <= 0:
        return "전환할 포인트를 확인하세요."
    have = bank_db.get_points(_uid())
    if points > have:
        return f"보유 포인트({have:,}P)보다 많이 전환할 수 없습니다."
    token = _make_pending({"type": "convert", "points": points})
    return (f"[확인 필요] {points:,}P를 {points*POINT_TO_WON:,}원으로 전환합니다.\n"
            f"확인 토큰: {token}\n확인되면 confirm_action('{token}')로 실행하세요.")


@tool
def prepare_stock_buy(code: str, points: int) -> str:
    """포인트로 모의주식 매수를 준비한다. 확인 후 confirm_action으로 실행."""
    if points <= 0:
        return "매수에 쓸 포인트를 확인하세요."
    stock = bank_db.get_stock(code)
    if not stock:
        return "존재하지 않는 종목코드입니다. get_stock_list로 확인하세요."
    token = _make_pending({"type": "buy", "code": code, "points": points, "name": stock["name"]})
    return (f"[확인 필요] {stock['name']}({code})을 {points:,}P({points*POINT_TO_WON:,}원)어치 매수합니다.\n"
            f"확인 토큰: {token}\n확인되면 confirm_action('{token}')로 실행하세요.")


@tool
def get_stock_list() -> str:
    """모의투자 가능한 종목과 코드·현재가를 조회한다."""
    return "\n".join(f"{s['code']} {s['name']} ({s['market']}) {s['price']:,}원"
                     for s in bank_db.get_stocks())


@tool
def confirm_action(confirmation_token: str) -> str:
    """사용자가 '네'로 확인한 뒤, 준비된 돈 관련 작업(이체/포인트전환/주식매수)을 실제로 실행한다.
    반드시 직전 prepare_*가 돌려준 정확한 확인 토큰이 필요하다."""
    act = _pending.get(_thread())
    if not act or act.get("token") != confirmation_token:
        return "확인 토큰이 유효하지 않습니다. 먼저 prepare_*로 작업을 준비하세요."
    _pending.pop(_thread(), None)
    uid = _uid()
    if act["type"] == "transfer":
        if act["bank"] == "FinPick":
            ok, msg = bank_db.transfer(uid, act["to_account"], act["amount"], act["memo"])
        else:
            ok, msg = bank_db.transfer_external(uid, act["bank"], act["to_account"],
                                                act["amount"], act["memo"])
        try:
            accts = bank_db.get_accounts(uid)
            bal = accts[0]["balance"] if accts else None
            a = _actor.get() or {}
            bank_kafka.publish_transfer(
                user_id=uid, from_account=(accts[0]["account_no"] if accts else None),
                to_bank=act["bank"], to_account=act["to_account"], amount=act["amount"], ok=ok,
                kind="agent-internal" if act["bank"] == "FinPick" else "agent-external",
                message=msg, memo=act["memo"], user_name=None, balance_after=bal,
                recipient_name=act.get("recipient"), channel="agent", async_mode=False)
        except Exception:
            pass
        return ("✅ " + msg) if ok else ("❌ " + msg)
    if act["type"] == "convert":
        ok, msg, _pts, _bal = bank_db.convert_points(uid, act["points"], POINT_TO_WON)
        return ("✅ " + msg) if ok else ("❌ " + msg)
    if act["type"] == "buy":
        ok, msg, _pts = bank_db.buy_stock(uid, act["code"], act["points"], POINT_TO_WON)
        return ("✅ " + msg) if ok else ("❌ " + msg)
    return "알 수 없는 작업입니다."


@tool
def search_knowledge(query: str) -> str:
    """예적금·대출·환전 등 상품/약관 지식베이스에서 근거 문서를 검색한다(상담 답변용)."""
    try:
        context, _ = rag_core.search(query, folder="__all__", top_k=4)
        return context or "관련 자료를 찾지 못했습니다."
    except Exception as e:
        return f"검색 실패: {e}"


@tool
def get_rates() -> str:
    """한국은행 기준금리와 예적금 최고금리 TOP 상품을 조회한다."""
    br = _deps["base_rate"]() if _deps.get("base_rate") else None
    lines = [f"한국은행 기준금리: {br}%" if br else "기준금리: 조회 불가"]
    if _deps.get("rate_ranking"):
        for c in _deps["rate_ranking"](5):
            lines.append(f"{c['rank']}. {c['name']} ({c['co']}·{c['type']}) {c['rate']:.2f}%")
    return "\n".join(lines)


CUSTOMER_TOOLS = [get_balance, get_recent_transactions, get_my_products, get_my_points,
                  lookup_recipient, prepare_transfer, prepare_point_convert, prepare_stock_buy,
                  get_stock_list, confirm_action, search_knowledge, get_rates]


# =========================================================================
# 관리자 툴 — 이상거래탐지 / 여신심사 / 컴플라이언스 (bank-transfers ES 인덱스 활용)
# =========================================================================
def _es():
    return _deps.get("es")


def _search_transfers(query, size=50, aggs=None):
    es = _es()
    if es is None:
        return None
    body = {"size": size, "query": query, "sort": [{"ts": "desc"}]}
    if aggs is not None:
        body["aggs"] = aggs
        body["size"] = 0
    return es.search(index=TRANSFER_INDEX, **body)


@tool
def user_transfer_summary(user_id: int, days: int = 7) -> str:
    """특정 사용자의 최근 이체 요약(건수·총액·성공/실패·타행비율)을 ES에서 집계한다."""
    r = _search_transfers({"term": {"user_id": user_id}}, size=0, aggs={
        "total_amount": {"sum": {"field": "amount"}},
        "by_status": {"terms": {"field": "status"}},
        "by_band": {"terms": {"field": "amount_band"}},
        "external": {"filter": {"term": {"is_external": True}}}})
    if r is None:
        return "검색엔진에 연결할 수 없습니다."
    agg = r["aggregations"]
    total = r["hits"]["total"]["value"]
    statuses = {b["key"]: b["doc_count"] for b in agg["by_status"]["buckets"]}
    bands = {b["key"]: b["doc_count"] for b in agg["by_band"]["buckets"]}
    return (f"user {user_id}: 총 {total}건, 합계 {int(agg['total_amount']['value']):,}원, "
            f"성공/실패={statuses.get('success',0)}/{statuses.get('failed',0)}, "
            f"타행 {agg['external']['doc_count']}건, 금액구간={bands}")


@tool
def high_value_transfers(min_amount: int = 1000000, size: int = 20) -> str:
    """고액 이체(기본 100만원 이상)를 최근순으로 조회한다(이상거래·컴플라이언스용)."""
    r = _search_transfers({"range": {"amount": {"gte": min_amount}}}, size=size)
    if r is None:
        return "검색엔진에 연결할 수 없습니다."
    hits = r["hits"]["hits"]
    if not hits:
        return f"{min_amount:,}원 이상 이체가 없습니다."
    return "\n".join(
        f"{h['_source'].get('ts','')} user {h['_source'].get('user_id')} "
        f"{h['_source'].get('amount',0):,}원 → {h['_source'].get('to_bank')} "
        f"{h['_source'].get('to_account_masked','')} [{h['_source'].get('status')}]"
        for h in hits)


@tool
def failed_attempts(user_id: int) -> str:
    """특정 사용자의 실패한 이체 시도(잔액부족·오류 등)를 조회한다(도용·이상징후 단서)."""
    r = _search_transfers({"bool": {"must": [{"term": {"user_id": user_id}},
                                             {"term": {"status": "failed"}}]}}, size=20)
    if r is None:
        return "검색엔진에 연결할 수 없습니다."
    hits = r["hits"]["hits"]
    if not hits:
        return f"user {user_id}의 실패 이체가 없습니다."
    return f"user {user_id} 실패 {len(hits)}건:\n" + "\n".join(
        f"{h['_source'].get('ts','')} {h['_source'].get('amount',0):,}원 - {h['_source'].get('message','')}"
        for h in hits)


@tool
def night_activity(start_hour: int = 0, end_hour: int = 6) -> str:
    """심야 시간대(기본 0~6시) 이체를 조회한다(이상거래 단서)."""
    r = _search_transfers({"range": {"hour": {"gte": start_hour, "lte": end_hour}}}, size=20)
    if r is None:
        return "검색엔진에 연결할 수 없습니다."
    hits = r["hits"]["hits"]
    if not hits:
        return "해당 시간대 이체가 없습니다."
    return "\n".join(f"{h['_source'].get('ts','')} user {h['_source'].get('user_id')} "
                     f"{h['_source'].get('amount',0):,}원" for h in hits)


FRAUD_TOOLS = [user_transfer_summary, high_value_transfers, failed_attempts, night_activity]


@tool
def get_applicant_profile(user_id: int) -> str:
    """여신심사 대상자의 프로필(잔액·거래활동·기존대출·참여도)을 모아서 반환한다."""
    user = bank_db.get_user(user_id)
    if not user:
        return "존재하지 않는 사용자입니다."
    accts = bank_db.get_accounts(user_id)
    total_bal = sum(a["balance"] for a in accts)
    subs = bank_db.get_subscriptions(user_id)
    loans = [s for s in subs if s["ptype"] in ("주택담보대출", "전세자금대출", "개인신용대출")]
    loan_amt = sum(s["principal"] for s in loans)
    txns = bank_db.get_transactions(accts[0]["id"], limit=100) if accts else []
    points = bank_db.get_points(user_id)
    streak = bank_db.get_streak(user_id)
    return (f"{user['name']}(user {user_id}) | 총잔액 {total_bal:,}원 | 최근거래 {len(txns)}건 | "
            f"기존대출 {len(loans)}건({loan_amt:,}원) | 포인트 {points:,}P | 연속학습 {streak}일")


@tool
def score_credit(user_id: int) -> str:
    """가용 신호로 규칙기반 신용점수(0~100)와 구성요소를 계산한다. (실데이터 없는 데모 스코어링)"""
    accts = bank_db.get_accounts(user_id)
    if not accts:
        return "계좌가 없어 심사할 수 없습니다."
    total_bal = sum(a["balance"] for a in accts)
    subs = bank_db.get_subscriptions(user_id)
    loans = [s for s in subs if s["ptype"] in ("주택담보대출", "전세자금대출", "개인신용대출")]
    loan_amt = sum(s["principal"] for s in loans)
    txns = bank_db.get_transactions(accts[0]["id"], limit=100)
    points = bank_db.get_points(user_id)
    # 규칙: 잔액(40)+거래활동(20)+부채부담 역(25)+참여도(15)
    s_bal = min(40, total_bal // 250000)          # 1천만원=40점
    s_act = min(20, len(txns))                     # 거래 20건=20점
    s_debt = 25 if loan_amt == 0 else max(0, 25 - loan_amt // 4000000)
    s_eng = min(15, points // 20)
    score = int(s_bal + s_act + s_debt + s_eng)
    grade = "우량" if score >= 75 else ("보통" if score >= 50 else "주의")
    return (f"신용점수 {score}/100 ({grade}) — 잔액 {s_bal}, 거래활동 {s_act}, "
            f"부채부담 {s_debt}, 참여도 {s_eng} [규칙기반 데모]")


@tool
def get_loan_products() -> str:
    """FinPick 대출상품(주담대/전세/신용)의 금리·한도를 조회한다(여신 추천용)."""
    loans = [p for p in _deps.get("finpick_products", []) if p.get("kind") == "대출"]
    if not loans:
        return "대출상품 정보가 없습니다."
    return "\n".join(f"{p['name']} ({p['type']}) 금리 {p['rate']:.2f}% 최소 {p.get('min',0):,}원"
                     for p in loans)


CREDIT_TOOLS = [get_applicant_profile, score_credit, get_loan_products]


@tool
def aml_screen(user_id: int) -> str:
    """자금세탁방지(AML) 스크리닝: 고액신고 기준(1천만↑)·분할이체(구조화) 의심·반복 타행이체를 점검한다."""
    r = _search_transfers({"term": {"user_id": user_id}}, size=100)
    if r is None:
        return "검색엔진에 연결할 수 없습니다."
    hits = [h["_source"] for h in r["hits"]["hits"]]
    if not hits:
        return f"user {user_id} 이체 기록 없음."
    big = [h for h in hits if (h.get("amount") or 0) >= 10000000]
    near = [h for h in hits if 9000000 <= (h.get("amount") or 0) < 10000000]  # 구조화 의심
    external = [h for h in hits if h.get("is_external")]
    flags = []
    if big:
        flags.append(f"고액신고대상(1천만↑) {len(big)}건")
    if len(near) >= 2:
        flags.append(f"구조화 의심(900~999만원) {len(near)}건")
    if len(external) >= 5:
        flags.append(f"반복 타행이체 {len(external)}건")
    return (f"user {user_id} AML: " + ("; ".join(flags) if flags else "특이사항 없음")
            + f" (총 {len(hits)}건 분석)")


@tool
def check_large_transactions(threshold: int = 10000000) -> str:
    """전체에서 고액신고 기준 이상 이체를 조회한다(컴플라이언스 보고용)."""
    r = _search_transfers({"range": {"amount": {"gte": threshold}}}, size=30)
    if r is None:
        return "검색엔진에 연결할 수 없습니다."
    hits = r["hits"]["hits"]
    if not hits:
        return f"{threshold:,}원 이상 이체가 없습니다."
    return "\n".join(
        f"{h['_source'].get('ts','')} user {h['_source'].get('user_id')} "
        f"{h['_source'].get('amount',0):,}원 → {h['_source'].get('to_bank')} [{h['_source'].get('status')}]"
        for h in hits)


COMPLIANCE_TOOLS = [aml_screen, check_large_transactions, high_value_transfers]


# =========================================================================
# 에이전트(그래프) 구성
# =========================================================================
_PROMPTS = {
    "agent_account": ("고객 은행 에이전트(계좌거래+상담)", (
        "당신은 FinPick 은행의 AI 은행원입니다. 로그인한 본인의 계좌 업무를 도와줍니다.\n"
        "- 잔액·거래내역·상품·포인트 조회, 이체, 포인트 전환, 모의주식 매수, 상품 상담을 할 수 있습니다.\n"
        "- 돈이 움직이는 작업(이체·포인트전환·주식매수)은 반드시 prepare_* 툴로 먼저 요약을 만들고, "
        "사용자에게 내용을 보여준 뒤 '네' 같은 명시적 확인을 받고 나서만 confirm_action으로 실행합니다. "
        "사용자 확인 없이 절대 confirm_action을 호출하지 마세요.\n"
        "- 상품·금리·약관 질문은 search_knowledge와 get_rates로 근거를 찾아 답합니다.\n"
        "- 마크다운 기호나 이모지는 최소화하고, 존댓말로 간결하게 답합니다.")),
    "agent_fraud": ("이상거래탐지 에이전트", (
        "당신은 FinPick의 이상거래탐지(FDS) 분석가입니다. 관리자를 돕습니다.\n"
        "- 제공된 툴로 이체 데이터를 조회·집계해 위험 신호(고액·심야·연속실패·타행반복)를 찾습니다.\n"
        "- 결론은 위험도(낮음/주의/높음)와 근거를 명확히 제시합니다. 데이터가 부족하면 그렇게 말합니다.")),
    "agent_credit": ("여신심사 에이전트", (
        "당신은 FinPick의 여신심사역입니다. 관리자를 돕습니다.\n"
        "- get_applicant_profile·score_credit로 신청자를 평가하고, get_loan_products로 상품을 참고해 "
        "승인여부·한도·금리 의견을 제시합니다.\n"
        "- 실제 신용정보가 아닌 규칙기반 데모 점수임을 반드시 밝힙니다.")),
    "agent_compliance": ("컴플라이언스 에이전트", (
        "당신은 FinPick의 컴플라이언스(AML) 담당자입니다. 관리자를 돕습니다.\n"
        "- aml_screen·check_large_transactions 등으로 자금세탁·고액신고·구조화 의심을 점검합니다.\n"
        "- 결과를 통과/주의(flag)로 분류하고 사유와 근거 건수를 제시합니다.")),
}


def _prompt(key):
    label, default = _PROMPTS[key]
    return bank_db.ensure_prompt(key, label, default)


_checkpointer = MemorySaver()
_agents = {}


def _get_agent(kind):
    """지연 생성(최초 호출 시). kind: customer|fraud|credit|compliance."""
    if kind in _agents:
        return _agents[kind]
    llm = make_llm()
    if kind == "customer":
        ag = create_react_agent(llm, CUSTOMER_TOOLS, prompt=_prompt("agent_account"),
                                checkpointer=_checkpointer)
    elif kind == "fraud":
        ag = create_react_agent(llm, FRAUD_TOOLS, prompt=_prompt("agent_fraud"))
    elif kind == "credit":
        ag = create_react_agent(llm, CREDIT_TOOLS, prompt=_prompt("agent_credit"))
    elif kind == "compliance":
        ag = create_react_agent(llm, COMPLIANCE_TOOLS, prompt=_prompt("agent_compliance"))
    else:
        raise ValueError(f"알 수 없는 에이전트: {kind}")
    _agents[kind] = ag
    return ag


def _last_text(result):
    msgs = result.get("messages", [])
    for m in reversed(msgs):
        content = getattr(m, "content", None)
        if content and getattr(m, "type", "") in ("ai", "assistant"):
            return content
    return "(응답을 생성하지 못했습니다.)"


def run_customer(user_id, message):
    """고객 에이전트 실행. thread_id로 멀티턴 대화·확인 흐름 유지."""
    thread = f"cust-{user_id}"
    set_actor(user_id=user_id, thread_id=thread)
    ag = _get_agent("customer")
    result = ag.invoke({"messages": [("user", message)]},
                       config={"configurable": {"thread_id": thread}})
    return _last_text(result)


def run_admin(domain, message, admin_user_id=None):
    """관리자 에이전트 실행(도메인 직접 지정). domain: fraud|credit|compliance."""
    set_actor(user_id=admin_user_id, thread_id=f"admin-{domain}-{admin_user_id}", is_admin=True)
    ag = _get_agent(domain)
    result = ag.invoke({"messages": [("user", message)]})
    return _last_text(result)
