# -*- coding: utf-8 -*-
"""
ONBANK — 은행 웹앱 (Flask).

페이지: 메인 / 예적금·대출 / 로그인·회원가입 / 계좌조회 / 이체
연동  : ES 실시간 금리(Redis 캐시) + RAG 자연어 챗봇(/api/ask)

실행: python bank_web.py  → http://127.0.0.1:5002
환경변수: UPSTAGE_API_KEY(임베딩·LLM), ES_URL(기본 localhost:9200), BANK_SECRET(세션키)
"""
import os
import re
import random
import subprocess
import time
import uuid
import socket
from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import urlparse

import requests
from flask import (Flask, request, jsonify, render_template,
                   redirect, url_for, session, flash, g)
from flask_socketio import SocketIO, join_room, leave_room
from elasticsearch import Elasticsearch

import bank_db
import bank_kafka
import bank_consult_bot
import rag_core
import cache
import monitoring
import bank_agents

# Phoenix tracing is opt-in so local tests do not stall when Phoenix is down.
if os.environ.get("ENABLE_PHOENIX") == "1":
    try:
        import observability
        observability.init_tracing("bank-web")
    except Exception as e:
        print("[tracing] 비활성화:", e)

BANK_NAME = "FinPick"
BANKS = ["FinPick", "국민은행", "신한은행", "우리은행", "하나은행", "카카오뱅크", "토스뱅크"]

# --------------------------- 게시판: 이벤트 / 공지사항 (카테고리 통합) ---------------------------
BOARD_POSTS = [
    {"cat": "이벤트", "title": "여름맞이 예적금 가입 이벤트, 최대 3만원 축하금",
     "date": "2026.07.01 ~ 2026.08.31", "link": None},
    {"cat": "이벤트", "title": "픽앤업 경제공부 챌린지 100P 추가 적립",
     "date": "2026.06.15 ~ 2026.07.31", "link": "/learn"},
    {"cat": "이벤트", "title": "첫 거래 고객 우대금리 +0.3%p 쿠폰",
     "date": "2026.06.01 ~ 상시", "link": None},
    {"cat": "이벤트", "title": "친구 초대하고 커피 기프티콘 받기",
     "date": "2026.05.20 ~ 2026.07.20", "link": None},
    {"cat": "공지사항", "title": "시스템 정기 점검 안내 (7/6 새벽 2~4시)",
     "date": "2026.07.02", "link": None},
    {"cat": "공지사항", "title": "개인정보 처리방침 개정 안내",
     "date": "2026.06.28", "link": "/privacy"},
    {"cat": "공지사항", "title": "보이스피싱 주의 안내: 출처 불명 링크 클릭 금지",
     "date": "2026.06.20", "link": None},
    {"cat": "공지사항", "title": "나의 소원 우리 정기예금/적금 한도 소진 판매 종료",
     "date": "2026.06.10", "link": "/products"},
]
HOME_FAQ = [
    {"q": "계좌는 어떻게 개설하나요?", "a": "회원가입만 하면 입출금 통장이 자동으로 개설되고, 축하금 100만원이 함께 지급돼요."},
    {"q": "포인트는 어떻게 모으나요?", "a": "픽앤업 경제공부에서 카드를 읽고 퀴즈를 맞히면 포인트가 쌓여요. 하루 최대 130P."},
    {"q": "포인트로 우대금리를 받으려면?", "a": "모은 포인트를 포인트머니 통장으로 전환하면 잔액 구간에 따라 우대금리가 자동 적용돼요."},
    {"q": "상품 비교는 어디서 하나요?", "a": "상단 검색창이나 상품 메뉴에서 예금·적금·대출을 금리순으로 비교할 수 있어요."},
]
app = Flask(__name__)
app.secret_key = os.environ.get("BANK_SECRET", "dev-secret-change-me")
# 실시간 상담채팅용 Socket.IO (threading 모드 — 추가 서버 없이 Flask 개발서버로 동작)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

bank_db.init_db()
es = Elasticsearch(os.environ.get("ES_URL", "http://localhost:9200"), request_timeout=30)
_es_probe_state = {"ts": 0.0, "ok": True}


def es_quick():
    """사용자 페이지용 ES 클라이언트. 다운 시 오래 붙잡지 않도록 짧게 실패한다."""
    return es.options(request_timeout=2, max_retries=0, retry_on_timeout=False)


def es_online(probe_ttl=5.0):
    """공용 페이지용 ES 생존 확인. 포트가 닫혀 있으면 검색 자체를 건너뛴다."""
    now = time.time()
    if now - _es_probe_state["ts"] < probe_ttl:
        return _es_probe_state["ok"]

    parsed = urlparse(os.environ.get("ES_URL", "http://localhost:9200"))
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 9200)
    ok = False
    try:
        with socket.create_connection((host, port), timeout=0.4):
            ok = True
    except OSError:
        ok = False

    _es_probe_state["ts"] = now
    _es_probe_state["ok"] = ok
    return ok


@app.context_processor
def inject_globals():
    return {"bank_name": BANK_NAME, "current_user": session.get("user")}


# --------------------------- 요청 응답시간 수집(모니터링 시계열) ---------------------------
@app.before_request
def _mon_before():
    g._mon_t0 = time.time()


@app.after_request
def _mon_after(resp):
    t0 = getattr(g, "_mon_t0", None)
    # 정적파일·소켓 폴링은 제외 — '앱 응답시간'이 의미 있게 나오도록 실제 페이지/API만 집계
    p = request.path
    if t0 is not None and not p.startswith("/static") and not p.startswith("/socket.io"):
        monitoring.record_request((time.time() - t0) * 1000)
    return resp


# 실측 시계열 수집기 시작(30초마다 캐시 히트율·컨테이너 CPU·요청 p95 등 적재)
monitoring.start_sampler()


# --------------------------- 경제공부 콘텐츠 ---------------------------
# 학습 카드: 자산 간 연관 지식 (사람이 정리한 일반 패턴, ML 아님)
LEARN_CARDS = [
    {"a": "미국 금리 인상", "b": "원/달러 환율 상승(원화 약세)", "rel": "같은 방향",
     "why": "금리 높은 미국으로 돈이 몰려 달러가 강해지면, 상대적으로 원화 가치가 떨어져요."},
    {"a": "원/달러 환율 상승", "b": "외국인 한국주식 매도 → 코스피 하락", "rel": "반대 방향",
     "why": "환율이 오르면 외국인은 환차손이 무서워 한국 주식을 팔고 나가 코스피가 눌려요."},
    {"a": "달러 강세", "b": "금값 하락", "rel": "반대 방향",
     "why": "금은 달러로 사요. 달러가 비싸지면 같은 금을 사는 데 돈이 더 들어 수요·가격이 눌려요."},
    {"a": "금리 인상", "b": "부동산 하락", "rel": "반대 방향",
     "why": "대출 이자가 비싸지면 집 살 여력이 줄어 부동산 수요·가격에 부담이 돼요."},
    {"a": "금리 인하·돈 풀기", "b": "주식·부동산·금 상승", "rel": "같은 방향",
     "why": "시중에 돈이 많아지면 갈 곳을 찾아 위험·실물자산으로 몰려들어 값이 오르기 쉬워요."},
    {"a": "경기 침체 공포", "b": "금·달러 상승 / 주식 하락", "rel": "안전자산 선호",
     "why": "불안하면 사람들이 주식을 팔고 안전하다고 믿는 금·달러로 피신해요."},
]

# 퀴즈: '오늘의 학습' 카드로부터 자동 생성 (카드에서 본 A↔B 관계를 맞히기).
# 정답/해설을 카드에서 가져오므로, 카드를 늘리면 퀴즈도 자동으로 늘어난다.
REL_OPTIONS = ["같은 방향", "반대 방향", "안전자산 선호"]


def build_card_quiz():
    quiz = []
    for c in LEARN_CARDS:
        ans = REL_OPTIONS.index(c["rel"]) if c["rel"] in REL_OPTIONS else 0
        quiz.append({"q": f"‘{c['a']}’ → ‘{c['b']}’. 이 둘의 관계는?",
                     "opts": REL_OPTIONS, "ans": ans, "exp": c["why"]})
    return quiz


CARD_QUIZ = build_card_quiz()
QUIZ_REWARD = 10    # 퀴즈 1문제 정답 보상
STOCK_PREDICT_REWARD = 30  # 주가 예측 성공 보상

# --------------------------- 포인트머니 통장 ---------------------------
POINT_TO_WON = 10   # 포인트 → 원 환전 비율 (1P = 10원)
# (보유 잔액 하한, 우대금리%) — 잔액이 높을수록 우대. 내림차순으로 첫 매치 적용
POINT_ACCOUNT_TIERS = [(500_000, 4.00), (200_000, 3.50), (50_000, 3.00), (0, 2.50)]


def point_account_rate(balance):
    for threshold, rate in POINT_ACCOUNT_TIERS:
        if balance >= threshold:
            return rate
    return POINT_ACCOUNT_TIERS[-1][1]


def point_account_next_tier(balance):
    """다음 우대 구간까지 남은 금액. 이미 최고 구간이면 None."""
    higher = [t for t in POINT_ACCOUNT_TIERS if t[0] > balance]
    if not higher:
        return None
    threshold, rate = min(higher, key=lambda t: t[0])
    return {"need": threshold - balance, "rate": rate}


def login_required(f):
    @wraps(f)
    def wrap(*a, **kw):
        if not session.get("user"):
            return redirect(url_for("login", next=request.path))
        return f(*a, **kw)
    return wrap


def admin_required(f):
    @wraps(f)
    def wrap(*a, **kw):
        user = session.get("user")
        if not user:
            return redirect(url_for("login", next=request.path))
        if user.get("role") != "admin":
            flash("관리자만 접근할 수 있습니다.", "err")
            return redirect(url_for("accounts"))
        return f(*a, **kw)
    return wrap


def _safe_next_url(target):
    """로그인 후 이동은 같은 사이트의 상대 경로만 허용한다."""
    if not target:
        return None
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return None
    if not target.startswith("/") or target.startswith("//"):
        return None
    return target


def _mask_account_no(no):
    """이체 상대 계좌번호를 거래내역 목록에 노출할 때 중간 자리를 가린다."""
    m = re.match(r"^(\d{3}-\d{3}-)(\d{6})$", no or "")
    if not m:
        return no
    return f"{m.group(1)}***{m.group(2)[-3:]}"


def _safe(fn, fallback):
    """관리자 대시보드용: 외부 서비스 호출이 죽어도 패널 하나만 '연결 안 됨'으로 표시하고
    나머지 패널·페이지 전체는 절대 죽지 않게 한다."""
    try:
        return fn()
    except Exception as e:
        print(f"[admin] 데이터 조회 실패: {e}")
        return fallback


# --------------------------- ES 데이터 헬퍼 (Redis 캐시) ---------------------------
def es_status(client):
    """관리자 대시보드용 ES 상태 요약. 클러스터/인덱스 각각 개별적으로 실패를 허용한다."""
    status = {"connected": False, "cluster_status": None,
              "products_count": None, "keystat_count": None}
    try:
        health = client.cluster.health()
        status["connected"] = True
        status["cluster_status"] = health.get("status")
    except Exception as e:
        status["error"] = str(e)
        return status
    try:
        status["products_count"] = client.count(index="fss-products")["count"]
    except Exception:
        pass
    try:
        status["keystat_count"] = client.count(index="bok-keystat")["count"]
    except Exception:
        pass
    return status


def _best_rate(d, term="12"):
    rates = [o.get("intr_rate2") for o in d.get("options", [])
             if o.get("save_trm") == term and o.get("intr_rate2") is not None]
    return max(rates) if rates else None


def _top_product(ptype):
    r = es_quick().search(index="fss-products", size=300,
                          query={"term": {"product_type": ptype}},
                          source_excludes=["embedding"])
    best = None
    for h in r["hits"]["hits"]:
        s = h["_source"]
        rate = _best_rate(s)
        if rate is not None and (best is None or rate > best["rate"]):
            best = {"type": ptype, "rate": rate,
                    "co": s["kor_co_nm"], "name": s["fin_prdt_nm"]}
    return best


def rate_cards():
    """메인용 대표 금리 카드 (1시간 캐시)."""
    if not es_online():
        return []
    def produce():
        cards = []
        for ptype in ["정기예금", "적금"]:
            c = _top_product(ptype)
            if c:
                cards.append(c)
        return cards
    try:
        val, _ = cache.cached_call("ratecards", cache.TTL_SEARCH, produce)
        return val
    except Exception:
        return []


def rate_ranking(n=5):
    """정기예금·적금 통합 금리 TOP N (1시간 캐시)."""
    if not es_online():
        return []
    def produce():
        items = []
        for ptype in ["정기예금", "적금"]:
            r = es_quick().search(index="fss-products", size=300,
                                  query={"term": {"product_type": ptype}},
                                  source_excludes=["embedding"])
            for h in r["hits"]["hits"]:
                s = h["_source"]
                rate = _best_rate(s)
                if rate is not None:
                    items.append({"type": ptype, "rate": rate,
                                  "co": s["kor_co_nm"], "name": s["fin_prdt_nm"]})
        items.sort(key=lambda x: x["rate"], reverse=True)
        for i, it in enumerate(items[:n], start=1):
            it["rank"] = i
        return items[:n]
    try:
        val, _ = cache.cached_call("rateranking", cache.TTL_SEARCH, produce)
        return val
    except Exception:
        return []


def base_rate():
    """한국은행 기준금리 (1시간 캐시)."""
    if not es_online():
        return None
    def produce():
        r = es_quick().search(index="bok-keystat", size=1,
                              query={"match_phrase": {"keystat_name": "한국은행 기준금리"}},
                              source_excludes=["embedding"])
        hits = r["hits"]["hits"]
        return hits[0]["_source"]["data_value"] if hits else None
    try:
        val, _ = cache.cached_call("baserate", cache.TTL_SEARCH, produce)
        return val
    except Exception:
        return None


def product_search(query, k=12):
    """의미검색으로 상품 목록 (예적금·대출검색 페이지용)."""
    if not es_online():
        return []
    qv = cache.cached_embed_query(query)
    r = es_quick().search(index="fss-products", size=k,
                          knn={"field": "embedding", "query_vector": qv,
                               "k": k, "num_candidates": 100},
                          source_excludes=["embedding"])
    out = []
    for h in r["hits"]["hits"]:
        s = h["_source"]
        out.append({"type": s.get("product_type"), "co": s.get("kor_co_nm"),
                    "name": s.get("fin_prdt_nm"), "rate": _best_rate(s),
                    "grp": s.get("fin_grp_nm")})
    return out


def rag_answer(question, k=5):
    """RAG: 질문 → ES 의미검색 → 근거 → Upstage LLM 답변.

    답변 캐싱: 같은 질문이면 임베딩·검색·LLM을 모두 건너뛰고 Redis에서 즉시 반환.
    (LLM 생성이 응답시간의 ~96%라 효과가 큼. 금리 재적재 시 flush_kind('answer')로 무효화.)
    """
    cached = cache.cache_get("answer", question)
    if cached is not None:
        return cached["answer"], True   # (답변, 캐시히트)

    if not es_online():
        return "현재 상품 검색엔진에 연결할 수 없어 근거 검색 답변을 생성할 수 없습니다. 잠시 후 다시 시도해 주세요.", False

    qv = cache.cached_embed_query(question)
    r = es_quick().search(index="fss-products", size=k,
                          knn={"field": "embedding", "query_vector": qv,
                               "k": k, "num_candidates": 100},
                          source_excludes=["embedding"])
    docs = [h["_source"] for h in r["hits"]["hits"]]
    context = "\n\n".join(d.get("search_text", "") for d in docs)
    system_prompt = bank_db.render_prompt(bank_db.ensure_prompt(
        "rag_system", "RAG 챗봇(상품검색 /api/ask) 시스템 프롬프트", rag_core.SYSTEM_PROMPT_DEFAULT))
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"참고 자료:\n{context}\n\n질문: {question}"},
    ]
    resp = rag_core.client.chat.completions.create(
        model=rag_core.CHAT_MODEL, messages=messages, temperature=0.2)
    answer = resp.choices[0].message.content
    cache.cache_set("answer", {"answer": answer}, cache.TTL_SEARCH, question)
    return answer, False


# --------------------------- 핀픽 자체 상품 ---------------------------
OUR_TAB = "핀픽 상품"
FINPICK_PRODUCTS = [
    # 예금형 — 가입 시 계좌에서 예치(출금)
    {"code": "FP-D12", "kind": "예금", "type": "정기예금", "name": "핀픽 으뜸예금", "rate": 3.60,
     "term": 12, "min": 100000, "sub": "12개월", "desc": "12개월 단리·만기일시지급. 목돈 굴리기 좋은 대표 예금."},
    {"code": "FP-D24", "kind": "예금", "type": "정기예금", "name": "핀픽 장기든든예금", "rate": 3.90,
     "term": 24, "min": 100000, "sub": "24개월", "desc": "24개월 장기 예치 고금리 예금."},
    {"code": "FP-S12", "kind": "예금", "type": "적금", "name": "핀픽 매일적금", "rate": 4.20,
     "term": 12, "min": 10000, "sub": "12개월", "desc": "12개월 자유적립식. 매일 조금씩 모으는 습관 적금."},
    {"code": "FP-PARK", "kind": "예금", "type": "파킹통장", "name": "핀픽 파킹플러스", "rate": 2.80,
     "term": 0, "min": 0, "sub": "수시입출", "desc": "수시입출금·하루만 맡겨도 이자. 비상금 보관에 딱."},
    {"code": "FP-PEN", "kind": "예금", "type": "연금저축", "name": "핀픽 미래연금저축", "rate": 4.00,
     "term": 12, "min": 50000, "sub": "연금저축", "desc": "노후 대비 세제혜택 연금저축. 길게 모을수록 유리."},
    # 펀드형 — 매수(투자), 원금 비보장
    {"code": "FP-FUND1", "kind": "예금", "type": "펀드", "name": "핀픽 글로벌성장펀드", "rate": 7.50,
     "term": 0, "min": 10000, "sub": "예상 연수익률 · 고위험",
     "desc": "글로벌 우량주에 투자하는 주식형 펀드. 장기 성장 추구(원금 비보장). 소액으로도 시작 가능."},
    {"code": "FP-FUND2", "kind": "예금", "type": "펀드", "name": "핀픽 든든채권펀드", "rate": 3.80,
     "term": 0, "min": 10000, "sub": "예상 연수익률 · 저위험",
     "desc": "우량 채권 중심의 채권형 펀드. 안정적 수익 추구. 소액으로도 시작 가능."},
    # 외환형 — 외화 예치
    {"code": "FP-FX1", "kind": "예금", "type": "외환", "name": "핀픽 달러외화예금", "rate": 4.10,
     "term": 12, "min": 100000, "sub": "연 금리(USD) · 12개월",
     "desc": "미 달러로 예치하는 외화 정기예금. 금리와 환차익을 함께."},
    {"code": "FP-FX2", "kind": "예금", "type": "외환", "name": "핀픽 엔화외화예금", "rate": 0.50,
     "term": 12, "min": 100000, "sub": "연 금리(JPY) · 12개월",
     "desc": "엔화 외화 정기예금. 엔저 활용 예치."},
    # 골드형 — 금 투자(시세 연동)
    {"code": "FP-GOLD", "kind": "예금", "type": "골드", "name": "핀픽 골드뱅킹", "rate": 0,
     "rate_text": "국제 금시세 연동", "term": 0, "min": 10000, "sub": "금 투자",
     "desc": "0.01g 단위로 금에 투자. 실물 보관 없이 간편하게."},
    # 대출형 — 실행 시 계좌로 입금(차입)
    {"code": "FP-MTG", "kind": "대출", "type": "주택담보대출", "name": "핀픽 내집마련대출", "rate": 3.40,
     "term": 360, "min": 1000000, "sub": "최장 30년",
     "desc": "주택 담보 저금리 대출. 내 집 마련의 든든한 시작."},
    {"code": "FP-RENT", "kind": "대출", "type": "전세자금대출", "name": "핀픽 전세안심대출", "rate": 3.50,
     "term": 24, "min": 1000000, "sub": "24개월", "desc": "전세 보증금 마련 대출. 안심하고 이사하세요."},
    {"code": "FP-CREDIT", "kind": "대출", "type": "개인신용대출", "name": "핀픽 직장인신용대출", "rate": 5.40,
     "term": 12, "min": 500000, "sub": "12개월", "desc": "직장인 대상 신용대출. 급할 때 빠르게."},
]


def get_finpick_product(code):
    return next((p for p in FINPICK_PRODUCTS if p["code"] == code), None)


# 핀픽 상품 카탈로그 탭(카테고리)
CATALOG_TABS = ["전체", "예금", "적금", "펀드", "대출", "연금", "외환", "골드"]


def _catalog_group(p):
    t = p["type"]
    if t in ("정기예금", "파킹통장"):
        return "예금"
    if t == "적금":
        return "적금"
    if t == "연금저축":
        return "연금"
    if t in ("펀드", "외환", "골드"):
        return t
    return "대출"  # 주택담보/전세/신용


# --------------------------- 상품 목록/비교 (금감원 공시) ---------------------------
PRODUCT_TABS = ["정기예금", "적금", "주택담보대출", "전세자금대출", "개인신용대출", "연금저축"]
LOAN_TYPES = {"주택담보대출", "전세자금대출", "개인신용대출"}


def _product_rate(d, ptype):
    """상품 대표금리. 예적금은 최고금리(높을수록↑), 대출은 최저금리(낮을수록↑)."""
    opts = d.get("options", [])
    if ptype in ("정기예금", "적금"):
        vals = [o.get("intr_rate2") for o in opts if o.get("intr_rate2") is not None]
        return max(vals) if vals else None
    # 대출/연금: 옵션에서 잡히는 금리 필드 중 최저
    vals = []
    for o in opts:
        for k in ("lend_rate_min", "lend_rate_avg", "lend_rate_max",
                  "crdt_grad_avg", "intr_rate2", "intr_rate"):
            try:
                vals.append(float(o.get(k)))
            except (TypeError, ValueError):
                pass
    return min(vals) if vals else None


DEPOSIT_TABS = {"정기예금", "적금"}
TERM_OPTIONS = [6, 12, 24, 36]


def _rate_at_term(d, term):
    """특정 저축기간(개월)의 최고 금리(intr_rate2)."""
    vals = [o.get("intr_rate2") for o in d.get("options", [])
            if str(o.get("save_trm")) == str(term) and o.get("intr_rate2") is not None]
    return max(vals) if vals else None


def available_groups(ptype):
    """해당 종류에 실제 존재하는 금융권 목록 (드롭다운용, 1시간 캐시)."""
    def produce():
        a = es.search(index="fss-products", size=0, query={"term": {"product_type": ptype}},
                      aggs={"g": {"terms": {"field": "fin_grp_nm", "size": 10}}})
        return [b["key"] for b in a["aggregations"]["g"]["buckets"]]
    val, _ = cache.cached_call("groups", cache.TTL_SEARCH, produce, ptype)
    return val


def list_products_by_type(ptype, term=None, grp=None, n=20):
    """카테고리별 상품을 금리순으로 정렬 + 핀픽 상품을 하이라이트로 끼워넣어 반환.
    예적금은 term(개월) 기준 금리, grp로 금융권 필터. (시중 목록은 1시간 캐시)"""
    is_dep = ptype in DEPOSIT_TABS
    is_high = ptype not in LOAN_TYPES  # 예적금·연금 높은순 / 대출 낮은순

    def produce():
        r = es.search(index="fss-products", size=1000,
                      query={"term": {"product_type": ptype}}, source_excludes=["embedding"])
        rows = []
        for h in r["hits"]["hits"]:
            s = h["_source"]
            rate = _rate_at_term(s, term) if (is_dep and term) else _product_rate(s, ptype)
            rows.append({"co": s["kor_co_nm"], "name": s["fin_prdt_nm"],
                         "grp": s.get("fin_grp_nm"), "rate": rate, "ours": False})
        return rows

    market, _ = cache.cached_call("plist2", cache.TTL_SEARCH, produce, ptype, term or "best")
    if grp and grp != "전체":  # 금융권 필터 (핀픽 상품은 필터와 무관하게 항상 노출)
        market = [x for x in market if x["grp"] == grp]

    # 핀픽 상품 주입 (기간 지정 시 해당 기간 상품만)
    ours = []
    for p in FINPICK_PRODUCTS:
        if p["type"] != ptype:
            continue
        if is_dep and term and p.get("term") != term:
            continue
        ours.append({"co": "FinPick", "grp": "본행", "name": p["name"],
                     "rate": p["rate"], "ours": True, "code": p["code"]})

    combined = [x for x in (ours + market) if x["rate"] is not None]
    combined.sort(key=lambda x: x["rate"], reverse=is_high)
    for i, x in enumerate(combined, 1):
        x["rank"] = i
    top = combined[:n]
    for o in [x for x in combined if x["ours"]]:  # 우리 상품은 순위 밖이어도 항상 노출
        if o not in top:
            top.append(o)
    return top


def market_stats(ptype):
    """해당 종류의 시중 금리 통계(평균·최고·최저·개수). 핀픽 상품 비교용. (1시간 캐시)"""
    if not es_online():
        return None

    def produce():
        r = es_quick().search(index="fss-products", size=1000,
                              query={"term": {"product_type": ptype}},
                              source_excludes=["embedding"])
        rates = []
        for h in r["hits"]["hits"]:
            v = _product_rate(h["_source"], ptype)
            if v is not None:
                rates.append(v)
        if not rates:
            return None
        return {"avg": round(sum(rates) / len(rates), 2), "max": round(max(rates), 2),
                "min": round(min(rates), 2), "n": len(rates)}
    try:
        val, _ = cache.cached_call("mstats", cache.TTL_SEARCH, produce, ptype)
        return val
    except Exception:
        return None


# --------------------------- 금리 대시보드 (ES 집계) ---------------------------
# (종류, 대표금리 방식) — high: 최고금리↑, low: 최저금리↑, pension: 평균 공시이율
DASHBOARD_TYPES = [
    ("정기예금", "high"), ("적금", "high"), ("연금저축", "pension"),
    ("주택담보대출", "low"), ("전세자금대출", "low"), ("개인신용대출", "low"),
]


def pension_stats():
    """연금저축은 금리가 옵션이 아니라 문서의 공시이율(btrm_prft_rate_1)에 있어 별도 집계."""
    if not es_online():
        return None

    def produce():
        a = es_quick().search(index="fss-products", size=0,
                              query={"term": {"product_type": "연금저축"}},
                              aggs={"s": {"stats": {"field": "btrm_prft_rate_1"}}})
        s = a["aggregations"]["s"]
        if not s["count"]:
            return None
        return {"avg": round(s["avg"], 2), "max": round(s["max"], 2),
                "min": round(s["min"], 2), "n": s["count"]}
    try:
        val, _ = cache.cached_call("pstats", cache.TTL_SEARCH, produce)
        return val
    except Exception:
        return None


def rates_dashboard_data():
    """ES 집계로 상품군(fin_grp_nm)·종류(product_type)별 상품 수를 한 번에 뽑는다. (1시간 캐시)"""
    empty = {"groups": [], "type_counts": {}, "total": 0}
    if not es_online():
        return empty

    def produce():
        agg = es_quick().search(index="fss-products", size=0,
                                aggs={"grp": {"terms": {"field": "fin_grp_nm", "size": 20}},
                                      "typ": {"terms": {"field": "product_type", "size": 20}}})
        groups = [{"name": b["key"], "count": b["doc_count"]}
                  for b in agg["aggregations"]["grp"]["buckets"]]
        type_counts = {b["key"]: b["doc_count"] for b in agg["aggregations"]["typ"]["buckets"]}
        return {"groups": groups, "type_counts": type_counts,
                "total": sum(type_counts.values())}
    try:
        val, _ = cache.cached_call("ratesdash", cache.TTL_SEARCH, produce)
        return val or empty
    except Exception:
        return empty


# --------------------------- 페이지 ---------------------------
@app.route("/")
def index():
    bank_db.refresh_market(STOCK_PREDICT_REWARD)
    events = [p for p in BOARD_POSTS if p["cat"] == "이벤트"][:4]
    notices = [p for p in BOARD_POSTS if p["cat"] == "공지사항"][:4]
    return render_template("index.html", active="home",
                           cards=rate_cards(), base_rate=base_rate(), market=bank_db.get_market_index(),
                           ranking=rate_ranking(), events=events, notices=notices,
                           faq=HOME_FAQ)


@app.route("/board")
def board():
    bcat = request.args.get("bcat", "전체")
    if bcat not in ("전체", "이벤트", "공지사항"):
        bcat = "전체"
    posts = [p for p in BOARD_POSTS if bcat == "전체" or p["cat"] == bcat]
    return render_template("board.html", active="", bcat=bcat, posts=posts)


@app.route("/products")
def products():
    q = (request.args.get("q") or "").strip()
    if q:  # AI 검색 모드
        try:
            results = product_search(q)
        except Exception as e:
            print(f"[products] 검색 실패: {e}")
            results = []
        return render_template("products.html", active="products", mode="search",
                               q=q, results=results, cat_tabs=CATALOG_TABS)
    # 핀픽 상품 카탈로그 (카테고리별)
    cat = request.args.get("cat") or "전체"
    if cat not in CATALOG_TABS:
        cat = "전체"
    items = [p for p in FINPICK_PRODUCTS if cat == "전체" or _catalog_group(p) == cat]
    mstats = {t: _safe(lambda typ=t: market_stats(typ), None) for t in {p["type"] for p in items}}
    return render_template("products.html", active="products", mode="catalog",
                           cat_tabs=CATALOG_TABS, active_cat=cat, items=items, mstats=mstats)


@app.route("/rates")
def rates_dashboard():
    """금리 대시보드: ES에 색인된 모든 상품군·종류별 상품 수와 대표금리, 핀픽 펀드 금리를 한눈에."""
    dash = rates_dashboard_data()
    max_grp = max((g["count"] for g in dash["groups"]), default=1) or 1
    for g in dash["groups"]:
        g["pct"] = round(g["count"] / max_grp * 100)
    kind_label = {"high": "최고금리", "low": "최저금리", "pension": "평균 공시이율"}
    types = []
    for ptype, kind in DASHBOARD_TYPES:
        stats = pension_stats() if kind == "pension" else market_stats(ptype)
        rep = None
        if stats:
            rep = stats["max"] if kind == "high" else (stats["min"] if kind == "low" else stats["avg"])
        types.append({"type": ptype, "count": dash["type_counts"].get(ptype, 0),
                      "stats": stats, "rep": rep, "kind": kind,
                      "kind_label": kind_label[kind]})
    funds = [p for p in FINPICK_PRODUCTS if p["type"] == "펀드"]
    return render_template("rates.html", active="rates",
                           groups=dash["groups"], types=types, total=dash["total"],
                           base_rate=base_rate(), funds=funds)


@app.route("/subscribe/<code>", methods=["GET", "POST"])
@login_required
def subscribe(code):
    product = get_finpick_product(code)
    if not product:
        flash("존재하지 않는 상품입니다.", "err")
        return redirect(url_for("products"))
    uid = session["user"]["id"]
    # 가입 출금은 항상 입출금 통장(main)에서만 처리 (포인트머니 통장 제외)
    accts = [a for a in bank_db.get_accounts(uid) if a["acct_type"] == "main"]
    if request.method == "POST":
        try:
            amount = int((request.form.get("amount") or "0").replace(",", ""))
        except ValueError:
            amount = 0
        ok, msg = bank_db.subscribe(uid, product, amount)
        flash(msg, "ok" if ok else "err")
        if ok:
            return redirect(url_for("accounts"))
    return render_template("subscribe.html", active="products",
                           product=product, accounts=accts)


@app.route("/terms")
def terms():
    return render_template("terms.html", active="")


@app.route("/privacy")
def privacy():
    return render_template("privacy.html", active="")


# --------------------------- 인증 ---------------------------
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        name = (request.form.get("name") or "").strip()
        if not (username and password and name):
            flash("모든 항목을 입력하세요.", "err")
        elif len(password) < 4:
            flash("비밀번호는 4자 이상이어야 합니다.", "err")
        else:
            ok, msg = bank_db.create_user(username, password, name)
            if ok:
                flash("회원가입 완료! 계좌가 개설되었습니다(축하금 100만원). 로그인하세요.", "ok")
                return redirect(url_for("login"))
            flash(msg, "err")
    return render_template("register.html", active="")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = bank_db.verify_user(request.form.get("username", ""),
                                   request.form.get("password", ""))
        if user:
            session["user"] = user
            next_url = _safe_next_url(request.args.get("next"))
            if next_url:
                return redirect(next_url)
            if user.get("role") == "admin":
                return redirect(url_for("admin_dashboard"))
            return redirect(url_for("accounts"))
        flash("아이디 또는 비밀번호가 올바르지 않습니다.", "err")
    return render_template("login.html", active="")


@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("index"))


CACHE_KINDS = [
    ("embq", "질문 임베딩"), ("search", "검색 결과"), ("answer", "RAG 챗봇 답변"),
    ("ratecards", "대표금리 카드"), ("baserate", "기준금리"), ("groups", "상품 그룹"),
    ("plist2", "상품 목록"), ("mstats", "시장 통계"),
]


def _gauge_color(pct):
    if pct is None:
        return "#8b8b93"
    if pct >= 70:
        return "#0a8f3c"
    if pct >= 40:
        return "#e8a33d"
    return "#d33d3d"


DOCKER_CONTAINERS = [
    ("phoenix", "피닉스"), ("redis-bank", "Redis"),
    ("es-bank", "Elasticsearch"), ("kafchat-kafka", "카프카"),
]
AGENT_DOMAIN_COLORS = {
    "customer": "#5308C4",
    "fraud": "#d33d3d",
    "credit": "#0a8f3c",
    "compliance": "#2563c9",
}


def docker_stats():
    """관리자 대시보드용 실제 컨테이너 리소스 사용량 (docker stats). docker 명령이 없거나
    컨테이너가 없으면 조용히 빈 리스트 — 지어내지 않는다(이건 실제로 공짜로 구할 수 있는 값)."""
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemPerc}}"],
            capture_output=True, text=True, timeout=5)
        rows = {}
        for line in out.stdout.strip().splitlines():
            parts = line.split("|")
            if len(parts) == 3:
                name, cpu, mem = parts
                rows[name] = {"cpu": float(cpu.strip("% \n")), "mem": float(mem.strip("% \n"))}
    except Exception:
        return []
    def usage_color(pct):
        if pct >= 80:
            return "#d33d3d"
        if pct >= 50:
            return "#e8a33d"
        return "#0a8f3c"

    result = []
    for cid, label in DOCKER_CONTAINERS:
        if cid in rows:
            cpu, mem = rows[cid]["cpu"], rows[cid]["mem"]
            result.append({"name": label, "cpu": cpu, "mem": mem,
                           "cpu_w": min(cpu, 100), "mem_w": min(mem, 100),
                           "cpu_color": usage_color(cpu), "mem_color": usage_color(mem),
                           "cpu_chart": build_chart(f"cpu_{cid}", unit="%")})
    return result


def build_chart(metric, vmax=None, vmin=0, unit=""):
    """monitoring.py의 실측 시계열(metric)을 SVG 라인/영역 차트용 좌표로 변환.
    점이 2개 미만이면 None(화면엔 '데이터 수집 중'). 지어내지 않고 쌓인 실측치만 그린다."""
    data = monitoring.series(metric)
    if len(data) < 2:
        return {"collecting": True, "points_count": len(data)}
    vals = [p["v"] for p in data]
    hi = vmax if vmax is not None else (max(vals) * 1.25 or 1)
    if hi <= vmin:
        hi = vmin + 1
    W = 285.0
    step = W / (len(data) - 1)

    def y(v):
        r = (v - vmin) / (hi - vmin)
        return round(100 - max(0.0, min(1.0, r)) * 100, 1)

    pts = " ".join(f"{round(i * step, 1)},{y(p['v'])}" for i, p in enumerate(data))
    area = f"0,100 {pts} {round((len(data) - 1) * step, 1)},100"
    return {"collecting": False, "points": pts, "area": area,
            "vmax": round(hi, 1), "unit": unit,
            "last": vals[-1], "t_first": data[0]["t"], "t_last": data[-1]["t"]}


def build_chart_from_points(data, vmax=None, vmin=0, unit=""):
    """[{t:'HH:MM', v:숫자}] 형태의 포인트를 차트로 변환."""
    if len(data) < 2:
        return {"collecting": True, "points_count": len(data)}
    vals = [p["v"] for p in data]
    hi = vmax if vmax is not None else (max(vals) * 1.25 or 1)
    if hi <= vmin:
        hi = vmin + 1
    W = 285.0
    step = W / (len(data) - 1)

    def y(v):
        r = (v - vmin) / (hi - vmin)
        return round(100 - max(0.0, min(1.0, r)) * 100, 1)

    pts = " ".join(f"{round(i * step, 1)},{y(p['v'])}" for i, p in enumerate(data))
    area = f"0,100 {pts} {round((len(data) - 1) * step, 1)},100"
    return {"collecting": False, "points": pts, "area": area,
            "vmax": round(hi, 1), "unit": unit,
            "last": vals[-1], "t_first": data[0]["t"], "t_last": data[-1]["t"]}


def _bucket_agent_rows(rows, step_minutes=5, window_minutes=60):
    now = datetime.now().replace(second=0, microsecond=0)
    slot_count = max(2, window_minutes // step_minutes + 1)
    slots = []
    for idx in range(slot_count):
        t = now - timedelta(minutes=(slot_count - 1 - idx) * step_minutes)
        slots.append({
            "dt": t,
            "t": t.strftime("%H:%M"),
            "calls": 0,
            "ok": 0,
            "fail": 0,
            "elapsed": [],
        })

    start = slots[0]["dt"]
    for row in rows:
        try:
            dt = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
        except Exception:
            continue
        if dt < start or dt > now:
            continue
        bucket_idx = int((dt - start).total_seconds() // (step_minutes * 60))
        bucket_idx = max(0, min(bucket_idx, len(slots) - 1))
        slot = slots[bucket_idx]
        slot["calls"] += 1
        if row.get("ok"):
            slot["ok"] += 1
        else:
            slot["fail"] += 1
        slot["elapsed"].append(float(row.get("elapsed_ms") or 0))

    def make_points(key):
        points = []
        for slot in slots:
            if key == "calls":
                value = slot["calls"]
            elif key == "success":
                value = round(slot["ok"] / slot["calls"] * 100, 1) if slot["calls"] else 0
            elif key == "errors":
                value = round(slot["fail"] / slot["calls"] * 100, 1) if slot["calls"] else 0
            else:  # p95
                vals = sorted(slot["elapsed"])
                if vals:
                    idx = min(len(vals) - 1, int(len(vals) * 0.95))
                    value = round(vals[idx], 1)
                else:
                    value = 0
            points.append({"t": slot["t"], "v": value})
        return points

    return {
        "calls": make_points("calls"),
        "success": make_points("success"),
        "errors": make_points("errors"),
        "p95": make_points("p95"),
    }


def build_agent_panel():
    """관리자 화면의 에이전트 모니터링 패널 데이터."""
    rows = bank_db.agent_logs_in_window(60)
    summary = {
        "calls": len(rows),
        "ok": sum(1 for r in rows if r.get("ok")),
        "fail": sum(1 for r in rows if not r.get("ok")),
        "window_label": "최근 60분",
        "last_when": rows[0]["created_at"][5:16] if rows else None,
    }
    summary["success_rate"] = round(summary["ok"] / summary["calls"] * 100, 1) if summary["calls"] else None
    elapsed = [float(r.get("elapsed_ms") or 0) for r in rows]
    summary["avg_ms"] = round(sum(elapsed) / len(elapsed), 1) if elapsed else None
    if elapsed:
        vals = sorted(elapsed)
        idx = min(len(vals) - 1, int(len(vals) * 0.95))
        summary["p95_ms"] = round(vals[idx], 1)
    else:
        summary["p95_ms"] = None
    summary["answer_avg"] = round(sum(int(r.get("answer_chars") or 0) for r in rows) / len(rows), 1) if rows else None

    domain_names = ("customer", "fraud", "credit", "compliance")
    domain_max = max((sum(1 for r in rows if r["domain"] == d) for d in domain_names), default=0) or 1

    domains = []
    for domain in domain_names:
        d_rows = [r for r in rows if r["domain"] == domain]
        d_calls = len(d_rows)
        d_ok = sum(1 for r in d_rows if r.get("ok"))
        d_fail = d_calls - d_ok
        d_elapsed = [float(r.get("elapsed_ms") or 0) for r in d_rows]
        d_success = round(d_ok / d_calls * 100, 1) if d_calls else None
        d_avg = round(sum(d_elapsed) / len(d_elapsed), 1) if d_elapsed else None
        if d_elapsed:
            vals = sorted(d_elapsed)
            idx = min(len(vals) - 1, int(len(vals) * 0.95))
            d_p95 = round(vals[idx], 1)
        else:
            d_p95 = None
        color = AGENT_DOMAIN_COLORS.get(domain, "var(--primary)")
        if d_success is None:
            level = "idle"
        elif d_success < 90 or (d_p95 or 0) > 8000:
            level = "bad"
        elif d_success < 97 or (d_p95 or 0) > 4000:
            level = "warn"
        else:
            level = "good"
        last = d_rows[0] if d_rows else None
        d_bucketed = _bucket_agent_rows(d_rows, step_minutes=5, window_minutes=60)
        domains.append({
            "domain": domain,
            "label": monitoring.AGENT_LABELS.get(domain, domain),
            "calls": d_calls,
            "ok": d_ok,
            "fail": d_fail,
            "success_rate": d_success,
            "avg_ms": d_avg,
            "p95_ms": d_p95,
            "share_pct": round(d_calls / summary["calls"] * 100, 1) if summary["calls"] else 0,
            "last_when": last["created_at"][5:16] if last else None,
            "last_status": "success" if last and last.get("ok") else ("error" if last else None),
            "last_user_id": last.get("user_id") if last else None,
            "last_message": last.get("question", "") if last else "",
            "color": color,
            "level": level,
            "bar_pct": round(d_calls / domain_max * 100) if domain_max else 0,
            "call_chart": build_chart_from_points(d_bucketed["calls"], unit="건"),
            "latency_chart": build_chart_from_points(d_bucketed["p95"], unit="ms"),
        })

    route_customer = sum(1 for r in rows if r.get("route") == "customer")
    route_admin = sum(1 for r in rows if r.get("route") == "admin")
    route_total = route_customer + route_admin or 1
    route_split = [
        {"label": "고객", "count": route_customer, "pct": round(route_customer / route_total * 100, 1)},
        {"label": "관리자", "count": route_admin, "pct": round(route_admin / route_total * 100, 1)},
    ]
    route_customer_deg = round((route_split[0]["pct"] if route_split else 0) * 3.6, 1)
    summary_cards = [
        {"label": "최근 1시간 호출", "value": f"{summary['calls']:,}건"},
        {"label": "성공률", "value": f"{summary['success_rate']}%" if summary["success_rate"] is not None else "집계중"},
        {"label": "평균 지연", "value": f"{summary['avg_ms']}ms" if summary["avg_ms"] is not None else "집계중"},
        {"label": "p95 지연", "value": f"{summary['p95_ms']}ms" if summary["p95_ms"] is not None else "집계중"},
        {"label": "실패 건수", "value": f"{summary['fail']:,}건"},
        {"label": "평균 답변 길이", "value": f"{summary['answer_avg']}자" if summary["answer_avg"] is not None else "집계중"},
    ]
    recent = []
    for row in bank_db.recent_agent_logs(40):
        recent.append({
            "when": row["created_at"],
            "domain": row["domain"],
            "label": monitoring.AGENT_LABELS.get(row["domain"], row["domain"]),
            "route": row["route"],
            "source": row.get("source") or "web",
            "user_id": row.get("user_id"),
            "ok": bool(row.get("ok")),
            "status": "success" if row.get("ok") else "error",
            "elapsed_ms": round(float(row.get("elapsed_ms") or 0), 1),
            "message": row.get("question") or "",
            "scenario_label": row.get("scenario_label") or "",
            "error": row.get("error") or "",
            "color": AGENT_DOMAIN_COLORS.get(row["domain"], "#5308C4"),
        })

    incidents = []
    if summary["success_rate"] is not None and summary["success_rate"] < 90:
        incidents.append({"level": "warning", "message": "에이전트 성공률 90% 미만", "value": summary["success_rate"]})
    if summary["p95_ms"] is not None and summary["p95_ms"] > 8000:
        incidents.append({"level": "warning", "message": "에이전트 p95 지연 8초 초과", "value": summary["p95_ms"]})
    if summary["fail"]:
        incidents.append({"level": "info", "message": "최근 1시간 내 에이전트 실패 발생", "value": summary["fail"]})

    bucketed = _bucket_agent_rows(rows, step_minutes=5, window_minutes=60)

    return {
        "summary": summary,
        "summary_cards": summary_cards,
        "domains": domains,
        "recent": recent,
        "incidents": incidents,
        "route_split": route_split,
        "route_customer_deg": route_customer_deg,
        "chart_calls": build_chart_from_points(bucketed["calls"], unit="건"),
        "chart_p95": build_chart_from_points(bucketed["p95"], unit="ms"),
        "chart_success": build_chart_from_points(bucketed["success"], vmax=100, unit="%"),
        "chart_errors": build_chart_from_points(bucketed["errors"], vmax=100, unit="%"),
    }


def build_kafka_panel(kafka_data):
    metrics = kafka_data.get("metrics") or {}
    runtime = kafka_data.get("runtime") or {}
    topic_runtime = {t["topic"]: t for t in runtime.get("topics", [])}

    topic_rows = []
    for t in metrics.get("topics", []) or kafka_data.get("topics", []):
        topic = t.get("topic") or t.get("name")
        rt = topic_runtime.get(topic, {})
        topic_rows.append({
            "topic": topic,
            "purpose": t.get("purpose", ""),
            "messages": t.get("messages"),
            "partitions": t.get("partitions"),
            "delta_30s": int(monitoring.latest(f"kafka_in_{topic.replace('-', '_')}") or 0),
            "published_ok": rt.get("published_ok", 0),
            "published_fail": rt.get("published_fail", 0),
            "consumed": rt.get("consumed", 0),
            "last_published_ts": rt.get("last_published_ts"),
            "last_consumed_ts": rt.get("last_consumed_ts"),
            "last_error": rt.get("last_error"),
        })

    workers = []
    for w in runtime.get("workers", []):
        level = "good" if w.get("alive") and w.get("status") == "running" else ("warn" if w.get("alive") else "bad")
        workers.append({
            "label": w.get("label", w.get("name")),
            "topic": w.get("topic"),
            "alive": w.get("alive"),
            "status": w.get("status"),
            "processed": w.get("processed", 0),
            "restarts": w.get("restarts", 0),
            "last_seen": w.get("last_seen"),
            "last_error": w.get("last_error"),
            "level": level,
        })

    summary = runtime.get("summary", {})
    lag_now = metrics.get("lag")
    incidents = []
    if lag_now:
        incidents.append({"level": "warning", "message": "이체요청 큐 랙이 쌓이고 있어요.", "value": f"{lag_now}건"})
    if summary.get("published_fail_total"):
        incidents.append({"level": "warning", "message": "카프카 발행 실패가 발생했어요.", "value": f"{summary['published_fail_total']}건"})
    dead_workers = [w for w in workers if not w["alive"]]
    if dead_workers:
        incidents.append({"level": "warning", "message": "중단된 Kafka worker가 있어요.", "value": ", ".join(w["label"] for w in dead_workers)})

    return {
        "summary": {
            "lag": lag_now,
            "in_total_now": int(monitoring.latest("kafka_in_total") or 0),
            "workers_alive": summary.get("workers_alive", 0),
            "workers_total": summary.get("workers_total", 0),
            "published_fail_total": summary.get("published_fail_total", 0),
        },
        "topic_rows": topic_rows,
        "workers": workers,
        "chart_total_in": build_chart("kafka_in_total", unit="건"),
        "chart_lag": build_chart("kafka_lag", unit="건"),
        "chart_consult": build_chart("kafka_in_bank_consult", unit="건"),
        "chart_transfer_req": build_chart("kafka_in_bank_transfer_requests", unit="건"),
        "incidents": incidents,
    }


def build_overview(kafka_data, cache_data, search_data, db_data, kb_data, consult_data, agent_data=None):
    """관리자 대시보드 첫 화면(종합현황)용 요약. 게이지·카드·상태표·막대그래프는 실측치.
    추이 그래프·이상 이벤트도 실측 시계열(monitoring.py) 기반 — 지어내지 않는다."""
    hit_rate = cache_data.get("hit_rate")
    gauges = [
        {"label": "캐시 히트율", "pct": hit_rate if hit_rate is not None else 0,
         "value_text": f"{hit_rate}%" if hit_rate is not None else "–",
         "color": _gauge_color(hit_rate)},
    ]

    stat_cards = [
        {"label": "회원 수", "value": f"{db_data.get('user_count', 0):,}명"},
        {"label": "누적 거래건수", "value": f"{db_data.get('transaction_count', 0):,}건"},
        {"label": "상담봇 지식베이스", "value": f"{kb_data.get('total_chunks', 0):,}건"},
        {"label": "상담 누적 메시지", "value": f"{consult_data.get('total_messages', 0):,}건"},
    ]
    if agent_data:
        stat_cards.append({"label": "에이전트 호출(1h)", "value": f"{agent_data['summary']['calls']:,}건"})

    status_rows = [
        {"name": "카프카", "ok": kafka_data.get("connected") and not kafka_data.get("error"),
         "detail": kafka_data.get("broker", "–")},
        {"name": "캐시(Redis)", "ok": not cache_data.get("error"),
         "detail": f"히트율 {hit_rate}%" if hit_rate is not None else "데이터 없음"},
        {"name": "검색엔진(ES)", "ok": search_data.get("connected"),
         "detail": search_data.get("cluster_status") or "–"},
        {"name": "데이터베이스", "ok": not db_data.get("error"), "detail": "정상 조회됨"},
        {"name": "상담봇 지식베이스", "ok": not kb_data.get("error") and kb_data.get("total_chunks", 0) > 0,
         "detail": f"{kb_data.get('total_chunks', 0)}건 색인됨"},
        {"name": "실시간 상담", "ok": not consult_data.get("error"),
         "detail": f"채널 {consult_data.get('active_channels', 0)}개 활동"},
    ]
    if agent_data:
        s = agent_data["summary"]
        ok = (s.get("success_rate") or 0) >= 90 if s.get("success_rate") is not None else True
        detail = "집계중" if s.get("success_rate") is None else f"성공률 {s['success_rate']}% · p95 {s['p95_ms']}ms"
        status_rows.append({"name": "AI 에이전트", "ok": ok, "detail": detail})

    kb_folders = kb_data.get("folders") or []
    max_chunks = max((f["chunks"] for f in kb_folders), default=0) or 1
    kb_bars = [{"label": f["folder"], "count": f["chunks"],
                "pct": round(f["chunks"] / max_chunks * 100)} for f in kb_folders]

    containers = docker_stats()

    chart_p95 = build_chart("req_p95", unit="ms")
    chart_hit = build_chart("cache_hit", vmax=100, unit="%")
    incidents = monitoring.detect_incidents()

    return {"gauges": gauges, "stat_cards": stat_cards, "status_rows": status_rows,
            "kb_bars": kb_bars, "containers": containers,
            "chart_p95": chart_p95, "chart_hit": chart_hit,
            "p95_now": monitoring.latest("req_p95"),
            "incidents": incidents}


KIBANA_VIEW_LABELS = {
    "bank-transfers": "전체 이체 이벤트",
    "bok-keystat": "기준금리 값",
    "fss-products": "상품",
}


def kibana_data_views(base):
    """키바나에 등록된 데이터 뷰들을 Discover 딥링크와 함께 반환(10분 캐시).
    데이터 뷰 id는 환경마다 다르므로 하드코딩하지 않고 Kibana API로 조회.
    조회 실패 시 빈 리스트 — 화면은 홈(base)으로 폴백.

    이체(bank-transfers)처럼 시간 필드가 있는 인덱스는 Discover 기본 창이 '최근 15분'이라
    과거 데이터가 안 보인다. 그래서 넓은 시간범위(_g, 최근 15년)를 함께 실어 바로 보이게 한다."""
    def produce():
        try:
            r = requests.get(f"{base}/api/data_views", headers={"kbn-xsrf": "true"}, timeout=4)
            views = r.json().get("data_view", [])
            out = []
            for v in views:
                title = v.get("title")
                display_name = KIBANA_VIEW_LABELS.get(title) or v.get("name") or title
                url = (f"{base}/app/discover#/?_a=(index:'{v['id']}')"
                       "&_g=(time:(from:now-15y,to:now))")
                out.append({"title": title, "name": display_name, "url": url})
            return out
        except Exception as e:
            print("[kibana] 데이터 뷰 조회 실패:", e)
            return []
    views, _ = cache.cached_call("kibanadv2", 600, produce, base)
    return views


@app.route("/admin")
@admin_required
def admin_dashboard():
    kafka_data = _safe(bank_kafka.kafka_status,
                        {"broker": bank_kafka.BROKER, "connected": False,
                         "topics": bank_kafka.TOPIC_INFO, "error": True})

    # 카프카 심화지표(토픽별 메시지 수·컨슈머 랙) — admin/consumer API 실측, 15초 캐시로 페이지 지연 방지
    if kafka_data.get("connected"):
        km, _ = _safe(lambda: cache.cached_call("kafkametrics", 15, bank_kafka.kafka_topic_metrics),
                      ({"topics": [], "lag": None}, False))
        kafka_data["metrics"] = km
        kafka_data["runtime"] = _safe(bank_kafka.kafka_runtime_metrics,
                                       {"topics": [], "workers": [], "summary": {}})
    kafka_data["panel"] = build_kafka_panel(kafka_data)

    cache_data = _safe(cache.stats, {"error": True})
    if not cache_data.get("error"):
        hits, misses = cache_data.get("hits") or 0, cache_data.get("misses") or 0
        cache_data["hit_rate"] = round(hits / (hits + misses) * 100, 1) if (hits + misses) else None
        # 히트/미스 도넛차트용 각도(conic-gradient)
        total = hits + misses
        cache_data["hit_deg"] = round(hits / total * 360, 1) if total else 0
        # 실측 시계열 차트(구간 히트율 / 메모리)
        cache_data["chart_hit"] = build_chart("cache_hit", vmax=100, unit="%")
        cache_data["chart_mem"] = build_chart("cache_mem", unit="MB")

    search_data = _safe(lambda: es_status(es), {"connected": False, "error": True})
    if search_data.get("connected"):
        pc = search_data.get("products_count") or 0
        kc = search_data.get("keystat_count") or 0
        m = max(pc, kc) or 1
        search_data["bars"] = [
            {"label": "fss-products", "count": pc, "pct": round(pc / m * 100)},
            {"label": "bok-keystat", "count": kc, "pct": round(kc / m * 100)},
        ]

    db_data = _safe(bank_db.admin_stats, {"error": True})
    recent_tx = _safe(lambda: bank_db.recent_transactions(20), [])
    if recent_tx:
        in_cnt = sum(1 for t in recent_tx if t["kind"] == "입금")
        out_cnt = len(recent_tx) - in_cnt
        db_data["tx_deg"] = round(in_cnt / len(recent_tx) * 360, 1)
        db_data["tx_in_cnt"], db_data["tx_out_cnt"] = in_cnt, out_cnt

    kb_data = _safe(rag_core.kb_stats, {"error": True})
    consult_data = _safe(bank_kafka.consult_status, {"error": True})
    if consult_data.get("channels"):
        m = max(c["message_count"] for c in consult_data["channels"]) or 1
        for c in consult_data["channels"]:
            c["pct"] = round(c["message_count"] / m * 100)

    phoenix_endpoint = os.environ.get("PHOENIX_ENDPOINT", "http://localhost:6006/v1/traces")
    phoenix_url = phoenix_endpoint.replace("/v1/traces", "")
    kibana_base = os.environ.get("KIBANA_URL", "http://localhost:5601")
    kibana_views = _safe(lambda: kibana_data_views(kibana_base), [])
    # 기본 iframe: 이체 이벤트(bank-transfers) 우선 → 없으면 상품 → 없으면 첫 뷰 → 없으면 홈
    _kv = {v["title"]: v["url"] for v in kibana_views}
    kibana_discover = (_kv.get("bank-transfers") or _kv.get("fss-products")
                       or (kibana_views[0]["url"] if kibana_views else kibana_base))

    bank_db.ensure_prompt("rag_system", "RAG 챗봇(상품검색 /api/ask) 시스템 프롬프트",
                          rag_core.SYSTEM_PROMPT_DEFAULT)
    bank_consult_bot._seed_default_variables()
    for key, default_content in bank_consult_bot.CONSULT_PROMPT_DEFAULTS.items():
        bank_db.ensure_prompt(key, bank_consult_bot._category_label(key), default_content)

    agent_data = build_agent_panel()
    overview = build_overview(kafka_data, cache_data, search_data, db_data, kb_data, consult_data, agent_data)

    panels = {"kafka": kafka_data, "cache": cache_data, "search": search_data,
              "db": db_data, "recent_tx": recent_tx, "kb": kb_data,
              "consult": consult_data, "phoenix": {"url": phoenix_url},
              "kibana": {"url": kibana_base, "discover": kibana_discover, "views": kibana_views},
              "prompts": bank_db.get_all_prompts(), "variables": bank_db.get_all_variables(),
              "overview": overview, "agents": agent_data}
    return render_template("admin.html", active="admin", panels=panels, cache_kinds=CACHE_KINDS)


@app.route("/admin/prompts/save", methods=["POST"])
@admin_required
def admin_prompt_save():
    key = request.form.get("key")
    content = (request.form.get("content") or "").strip()
    if not key or not content:
        flash("프롬프트 내용을 입력하세요.", "err")
    else:
        bank_db.set_prompt(key, content)
        flash("프롬프트를 저장했어요.", "ok")
    return redirect(url_for("admin_dashboard") + "#prompts")


@app.route("/admin/prompts/variables/save", methods=["POST"])
@admin_required
def admin_variable_save():
    name = (request.form.get("name") or "").strip()
    value = (request.form.get("value") or "").strip()
    if not name:
        flash("변수 이름을 입력하세요.", "err")
    else:
        bank_db.set_variable(name, value)
        flash(f"변수 {{{{{name}}}}}를 저장했어요.", "ok")
    return redirect(url_for("admin_dashboard") + "#prompts")


@app.route("/admin/cache/flush", methods=["POST"])
@admin_required
def admin_cache_flush():
    try:
        n = cache.flush()
        flash(f"캐시 키 {n}개를 비웠어요.", "ok")
    except Exception as e:
        flash(f"캐시 비우기 실패: {e}", "err")
    return redirect(url_for("admin_dashboard") + "#cache")


@app.route("/admin/cache/flush/<kind>", methods=["POST"])
@admin_required
def admin_cache_flush_kind(kind):
    if kind not in dict(CACHE_KINDS):
        flash("알 수 없는 캐시 종류예요.", "err")
        return redirect(url_for("admin_dashboard") + "#cache")
    try:
        n = cache.flush_kind(kind)
        flash(f"'{dict(CACHE_KINDS)[kind]}' 캐시 키 {n}개를 비웠어요.", "ok")
    except Exception as e:
        flash(f"캐시 비우기 실패: {e}", "err")
    return redirect(url_for("admin_dashboard") + "#cache")


# --------------------------- 계좌 / 이체 ---------------------------
@app.route("/accounts")
@login_required
def accounts():
    uid = session["user"]["id"]
    accts = bank_db.get_accounts(uid)
    txns = bank_db.get_transactions(accts[0]["id"]) if accts else []
    for t in txns:
        t["counterpart"] = _mask_account_no(t["counterpart"])
    subs = bank_db.get_subscriptions(uid)
    deposits = [s for s in subs if s["ptype"] in DEPOSIT_TABS]
    loans = [s for s in subs if s["ptype"] in LOAN_TYPES]
    atab = request.args.get("atab", "입출금")
    if atab not in ("입출금", "예적금", "대출"):
        atab = "입출금"
    return render_template("accounts.html", active="accounts", atab=atab,
                           accounts=accts, txns=txns,
                           deposits=deposits, loans=loans)


@app.route("/mypage")
@login_required
def mypage():
    uid = session["user"]["id"]
    user = bank_db.get_user(uid)
    accts = bank_db.get_accounts(uid)
    subs = bank_db.get_subscriptions(uid)
    bal = sum(a["balance"] for a in accts)
    assets = bal + sum(s["principal"] for s in subs if s["ptype"] not in LOAN_TYPES)
    debts = sum(s["principal"] for s in subs if s["ptype"] in LOAN_TYPES)

    points = bank_db.get_points(uid)
    point_acct = bank_db.get_point_account(uid)
    point_balance = point_acct["balance"] if point_acct else 0

    return render_template("mypage.html", active="mypage", user=user, accounts=accts,
                           subscriptions=subs, total_assets=assets, total_debts=debts,
                           net=assets - debts, points=points, point_balance=point_balance,
                           point_rate=point_account_rate(point_balance),
                           next_tier=point_account_next_tier(point_balance))


@app.route("/transfer", methods=["GET", "POST"])
@login_required
def transfer():
    uid = session["user"]["id"]
    accts = bank_db.get_accounts(uid)

    def step1():
        return render_template("transfer.html", active="transfer", accounts=accts,
                               banks=BANKS, step=1)

    if request.method == "POST":
        action = request.form.get("action")
        bank = request.form.get("bank") or "FinPick"
        to_no = (request.form.get("to_account") or "").strip()
        from_account = request.form.get("from_account") or ""   # 출금 계좌 id(선택)
        # 선택된 출금 계좌(없으면 첫 계좌)
        src_acct = next((a for a in accts if str(a["id"]) == str(from_account)),
                        accts[0] if accts else None)

        # 1단계 → 받는분 확인(2단계)
        if action == "lookup":
            if not to_no:
                flash("받는 계좌번호를 입력하세요.", "err"); return step1()
            recipient = None
            if bank == "FinPick":
                recipient = bank_db.lookup_account_name(to_no)
                if not recipient:
                    flash("FinPick에서 해당 계좌를 찾을 수 없습니다.", "err"); return step1()
                if src_acct and to_no == src_acct["account_no"]:
                    flash("같은 계좌로는 이체할 수 없습니다.", "err"); return step1()
            return render_template("transfer.html", active="transfer", accounts=accts,
                                   banks=BANKS, step=2, bank=bank, to_account=to_no,
                                   recipient=recipient,
                                   from_account=(src_acct["id"] if src_acct else ""),
                                   from_account_no=(src_acct["account_no"] if src_acct else ""))

        # 2단계 → 실제 이체
        if action == "execute":
            memo = (request.form.get("memo") or "").strip()
            try:
                amount = int((request.form.get("amount") or "0").replace(",", ""))
            except ValueError:
                amount = 0
            src_id = src_acct["id"] if src_acct else None

            # 비동기(카프카 큐) 처리: 요청을 큐에 넣고 즉시 접수 응답, 실제 이체는 뒤에서 순차 처리
            if request.form.get("async_mode"):
                if amount <= 0:
                    flash("이체 금액을 확인하세요.", "err"); return step1()
                # 접속정보·이름은 요청(Flask 컨텍스트)에서 미리 담아둔다
                # (실제 이체는 컨텍스트 없는 consumer 스레드에서 처리되므로).
                req = {"id": str(uuid.uuid4()), "user_id": uid, "bank": bank,
                       "to_account": to_no, "amount": amount, "memo": memo,
                       "from_account_id": src_id, "ts_epoch": time.time(),
                       "user_name": session["user"].get("name"),
                       "client_ip": request.remote_addr,
                       "user_agent": request.headers.get("User-Agent")}
                if bank_kafka.publish_transfer_request(req):
                    flash("이체 요청이 접수되었습니다(카프카 큐). 잠시 후 거래내역에서 확인하세요.", "ok")
                    return redirect(url_for("accounts"))
                flash("이체 요청 접수 실패(카프카 연결 확인). 즉시 처리로 다시 시도하세요.", "err")
                return step1()

            if bank == "FinPick":
                ok, msg = bank_db.transfer(uid, to_no, amount, memo, from_account_id=src_id)
            else:
                ok, msg = bank_db.transfer_external(uid, bank, to_no, amount, memo, from_account_id=src_id)
            # 이체 후 출금계좌 잔액(감사용) + 받는분 이름(본행이면 조회)
            post = bank_db.get_accounts(uid)
            bal_after = next((a["balance"] for a in post
                              if src_acct and a["id"] == src_acct["id"]), None)
            recipient = bank_db.lookup_account_name(to_no) if bank == "FinPick" else None
            # 이체 결과를 카푸카에 이벤트로 발행 (성공/실패 모두 — 감사·실시간 알림용)
            bank_kafka.publish_transfer(
                user_id=uid, from_account=(src_acct["account_no"] if src_acct else None),
                to_bank=bank, to_account=to_no, amount=amount, ok=ok,
                kind="internal" if bank == "FinPick" else "external",
                message=msg, memo=memo,
                user_name=session["user"].get("name"), balance_after=bal_after,
                recipient_name=recipient, channel="web", async_mode=False,
                client_ip=request.remote_addr, user_agent=request.headers.get("User-Agent"))
            flash(msg, "ok" if ok else "err")
            if ok:
                return redirect(url_for("accounts"))
            return step1()

    return step1()


# --------------------------- API ---------------------------
@app.route("/api/ask", methods=["POST"])
def api_ask():
    if rag_core.client is None:
        return jsonify(error="UPSTAGE_API_KEY가 설정되지 않았습니다."), 400
    question = ((request.json or {}).get("question") or "").strip()
    if not question:
        return jsonify(error="질문을 입력하세요."), 400
    try:
        ans, cached = rag_answer(question)
        return jsonify(answer=ans, cached=cached)
    except Exception as e:
        return jsonify(error=f"오류: {e}"), 500


# --------------------------- AI 은행원 에이전트 (LangGraph) ---------------------------
@app.route("/api/agent", methods=["POST"])
@login_required
def api_agent():
    """고객 AI 은행원 에이전트. 대상 계좌는 항상 로그인 세션의 user_id로 고정(LLM이 못 바꿈)."""
    message = ((request.json or {}).get("message") or "").strip()
    if not message:
        return jsonify(error="메시지를 입력하세요."), 400
    uid = session["user"]["id"]
    t0 = time.time()
    try:
        answer = bank_agents.run_customer(uid, message)
        monitoring.record_agent_call("customer", True, (time.time() - t0) * 1000,
                                     route="customer", user_id=uid, message=message,
                                     answer_chars=len(answer or ""))
        return jsonify(answer=answer)
    except Exception as e:
        monitoring.record_agent_call("customer", False, (time.time() - t0) * 1000,
                                     route="customer", user_id=uid, message=message,
                                     error=str(e))
        print(f"[agent] 고객 에이전트 오류: {e}")
        return jsonify(error=f"에이전트 오류: {e}"), 500


@app.route("/admin/agent/<domain>", methods=["POST"])
@admin_required
def admin_agent(domain):
    """관리자 백오피스 에이전트. domain: fraud(이상거래탐지)/credit(여신심사)/compliance(컴플라이언스)."""
    if domain not in ("fraud", "credit", "compliance"):
        return jsonify(error="알 수 없는 도메인입니다."), 404
    message = ((request.json or {}).get("message") or "").strip()
    if not message:
        return jsonify(error="질의를 입력하세요."), 400
    admin_user_id = session["user"]["id"]
    t0 = time.time()
    try:
        answer = bank_agents.run_admin(domain, message, admin_user_id=admin_user_id)
        monitoring.record_agent_call(domain, True, (time.time() - t0) * 1000,
                                     route="admin", user_id=admin_user_id, message=message,
                                     answer_chars=len(answer or ""))
        return jsonify(answer=answer)
    except Exception as e:
        monitoring.record_agent_call(domain, False, (time.time() - t0) * 1000,
                                     route="admin", user_id=admin_user_id, message=message,
                                     error=str(e))
        print(f"[agent] 관리자 에이전트({domain}) 오류: {e}")
        return jsonify(error=f"에이전트 오류: {e}"), 500


@app.route("/learn")
def learn():
    """픽앤업 경제공부: 오늘의 학습 카드 → 그 카드로 만든 퀴즈 → 포인트.
    + 오늘의 주가 예측(모의 투자)도 여기서 이어서 도전할 수 있다."""
    bank_db.refresh_market(STOCK_PREDICT_REWARD)
    user = session.get("user")
    points = bank_db.get_points(user["id"]) if user else 0
    streak = bank_db.get_streak(user["id"]) if user else 0
    # 퀴즈는 정답(ans)을 화면에 내려주지 않는다 (채점은 서버에서)
    quiz_public = [{"q": q["q"], "opts": q["opts"]} for q in CARD_QUIZ]
    predict_stocks = bank_db.get_stocks()[:3]
    predicted_today = bank_db.get_predictions_today(user["id"]) if user else {}
    return render_template("learn.html", active="learn",
                           cards=LEARN_CARDS, quiz=quiz_public,
                           quiz_reward=QUIZ_REWARD, points=points, streak=streak,
                           predict_stocks=predict_stocks, predicted_today=predicted_today,
                           predict_reward=STOCK_PREDICT_REWARD)


@app.route("/learn/predict", methods=["POST"])
@login_required
def learn_predict():
    """주가 예측(상승/하락) 제출 — 다음 장마감 때 정산."""
    user = session["user"]
    data = request.get_json(silent=True) or {}
    code, guess = data.get("code"), data.get("guess")
    ok, msg = bank_db.submit_prediction(user["id"], code, guess)
    return jsonify(ok=ok, message=msg)


@app.route("/learn/reward", methods=["POST"])
@login_required
def learn_reward():
    """카드 기반 퀴즈 채점 + 포인트 적립 (AJAX). 정답이면 문제당 하루 1회 적립."""
    user = session["user"]
    data = request.get_json(silent=True) or {}
    if data.get("kind") != "quiz":
        return jsonify(ok=False, error="알 수 없는 요청입니다."), 400
    try:
        qid = int(data.get("qid"))
        choice = int(data.get("choice"))
    except (TypeError, ValueError):
        return jsonify(ok=False, error="잘못된 요청입니다."), 400
    if not (0 <= qid < len(CARD_QUIZ)):
        return jsonify(ok=False, error="없는 문제입니다."), 400
    q = CARD_QUIZ[qid]
    correct = (choice == q["ans"])
    if not correct:
        return jsonify(ok=True, correct=False, answer=q["ans"], exp=q["exp"],
                       awarded=0, points=bank_db.get_points(user["id"]),
                       message="아쉬워요. 해설을 보고 다시 도전해보세요.")
    awarded, bal, got = bank_db.award_once_per_day(
        user["id"], QUIZ_REWARD, f"quiz-{qid}", "학습 퀴즈 정답")
    msg = f"정답! +{got}P" if awarded else "정답! (오늘 이미 받은 문제예요)"
    return jsonify(ok=True, correct=True, answer=q["ans"], exp=q["exp"],
                   awarded=got, points=bal, message=msg)


@app.route("/invest")
def invest():
    """모의 투자: 코스피/코스닥/환율 + 종목 목록(하루 1회 장마감 갱신) + 보유 현황."""
    bank_db.refresh_market(STOCK_PREDICT_REWARD)
    user = session.get("user")
    uid = user["id"] if user else None
    return render_template("invest.html", active="invest",
                           market=bank_db.get_market_index(), stocks=bank_db.get_stocks(),
                           holdings=bank_db.get_holdings(uid) if uid else [],
                           predicted_today=bank_db.get_predictions_today(uid) if uid else {},
                           recent_predictions=bank_db.get_recent_predictions(uid) if uid else [],
                           points=bank_db.get_points(uid) if uid else 0,
                           point_to_won=POINT_TO_WON, predict_reward=STOCK_PREDICT_REWARD)


@app.route("/invest/buy", methods=["POST"])
@login_required
def invest_buy():
    uid = session["user"]["id"]
    code = request.form.get("code")
    try:
        points = int(request.form.get("points"))
    except (TypeError, ValueError):
        points = 0
    ok, msg, _ = bank_db.buy_stock(uid, code, points, POINT_TO_WON)
    flash(msg, "ok" if ok else "err")
    return redirect(url_for("invest"))


@app.route("/invest/predict", methods=["POST"])
@login_required
def invest_predict():
    uid = session["user"]["id"]
    code = request.form.get("code")
    guess = request.form.get("guess")
    ok, msg = bank_db.submit_prediction(uid, code, guess)
    flash(msg, "ok" if ok else "err")
    return redirect(url_for("invest"))


@app.route("/pickup/money")
@login_required
def pickup_money():
    """포인트 → 입출금 전환 전용 페이지. 전환 통장(우대금리) 개설/전환을 여기서."""
    uid = session["user"]["id"]
    points = bank_db.get_points(uid)
    acct = bank_db.get_point_account(uid)          # 전환 통장(없으면 None)
    bal = acct["balance"] if acct else 0
    return render_template("pickup_money.html", active="learn",
                           points=points, point_to_won=POINT_TO_WON,
                           pickup_account=acct, point_balance=bal,
                           point_rate=point_account_rate(bal),
                           next_tier=point_account_next_tier(bal))


@app.route("/pickup/account/open", methods=["POST"])
@login_required
def pickup_account_open():
    """픽앤업 전환 통장(입출금 통장) 개설 — 사용자가 직접."""
    uid = session["user"]["id"]
    _, created = bank_db.open_pickup_account(uid)
    flash("픽앤업 전환 통장이 개설되었어요." if created else "이미 전환 통장이 있어요.",
          "ok" if created else "err")
    return redirect(url_for("pickup_money"))


@app.route("/pickup/convert", methods=["POST"])
@login_required
def pickup_convert():
    """포인트 → 전환 통장에 현금 입금(이체처럼 폼 제출). 통장이 없으면 먼저 개설 유도."""
    uid = session["user"]["id"]
    if not bank_db.get_point_account(uid):
        flash("먼저 전환 통장을 개설하세요.", "err")
        return redirect(url_for("pickup_money"))
    try:
        points = int((request.form.get("points") or "0").replace(",", ""))
    except ValueError:
        points = 0
    ok, msg, _, _ = bank_db.convert_points(uid, points, POINT_TO_WON)
    flash(msg, "ok" if ok else "err")
    return redirect(url_for("pickup_money"))


# --------------------------- 실시간 상담채팅 (카푸카 + Socket.IO) ---------------------------
# 상담 UI 는 모든 화면 우하단의 챗봇 위젯(FAB, base.html)에서 제공한다.
# 아래는 그 위젯이 쓰는 실시간 파이프라인: Socket.IO 이벤트 + 카푸카 consumer + Solar 봇.

# 서버 시작 시각 이후 메시지에만 봇이 반응(재시작 시 replay 되는 옛 메시지 무시)
_consult_start_ts = time.time()
_consult_bot_busy = set()   # 봇이 답변 생성 중인 채널


def _on_consult_message(msg):
    """카푸카 consumer 콜백: 그 채널 사람들에게 실시간 전달 + 봇 답변 트리거."""
    socketio.emit("message", msg, room=msg.get("channel", "lobby"))
    _maybe_bot_reply(msg)


def _maybe_bot_reply(msg):
    if not bank_consult_bot.BOT_ENABLED:
        return
    if msg.get("user") == bank_consult_bot.BOT_NAME:   # 봇 자기 말엔 반응 안 함(무한루프 방지)
        return
    if msg.get("ts_epoch", 0) < _consult_start_ts:     # 재시작 replay 무시
        return
    channel = msg.get("channel", "lobby")
    if channel in _consult_bot_busy:
        return
    _consult_bot_busy.add(channel)

    def _work():
        try:
            reply = bank_consult_bot.generate_reply(bank_kafka.get_consult_history(channel))
            if not reply:
                return
            bot_msg = {
                "id": str(uuid.uuid4()), "channel": channel,
                "user": bank_consult_bot.BOT_NAME, "text": reply[:2000],
                "ts": _now_hm(), "ts_epoch": time.time(), "bot": True,
            }
            bank_kafka.publish_consult(bot_msg)   # 봇 답변도 카푸카를 거쳐 모두에게(일관성+보존)
        except Exception as e:
            print(f"[consult] 봇 답변 실패: {e}")
        finally:
            _consult_bot_busy.discard(channel)

    socketio.start_background_task(_work)


def _now_hm():
    return time.strftime("%H:%M")


@socketio.on("join")
def _consult_join(data):
    channel = (data or {}).get("channel") or "lobby"
    join_room(channel)
    # 입장자에게 지난 대화 먼저 전송
    socketio.emit("history", bank_kafka.get_consult_history(channel), room=request.sid)


@socketio.on("leave")
def _consult_leave(data):
    channel = (data or {}).get("channel") or "lobby"
    leave_room(channel)


@socketio.on("send")
def _consult_send(data):
    data = data or {}
    channel = (data.get("channel") or "lobby").strip() or "lobby"
    user = (data.get("user") or "손님").strip() or "손님"
    text = (data.get("text") or "").strip()
    if not text:
        return
    msg = {
        "id": str(uuid.uuid4()), "channel": channel, "user": user,
        "text": text[:2000], "ts": _now_hm(), "ts_epoch": time.time(),
    }
    # 카푸카에만 발행 → consumer 가 다시 읽어 방 전체에 전달(일관성)
    if not bank_kafka.publish_consult(msg):
        socketio.emit("error_message", "메시지 전송에 실패했어요(카푸카 연결 확인).", room=request.sid)


# --------------------------- 비동기 이체 처리(카푸카 큐 소비) ---------------------------
def _process_transfer_request(req):
    """이체 요청 큐에서 하나를 꺼내 실제 이체를 실행하고 결과 이벤트를 남긴다.
    (예외를 밖으로 던지지 않는다 — consumer가 안전하게 커밋할 수 있게)"""
    uid = req.get("user_id")
    bank = req.get("bank") or "FinPick"
    to_no = req.get("to_account")
    try:
        amount = int(req.get("amount") or 0)
    except (TypeError, ValueError):
        amount = 0
    memo = req.get("memo") or ""
    from_id = req.get("from_account_id")
    try:
        if bank == "FinPick":
            ok, msg = bank_db.transfer(uid, to_no, amount, memo, from_account_id=from_id)
        else:
            ok, msg = bank_db.transfer_external(uid, bank, to_no, amount, memo, from_account_id=from_id)
    except Exception as e:
        ok, msg = False, f"처리 오류: {e}"

    accts = bank_db.get_accounts(uid)
    src = next((a for a in accts if str(a["id"]) == str(from_id)), accts[0] if accts else None)
    recipient = bank_db.lookup_account_name(to_no) if bank == "FinPick" else None
    bank_kafka.publish_transfer(
        user_id=uid, from_account=src["account_no"] if src else None,
        to_bank=bank, to_account=to_no, amount=amount, ok=ok,
        kind=("internal-async" if bank == "FinPick" else "external-async"),
        message=msg, memo=memo,
        user_name=req.get("user_name"), balance_after=(src["balance"] if src else None),
        recipient_name=recipient, channel="web", async_mode=True,
        client_ip=req.get("client_ip"), user_agent=req.get("user_agent"))
    print(f"[transfer-async] uid={uid} → {to_no} {amount:,} : {'성공' if ok else '실패'} ({msg})")


# --------------------------- 이체 이벤트 → Elasticsearch 색인(키바나 조회용) ---------------------------
TRANSFER_INDEX = "bank-transfers"
_TRANSFER_MAPPING = {"properties": {
    "event_id": {"type": "keyword"}, "type": {"type": "keyword"},
    "kind": {"type": "keyword"}, "status": {"type": "keyword"}, "ok": {"type": "boolean"},
    "user_id": {"type": "long"}, "user_name": {"type": "keyword"},
    "from_bank": {"type": "keyword"}, "from_account": {"type": "keyword"},
    "to_bank": {"type": "keyword"}, "to_account": {"type": "keyword"},
    "to_account_masked": {"type": "keyword"}, "recipient_name": {"type": "keyword"},
    "is_external": {"type": "boolean"},
    "amount": {"type": "long"}, "fee": {"type": "long"},
    "amount_band": {"type": "keyword"}, "currency": {"type": "keyword"},
    "balance_after": {"type": "long"},
    "channel": {"type": "keyword"}, "async": {"type": "boolean"},
    "message": {"type": "text"}, "memo": {"type": "text"},
    "client_ip": {"type": "ip"}, "user_agent": {"type": "keyword", "ignore_above": 1024},
    "ts": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss||strict_date_optional_time||epoch_millis"},
    "date": {"type": "date", "format": "yyyy-MM-dd"},
    "hour": {"type": "integer"},
}}


def _ensure_transfer_index():
    if not es_online(probe_ttl=0):
        print(f"[es] '{TRANSFER_INDEX}' 인덱스 준비 건너뜀(ES 연결 불가)")
        return
    try:
        quick_es = es_quick()
        if not quick_es.indices.exists(index=TRANSFER_INDEX):
            quick_es.indices.create(index=TRANSFER_INDEX, mappings=_TRANSFER_MAPPING)
            print(f"[es] '{TRANSFER_INDEX}' 인덱스 생성")
    except Exception as e:
        print(f"[es] '{TRANSFER_INDEX}' 인덱스 준비 실패(색인만 건너뜀): {e}")


def _index_transfer_event(ev, doc_id):
    # client_ip가 빈 문자열이면 ES ip 타입 색인이 실패하므로 None 처리
    if ev.get("client_ip") == "":
        ev["client_ip"] = None
    es.index(index=TRANSFER_INDEX, id=doc_id, document=ev)


# AI 은행원 에이전트에 앱 헬퍼 주입(순환참조 방지) — 툴이 ES·금리·상품 정보를 쓸 수 있게.
bank_agents.configure(es=es, market_stats=market_stats, rate_ranking=rate_ranking,
                      base_rate=base_rate, finpick_products=FINPICK_PRODUCTS)


_background_services_started = False


def start_background_services():
    """실제 웹 서버 실행 시 Kafka/ES 백그라운드 작업을 한 번만 시작한다."""
    global _background_services_started
    if _background_services_started:
        return
    _background_services_started = True

    # 테스트에서 bank_web을 import할 때는 소비자 스레드가 뜨지 않도록 __main__에서만 호출한다.
    bank_kafka.start_consult_consumer(_on_consult_message)
    bank_kafka.start_transfer_request_consumer(_process_transfer_request)
    _ensure_transfer_index()
    bank_kafka.start_transfer_indexer(_index_transfer_event)


if __name__ == "__main__":
    start_background_services()
    socketio.run(app, port=5002, debug=False, allow_unsafe_werkzeug=True)
