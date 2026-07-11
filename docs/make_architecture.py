# -*- coding: utf-8 -*-
"""FinPick 시스템 아키텍처 SVG 생성기.

하루한끼 포폴(docs/architecture.svg) 스타일을 참고 — 가로 계층 밴드 + 좌측 라벨 +
흰색 칩 + 세로 화살표 + 우측 크로스컷 컬럼 + 상단 핵심원칙 pill.
내용은 실제 코드 기준. 출력: docs/architecture.svg
"""
import os

FONT = "'Segoe UI','Malgun Gothic','Apple SD Gothic Neo',sans-serif"
W = 1500
MX, MW, PAD = 200, 1000, 24
CX0, CWT = MX + PAD, MW - 2 * PAD          # 칩 영역 좌측·전체폭
LABEL_X = 104
CENTER = MX + MW / 2                        # 700
S = []


def esc(t):
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def rect(x, y, w, h, fill, rx=9, stroke="none", sw=0, op=1, dash=None):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    S.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
             f'stroke="{stroke}" stroke-width="{sw}" opacity="{op}"{d}/>')


def text(x, y, t, size, fill, weight=500, anchor="middle"):
    S.append(f'<text x="{x}" y="{y}" font-size="{size}" fill="{fill}" font-weight="{weight}" '
             f'text-anchor="{anchor}" font-family="{FONT}" dominant-baseline="middle">{esc(t)}</text>')


def arrow(cx, y0, y1, color="#8a93a3", dash=None):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    S.append(f'<line x1="{cx}" y1="{y0}" x2="{cx}" y2="{y1}" stroke="{color}" stroke-width="2.4"{d}/>')
    S.append(f'<path d="M{cx-6},{y1-1} L{cx+6},{y1-1} L{cx},{y1+8} Z" fill="{color}"/>')


def chips_row(y, chips, h):
    """chips: [(title, sub|None), ...] 을 밴드 폭에 균등 배치."""
    n = len(chips)
    gap = 14
    cw = (CWT - gap * (n - 1)) / n
    for i, (title, sub) in enumerate(chips):
        x = CX0 + i * (cw + gap)
        rect(x, y, cw, h, "#ffffff", rx=10, stroke="#d7dbe3", sw=1.4)
        cx = x + cw / 2
        if sub:
            text(cx, y + h / 2 - 8, title, 13.5, "#2b2f38", 600)
            text(cx, y + h / 2 + 10, sub, 10.5, "#7b8290", 400)
        else:
            text(cx, y + h / 2, title, 13.5, "#2b2f38", 500)


# ── 레이어 정의 (위 → 아래) ── 모두 실제 코드 기준
LAYERS = [
    dict(label="Client", sub="Flask Templates · Socket.IO", color="#8B7FD9",
         rows=[[("웹 화면 (Jinja)", None), ("AI 은행원 챗봇 (FAB)", None),
                ("모의투자 · 픽앤업", None), ("관리자 콘솔", None)]], rh=44),
    dict(label="Web · API", sub="bank_web.py · Flask + Socket.IO", color="#EBA96A",
         rows=[[("/accounts · /transfer", None), ("/products · /rates", None),
                ("/invest · /learn", None), ("/api/agent", None),
                ("/admin", None), ("Socket.IO 상담", None)]], rh=42),
    dict(label="AI · Agent", sub="bank_agents.py · LangGraph", color="#46C0BC",
         rows=[[("고객 에이전트 (계좌·상담)", "get·prepare·confirm"),
                ("이상거래탐지", "ES 이체로그 집계"),
                ("여신심사", "규칙 스코어링"),
                ("컴플라이언스", "AML 스크리닝")],
               [("bank_consult_bot", "상담 챗봇"),
                ("rag_core", "RAG 검색·프롬프트"),
                ("프롬프트 매니저", "도메인별 프롬프트·변수")]],
         rh=56, note="판단은 AI · 실행 결정권은 사용자  —  prepare → 확인 → confirm",
         note_color="#0d6b64"),
    dict(label="External AI", sub="Upstage", color="#8FC97A",
         rows=[[("Upstage Solar Pro 2", "채팅 · 임베딩"),
                ("Upstage Document Parse", "OCR")]], rh=48),
    dict(label="Messaging", sub="Kafka · localhost:9092", color="#E79A80",
         rows=[[("bank-transfers", "이체 이벤트(28필드)"),
                ("bank-transfer-requests", "비동기 이체 큐"),
                ("bank-consult", "상담 메시지"),
                ("이체 색인 컨슈머", "Kafka → ES")]], rh=56),
    dict(label="Data · Search", sub="Storage · Search · Cache", color="#6E86D6",
         rows=[[("SQLite", "계좌·거래·포인트"),
                ("Elasticsearch", "상품 1,384건 · 이체로그"),
                ("Redis", "TTL 캐시 (195× ↑)"),
                ("store.pkl", "상담 KB 임베딩")]], rh=58),
    dict(label="Batch 적재", sub="Data Ingestion (1회)", color="#C9B98F",
         rows=[[("fetch_fss.py", "금감원 공시상품"),
                ("fetch_bok.py", "경제지표"),
                ("index_to_es.py", "ES 색인"),
                ("seed_consult_kb.py", "상담 KB 시드")]], rh=56),
    dict(label="Infra · Ops", sub="Docker · Runtime", color="#A7C6EE",
         rows=[[("Docker", "컨테이너 5종"),
                ("Kibana", "이상거래 모니터링"),
                ("Phoenix", "LLM 호출 추적"),
                ("monitoring.py", "관리자 대시보드")]], rh=48),
]

# ── 밴드 배치 계산 ──
TOP = 116
BAND_PAD_T, ROW_GAP, NOTE_H, BAND_PAD_B = 22, 10, 22, 16
ARROW = 27
y = TOP
band_geom = []
for L in LAYERS:
    rows_h = sum(L["rh"] for _ in L["rows"]) + ROW_GAP * (len(L["rows"]) - 1)
    note_h = NOTE_H if L.get("note") else 0
    bh = BAND_PAD_T + rows_h + note_h + BAND_PAD_B
    band_geom.append((y, bh))
    y += bh + ARROW
CANVAS_H = y - ARROW + BAND_PAD_B + 40

# ── 배경 / 카드 / 헤더 ──
S.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {CANVAS_H}" width="{W}" height="{CANVAS_H}">')
rect(0, 0, W, CANVAS_H, "#eef3fb", rx=0)
rect(16, 16, W - 32, CANVAS_H - 32, "#ffffff", rx=24, stroke="#dfe6f2", sw=2)
text(60, 60, "FinPick", 30, "#2b2f38", 800, anchor="start")
text(60, 90, "대화형 AI 은행 서비스 — System Architecture", 15, "#8891a0", 400, anchor="start")
# 핵심 원칙 pill (상단 우측)
rect(1035, 44, 400, 44, "#f3f0ff", rx=12, stroke="#d9d0f5", sw=1.4)
text(1235, 66, "핵심 원칙 :  판단은 AI  ·  실행 결정권은 사용자", 14.5, "#6b57c9", 700)

# ── 우측 크로스컷 컬럼 (인증·세션 / 관측·운영) ──
def side_col(x, title, color, items, top, bot):
    rect(x, top, 122, bot - top, color, rx=16)
    cx = x + 61
    text(cx, top + 26, title, 15.5, "#ffffff", 800)
    ih, gap = 62, 24
    yy = top + 56
    for a, b in items:
        rect(x + 12, yy, 98, ih, "#ffffff", rx=10, op=0.94)
        text(cx, yy + 22, a, 12.5, "#3a3f4a", 600)
        text(cx, yy + 40, b, 10, "#adb1ba", 500)
        yy += ih + gap

col_top = band_geom[0][0]
col_bot = band_geom[3][0] + band_geom[3][1]          # Client~External 높이에 맞춤
side_col(1220, "인증 · 세션", "#9CC77F",
         [("Flask", "session"), ("login_required", "본인 스코프"),
          ("admin_required", "role=admin")], col_top, col_bot)
side_col(1352, "관측 · 운영", "#E79AA0",
         [("Phoenix", "LLM 추적"), ("Kibana", "이체 모니터링"),
          ("프롬프트", "무중단 편집")], col_top, col_bot)

# ── 밴드 그리기 ──
for L, (by, bh) in zip(LAYERS, band_geom):
    dashed = L.get("dashed")
    rect(MX, by, MW, bh, L["color"], rx=16)
    if dashed:
        rect(MX, by, MW, bh, "none", rx=16, stroke=dashed, sw=2.4, dash="9 6")
    text(LABEL_X, by + bh / 2 - 9, L["label"], 17, "#3a3f4a", 700)
    text(LABEL_X, by + bh / 2 + 12, L["sub"], 11.5, "#9aa0ac", 400)
    ry = by + BAND_PAD_T
    for row in L["rows"]:
        chips_row(ry, row, L["rh"])
        ry += L["rh"] + ROW_GAP
    if L.get("note"):
        text(CENTER, ry - ROW_GAP + 14, L["note"], 12.5, L.get("note_color", "#3a3f4a"), 600)

# ── 세로 화살표 (밴드 사이) ──
for i in range(len(band_geom) - 1):
    y0 = band_geom[i][0] + band_geom[i][1]
    dash = "6 5" if LAYERS[i]["label"] == "External AI" else None
    col = "#c98b7f" if dash else "#8a93a3"
    arrow(CENTER, y0 + 1, y0 + ARROW - 8, col, dash)

S.append("</svg>")

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "architecture.svg")
open(OUT, "w", encoding="utf-8").write("\n".join(S))
print("saved:", OUT, "| canvas", W, "x", CANVAS_H)
