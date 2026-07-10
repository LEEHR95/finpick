"""
RAG 코어 엔진 — Upstage Solar Pro 2 (Flask 비의존, 순수 파이프라인)

은행 웹앱의 토대가 되는 RAG 기본 틀.
흐름: 파일 → 텍스트 추출(필요 시 OCR) → 쪼개기 → 임베딩(색인) → 검색 → 근거+질문을 LLM에 전달.

저장/검색은 index_chunks() / search() 두 함수로 분리되어 있다.
나중에 Elasticsearch로 바꾸려면 이 두 함수의 내부만 교체하면 된다(현재는 numpy 메모리 + store.pkl).

API 키는 코드에 적지 않는다. 환경변수 UPSTAGE_API_KEY 에서만 읽는다.
"""

import io
import os
import pickle
from functools import lru_cache

import httpx
import numpy as np
from openai import OpenAI
import fitz  # PyMuPDF — 한글 PDF 추출 품질이 좋음
from pypdf import PdfReader
from docx import Document

import bank_db  # 프롬프트 매니저(관리자 화면에서 시스템 프롬프트를 저장/수정)

# --------------------------- 설정 / 클라이언트 ---------------------------
API_KEY = os.environ.get("UPSTAGE_API_KEY")
client = OpenAI(api_key=API_KEY, base_url="https://api.upstage.ai/v1") if API_KEY else None

CHAT_MODEL = "solar-pro2"
EMBED_QUERY_MODEL = "embedding-query"
EMBED_PASSAGE_MODEL = "embedding-passage"

# 데이터 저장 폴더. 배포 시 볼륨 경로를 DATA_DIR로 지정하면 영구 보존.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)

DEFAULT_FOLDER = "기본"


# --------------------------- 저장소(STORE) ---------------------------
# 각 항목: {"source": 파일명, "text": 본문, "vec": 임베딩, "folder": 폴더, "page": 페이지}
# 디스크(store.pkl)에 저장해 서버를 껐다 켜도 유지된다.
STORE_PATH = os.path.join(DATA_DIR, "store.pkl")
STORE = []
STORE_VERSION = 0   # 문서가 바뀔 때마다 +1 → 검색용 정규화 행렬 캐시 무효화에 사용


def load_store():
    """디스크에 저장된 문서 색인을 불러온다."""
    global STORE, STORE_VERSION
    if os.path.exists(STORE_PATH):
        try:
            with open(STORE_PATH, "rb") as f:
                STORE = pickle.load(f)
        except Exception:
            STORE = []
    STORE_VERSION += 1


def save_store():
    """현재 문서 색인을 디스크에 저장한다."""
    global STORE_VERSION
    STORE_VERSION += 1   # 추가·삭제·비우기 모두 save_store()를 거치므로 여기서 캐시 무효화
    with open(STORE_PATH, "wb") as f:
        pickle.dump(STORE, f)
    try:
        sz = os.path.getsize(STORE_PATH)
    except OSError:
        sz = -1
    print(f"[save_store] path={STORE_PATH} items={len(STORE)} bytes={sz}", flush=True)


def chunk_folder(it):
    """예전에 폴더 없이 저장된 조각도 '기본' 폴더로 취급."""
    return it.get("folder") or DEFAULT_FOLDER


def chunks_in(folder, source=None):
    """folder/source로 범위 제한. source 주면 그 문서(파일) 조각만."""
    if not folder or folder == "__all__":
        pool = list(STORE)
    else:
        pool = [it for it in STORE if chunk_folder(it) == folder]
    if source:
        pool = [it for it in pool if it["source"] == source]
    return pool


# --------------------------- 파일 → 텍스트(페이지별) ---------------------------
def _pdf_text_segments(data):
    """PDF에서 텍스트 레이어 추출. PyMuPDF 우선, 비면 pypdf. [{page,text}]."""
    segs = []
    try:
        with fitz.open(stream=data, filetype="pdf") as doc:
            for i, page in enumerate(doc, start=1):
                t = page.get_text()
                if t and t.strip():
                    segs.append({"page": i, "text": t})
    except Exception:
        segs = []
    if not segs:
        try:
            reader = PdfReader(io.BytesIO(data))
            for i, page in enumerate(reader.pages, start=1):
                t = page.extract_text() or ""
                if t.strip():
                    segs.append({"page": i, "text": t})
        except Exception:
            segs = []
    return segs


def _looks_garbled(text):
    """폰트 깨짐(CID 매핑 실패 등)으로 글자가 깨졌는지 추정."""
    sample = text[:4000]
    if not sample.strip():
        return False
    bad = sum(1 for ch in sample
              if ord(ch) < 9 or (13 < ord(ch) < 32) or ch == "�"
              or 0xE000 <= ord(ch) <= 0xF8FF)
    return bad / len(sample) > 0.10


def ocr_pdf(filename, data):
    """Upstage Document Parse로 OCR(이미지·깨진 PDF). [{page,text}] 반환."""
    resp = httpx.post(
        "https://api.upstage.ai/v1/document-digitization",
        headers={"Authorization": f"Bearer {API_KEY}"},
        files={"document": (filename, data, "application/pdf")},
        data={"model": "document-parse", "output_formats": '["text"]',
              "ocr": "force", "coordinates": "false", "chart_recognition": "false"},
        timeout=110.0,
    )
    resp.raise_for_status()
    j = resp.json()
    by_page = {}
    for el in j.get("elements", []):
        pg = el.get("page", 1)
        txt = (el.get("content") or {}).get("text", "")
        if txt.strip():
            by_page.setdefault(pg, []).append(txt)
    segs = [{"page": pg, "text": "\n".join(parts)} for pg, parts in sorted(by_page.items())]
    if not segs:
        full = (j.get("content") or {}).get("text", "")
        if full.strip():
            segs = [{"page": None, "text": full}]
    return segs


def extract_segments(filename, data):
    """파일을 [{page, text}] 조각으로. PDF는 페이지별, 그 외는 page=None.
    PDF에서 글자가 없거나(이미지) 깨졌으면 자동으로 Upstage OCR로 재시도."""
    name = filename.lower()
    if name.endswith(".pdf"):
        segs = _pdf_text_segments(data)
        total = "".join(s["text"] for s in segs)
        # 텍스트가 없거나(스캔본) 심하게 깨졌으면 OCR
        if API_KEY and (not total.strip() or _looks_garbled(total)):
            try:
                ocr_segs = ocr_pdf(filename, data)
                if ocr_segs:
                    print(f"[ocr] {filename}: OCR로 {len(ocr_segs)}페이지 추출", flush=True)
                    return ocr_segs
            except Exception as e:
                print(f"[ocr] {filename} 실패: {e}", flush=True)
                if not total.strip():
                    raise ValueError(f"이미지 PDF인데 OCR에 실패했습니다: {e}")
        return segs
    if name.endswith(".docx"):
        doc = Document(io.BytesIO(data))
        return [{"page": None, "text": "\n".join(p.text for p in doc.paragraphs)}]
    if name.endswith((".txt", ".md", ".csv")):
        for enc in ("utf-8-sig", "cp949", "latin-1"):
            try:
                return [{"page": None, "text": data.decode(enc)}]
            except UnicodeDecodeError:
                continue
    # .doc(구버전 워드)는 미지원
    raise ValueError("지원하지 않는 형식입니다 (PDF, txt, docx 만 가능)")


def chunk_text(text, size=800, overlap=100):
    """긴 텍스트를 문단 기준으로 모으고 너무 길면 size 단위로 자른다."""
    paras = [p.strip() for p in text.split("\n") if p.strip()]
    chunks, buf = [], ""
    for p in paras:
        if len(buf) + len(p) + 1 <= size:
            buf = f"{buf}\n{p}" if buf else p
        else:
            if buf:
                chunks.append(buf)
            # 한 문단이 size보다 길면 통째로 잘라 담는다
            while len(p) > size:
                chunks.append(p[:size])
                p = p[size - overlap:]
            buf = p
    if buf:
        chunks.append(buf)
    return chunks


# --------------------------- 임베딩 ---------------------------
def embed(texts, model):
    """배치로 임베딩 (입력이 많아도 96개씩 나눠 호출)."""
    out = []
    for i in range(0, len(texts), 96):
        resp = client.embeddings.create(model=model, input=texts[i:i + 96])
        out.extend(np.array(d.embedding, dtype=np.float32) for d in resp.data)
    return out


@lru_cache(maxsize=256)
def embed_query(text):
    """질문 임베딩 캐시. 같은 문장(반복 질문)은 재호출 없이 재사용.
    반환 벡터는 읽기 전용으로만 사용할 것(캐시 공유 객체)."""
    resp = client.embeddings.create(model=EMBED_QUERY_MODEL, input=[text])
    return np.array(resp.data[0].embedding, dtype=np.float32)


# --------------------------- 색인 / 검색 (← ES 교체 지점) ---------------------------
def index_chunks(source, segments, folder=DEFAULT_FOLDER, keywords=None):
    """페이지별 조각을 잘게 나눠 임베딩해 STORE에 추가. 추가된 조각 수 반환.

    keywords: 이 문서(출처)와 관련된 짧은 키워드 목록(예: 챗봇 UI의 '관련 키워드칩'용).
              검색 자체에는 쓰이지 않고 메타데이터로만 저장한다.

    ※ Elasticsearch로 교체 시: embed()로 만든 벡터(또는 원문)를 ES 색인에 넣도록 이 함수 내부만 바꾸면 된다.
    """
    folder = (folder or DEFAULT_FOLDER).strip() or DEFAULT_FOLDER
    keywords = list(keywords) if keywords else []
    pieces = []  # (text, page)
    for seg in segments:
        for c in chunk_text(seg["text"]):
            pieces.append((c, seg.get("page")))
    if not pieces:
        return 0
    vecs = embed([c for c, _ in pieces], EMBED_PASSAGE_MODEL)
    for (c, page), v in zip(pieces, vecs):
        STORE.append({"source": source, "text": c, "vec": v, "folder": folder, "page": page,
                      "keywords": keywords})
    save_store()
    return len(pieces)


def kb_stats():
    """관리자 대시보드용 지식베이스(색인 문서) 현황 — 폴더별 조각 수 + 출처 수."""
    folders = {}
    for it in STORE:
        f = chunk_folder(it)
        folders.setdefault(f, {"chunks": 0, "sources": set()})
        folders[f]["chunks"] += 1
        folders[f]["sources"].add(it["source"])
    return {
        "total_chunks": len(STORE),
        "folders": [{"folder": f, "chunks": v["chunks"], "sources": len(v["sources"])}
                    for f, v in folders.items()],
    }


_NORM_CACHE = {"ver": None, "mat": None}


def normalized_store_matrix():
    """STORE 전체 벡터를 미리 정규화한 행렬을 캐시해 반환.
    문서가 바뀌면(STORE_VERSION 변동) 한 번만 다시 계산 → 검색 때마다 재정규화 안 함."""
    if _NORM_CACHE["ver"] != STORE_VERSION:
        if STORE:
            mat = np.stack([it["vec"] for it in STORE])         # (전체, D)
            mat = mat / (np.linalg.norm(mat, axis=1, keepdims=True) + 1e-9)
        else:
            mat = None
        _NORM_CACHE["mat"], _NORM_CACHE["ver"] = mat, STORE_VERSION
    return _NORM_CACHE["mat"]


def search(question, folder=None, top_k=4):
    """질문과 가장 비슷한 문서 조각을 찾아 (참고문맥, 출처목록) 반환. folder로 범위 제한.

    ※ Elasticsearch로 교체 시: 아래 코사인 유사도 계산 대신 ES 검색 질의를 호출하고,
       결과로 같은 형태의 (context, sources)를 만들어 반환하면 된다.
    """
    if not folder or folder == "__all__":
        idx = list(range(len(STORE)))
    else:
        idx = [i for i, it in enumerate(STORE) if chunk_folder(it) == folder]
    if not idx:
        raise ValueError("이 폴더에 문서가 없습니다. 먼저 문서를 추가하세요.")
    q_vec = embed_query(question)
    mat_n = normalized_store_matrix()[idx]                  # (N, D) 캐시에서 폴더 범위만 선택
    q_n = q_vec / (np.linalg.norm(q_vec) + 1e-9)
    sims = mat_n @ q_n                                       # (N,) 코사인 유사도
    order = np.argsort(-sims)[:top_k]
    top = [STORE[idx[i]] for i in order]

    def tag(it):
        p = it.get("page")
        return f"[{it['source']}" + (f" p.{p}" if p else "") + "]"

    context = "\n\n".join(f"{tag(it)} {it['text']}" for it in top)
    sources = [{"source": it["source"], "folder": chunk_folder(it),
                "page": it.get("page"), "text": it["text"][:160]} for it in top]
    return context, sources


# --------------------------- 답변 생성 ---------------------------
# 아래는 최초 시드용 fallback일 뿐, 실제 프롬프트는 관리자 화면(프롬프트 매니저)에서
# 언제든 수정할 수 있고 수정 즉시(서버 재시작 없이) 다음 답변부터 반영된다.
SYSTEM_PROMPT_DEFAULT = (
    "아래 '참고 문서'에 있는 내용만 근거로 한국어로 답하라. "
    "문서에 없으면 모른다고 솔직히 답하라. 이전 대화 맥락을 참고해 "
    "후속 질문에도 자연스럽게 답하라. "
    "마크다운 기호(**, ##, -, *, `, > 등)를 절대 쓰지 말고, "
    "깔끔한 평문(일반 문장)으로만 답하라. 항목 구분이 필요하면 "
    "줄바꿈과 '1) 2)' 또는 '·' 정도만 사용하라."
)

def build_messages(question, context, history):
    """시스템 + 직전 대화(history) + 현재 질문(참고문서 포함)으로 메시지 구성."""
    # bank_db.init_db() 실행 이후(요청 시점)에만 호출되므로, 최초 호출 때 prompts 테이블에
    # 기본값을 안전하게 시드하고 이후로는 관리자가 저장한 값을 그대로 읽어온다.
    system_prompt = bank_db.render_prompt(bank_db.ensure_prompt(
        "rag_system", "RAG 챗봇(상품검색 /api/ask) 시스템 프롬프트", SYSTEM_PROMPT_DEFAULT))
    msgs = [{"role": "system", "content": system_prompt}]
    for h in (history or [])[-6:]:  # 최근 3턴(6메시지)만 기억
        role = h.get("role")
        content = (h.get("content") or "").strip()
        if role in ("user", "assistant") and content:
            msgs.append({"role": role, "content": content})
    msgs.append({"role": "user", "content": f"참고 문서:\n{context}\n\n질문: {question}"})
    return msgs


# 모듈 로드 시 디스크 색인 복구
load_store()
