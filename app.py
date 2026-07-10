"""
RAG 백엔드 서버 (얇은 Flask 래퍼) — 은행 웹앱 토대.

rag_core 의 RAG 엔진을 HTTP API로 노출만 한다. UI는 별도 설계.

라우트:
  POST /upload        파일 업로드(PDF/txt/docx) → 색인
  GET  /files         폴더/파일 목록
  GET  /file-content  문서 1개의 본문 조각(뷰어용)
  POST /reset         폴더(또는 전체) 색인 삭제
  POST /delete-file   특정 파일 색인 삭제
  POST /ask           질문 → 근거 검색 + LLM 답변(스트리밍)

실행:
  pip install -r requirements.txt
  python app.py        → http://127.0.0.1:5001

API 키는 코드에 적지 않는다. 환경변수 UPSTAGE_API_KEY 에서만 읽는다.
공개 배포 시 환경변수 APP_PASSWORD 를 설정하면 Basic 인증으로 잠긴다.
"""

import os
import json

from flask import Flask, request, jsonify, Response

import rag_core
from rag_core import (
    client, DEFAULT_FOLDER, STORE,
    extract_segments, index_chunks, search, build_messages,
    chunk_folder, save_store, CHAT_MODEL,
)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024  # 파일 최대 30MB

# 공개 배포 시 접속 비밀번호. 환경변수 APP_PASSWORD 설정하면 로그인 요구.
# 로컬(미설정)에선 잠금 없이 바로 사용.
APP_PASSWORD = os.environ.get("APP_PASSWORD")


@app.before_request
def require_login():
    if not APP_PASSWORD:
        return  # 비밀번호 미설정 → 통과 (로컬용)
    auth = request.authorization
    if auth and auth.password == APP_PASSWORD:
        return
    return Response("로그인이 필요합니다.", 401,
                    {"WWW-Authenticate": 'Basic realm="bank-rag-login"'})


@app.route("/upload", methods=["POST"])
def upload():
    if client is None:
        return jsonify(error="API 키가 없습니다. UPSTAGE_API_KEY 환경변수를 설정하세요."), 400
    files = request.files.getlist("files")
    folder = (request.form.get("folder") or DEFAULT_FOLDER).strip() or DEFAULT_FOLDER
    if not files:
        return jsonify(error="파일이 없습니다."), 400
    results = []
    for f in files:
        try:
            segs = extract_segments(f.filename, f.read())
            n = index_chunks(f.filename, segs, folder)
            results.append({"name": f.filename, "chunks": n})
        except Exception as e:
            import traceback
            traceback.print_exc()
            results.append({"name": f.filename, "error": str(e)})
    print(f"[upload] folder={folder} results={results} store_total={len(STORE)}", flush=True)
    return jsonify(results=results, folder=folder)


@app.route("/files")
def files_list():
    """폴더별 파일 목록. {folders:[{name,count}], files:[{name,folder,chunks}]}"""
    folder_counts, file_counts = {}, {}
    for it in STORE:
        fd = chunk_folder(it)
        folder_counts[fd] = folder_counts.get(fd, 0) + 1
        key = (fd, it["source"])
        file_counts[key] = file_counts.get(key, 0) + 1
    folders = [{"name": k, "count": v} for k, v in sorted(folder_counts.items())]
    files = [{"folder": fd, "name": nm, "chunks": c}
             for (fd, nm), c in file_counts.items()]
    return jsonify(folders=folders, files=files)


@app.route("/file-content")
def file_content():
    """문서 1개의 저장된 본문 조각을 원본 순서대로 반환 (뷰어용)."""
    folder = request.args.get("folder")
    name = request.args.get("name")
    if not name:
        return jsonify(error="파일명이 필요합니다"), 400
    items = [it for it in STORE
             if it["source"] == name and (not folder or chunk_folder(it) == folder)]
    if not items:
        return jsonify(error="문서를 찾을 수 없습니다"), 404
    pieces = [{"page": it.get("page"), "text": it["text"]} for it in items]
    return jsonify(name=name, folder=folder, chunks=len(pieces), pieces=pieces)


@app.route("/reset", methods=["POST"])
def reset():
    """folder 지정 시 그 폴더만, 없으면 전체 삭제."""
    folder = (request.json or {}).get("folder")
    if folder and folder != "__all__":
        STORE[:] = [it for it in STORE if chunk_folder(it) != folder]
    else:
        STORE.clear()
    save_store()
    return jsonify(ok=True)


@app.route("/delete-file", methods=["POST"])
def delete_file():
    """폴더 안의 특정 파일만 삭제."""
    body = request.json or {}
    folder, name = body.get("folder"), body.get("name")
    if not name:
        return jsonify(error="파일명이 없습니다."), 400
    before = len(STORE)
    STORE[:] = [it for it in STORE
                if not (it["source"] == name and
                        (folder in (None, "__all__") or chunk_folder(it) == folder))]
    save_store()
    return jsonify(ok=True, removed=before - len(STORE))


@app.route("/ask", methods=["POST"])
def ask():
    if client is None:
        return jsonify(error="API 키가 없습니다. UPSTAGE_API_KEY 환경변수를 설정하세요."), 400
    body = request.json or {}
    question = body.get("question", "").strip()
    history = body.get("history", [])
    folder = body.get("folder")
    if not question:
        return jsonify(error="질문을 입력하세요."), 400

    # 검색은 스트리밍 전에 끝내서, 실패하면 일반 에러로 응답
    try:
        context, sources = search(question, folder)
    except Exception as e:
        return jsonify(error=str(e)), 400

    messages = build_messages(question, context, history)

    def generate():
        # 1) 첫 줄: 출처 정보(JSON) → 그 뒤로는 답변 토큰이 흘러나온다
        yield json.dumps({"sources": sources}, ensure_ascii=False) + "\n"
        try:
            stream = client.chat.completions.create(
                model=CHAT_MODEL, messages=messages, temperature=0.2, stream=True,
            )
            for chunk in stream:
                delta = chunk.choices[0].delta.content
                if delta:
                    yield delta
        except Exception as e:
            yield f"\n[오류: {e}]"

    return Response(generate(), mimetype="text/plain; charset=utf-8")


if __name__ == "__main__":
    app.run(port=5001, debug=False, threaded=True)
