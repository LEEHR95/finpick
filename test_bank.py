# -*- coding: utf-8 -*-
"""은행 웹앱 전체 흐름 검증.

반복 실행해도 기존 DB 상태에 흔들리지 않도록 매번 새 테스트 계정을 만든다.
"""
import uuid

import bank_web
import bank_db

app = bank_web.app
app.config["TESTING"] = True

PASSWORD = "pw1234"


def check(checks, label, ok):
    checks.append((label, bool(ok)))
    print(f"{label}: {bool(ok)}")


def main():
    sender = f"tester_{uuid.uuid4().hex[:8]}"
    receiver = f"receiver_{uuid.uuid4().hex[:8]}"
    checks = []

    # 받는 사람 미리 생성
    bank_db.create_user(receiver, PASSWORD, "받는이")
    receiver_user = bank_db.verify_user(receiver, PASSWORD)
    to_no = bank_db.get_accounts(receiver_user["id"])[0]["account_no"]
    print("받는 계좌:", to_no)

    c = app.test_client()

    # 1) 메인
    r = c.get("/")
    check(checks, "\n[메인] status 200", r.status_code == 200)
    check(checks, "[메인] 기준금리 표기", "기준금리" in r.get_data(as_text=True))

    # 2) 회원가입
    r = c.post("/register", data={"username": sender, "password": PASSWORD, "name": "테스터"},
               follow_redirects=True)
    check(checks, "[회원가입] status 200", r.status_code == 200)

    # 3) 로그인
    r = c.post("/login", data={"username": sender, "password": PASSWORD}, follow_redirects=True)
    check(checks, "[로그인] status 200", r.status_code == 200)

    # 4) 계좌조회
    r = c.get("/accounts")
    body = r.get_data(as_text=True)
    sender_user = bank_db.verify_user(sender, PASSWORD)
    sender_account = bank_db.get_accounts(sender_user["id"])[0]
    check(checks, "[계좌조회] status 200", r.status_code == 200)
    check(checks, "[계좌조회] 신규 잔액 1,000,000", sender_account["balance"] == 1_000_000)
    check(checks, "[계좌조회] 화면 잔액 표기", "1,000,000" in body)

    # 5) 이체: 받는분 확인(lookup) 후 실행(execute)
    r = c.post("/transfer", data={"action": "lookup", "bank": "FinPick", "to_account": to_no})
    lookup_body = r.get_data(as_text=True)
    check(checks, "[이체확인] status 200", r.status_code == 200)
    check(checks, "[이체확인] 금액 입력칸 표시", 'name="amount"' in lookup_body)

    r = c.post("/transfer", data={"action": "execute", "bank": "FinPick",
                                  "to_account": to_no, "amount": "150000", "memo": "테스트"},
               follow_redirects=True)
    body = r.get_data(as_text=True)
    sender_balance = bank_db.get_accounts(sender_user["id"])[0]["balance"]
    receiver_balance = bank_db.get_accounts(receiver_user["id"])[0]["balance"]
    check(checks, "[이체] status 200", r.status_code == 200)
    check(checks, "[이체] 송금자 잔액 850,000", sender_balance == 850_000)
    check(checks, "[이체] 수취인 잔액 1,150,000", receiver_balance == 1_150_000)
    check(checks, "[이체] 화면 잔액 표기", "850,000" in body)

    # 6) 상품 의미검색
    r = c.get("/products?q=" + "이자 높은 적금")
    check(checks, "[상품검색] status 200", r.status_code == 200)

    # 7) RAG 챗봇: 외부 API가 막힌 환경에서는 500이어도 JSON 오류 응답이면 앱은 살아있다.
    r = c.post("/api/ask", json={"question": "안전하게 목돈 굴리기 좋은 예금"})
    j = r.get_json()
    answer = (j.get("answer") or j.get("error") or "") if j else ""
    check(checks, "[챗봇] 구조화 응답 있음", bool(answer))
    print("[챗봇] status", r.status_code, "| 답변 앞부분:", answer[:60])

    failed = [label for label, ok in checks if not ok]
    if failed:
        raise SystemExit("실패한 검증: " + ", ".join(failed))


if __name__ == "__main__":
    main()
