# -*- coding: utf-8 -*-
"""2단계 이체 흐름 검증 (은행선택 → 받는분 확인 → 이체)."""
import uuid
import bank_web, bank_db

app = bank_web.app; app.config["TESTING"] = True
c = app.test_client()

# 임시 송금인 + 수취인 생성
u = "x_" + uuid.uuid4().hex[:8]
bank_db.create_user(u, "pw1234", "보내는사람")
r = "x_" + uuid.uuid4().hex[:8]
bank_db.create_user(r, "pw1234", "받는사람")
r_no = bank_db.get_accounts(bank_db.verify_user(r, "pw1234")["id"])[0]["account_no"]

c.post("/login", data={"username": u, "password": "pw1234"})

print("=== 1단계: 은행 선택 화면 ===")
s1 = c.get("/transfer").get_data(as_text=True)
print("  은행 드롭다운(select) 존재:", "<select name=\"bank\"" in s1)
print("  FinPick(본행) 옵션:", "(본행)" in s1)

print("\n=== 본행 이체: 2단계 받는분 확인 ===")
s2 = c.post("/transfer", data={"action": "lookup", "bank": "FinPick", "to_account": r_no}).get_data(as_text=True)
print("  받는분 이름 표시(받는사람님):", "받는사람님" in s2)
print("  금액 입력칸 등장:", "name=\"amount\"" in s2)

print("\n=== 본행 이체 실행 ===")
res = c.post("/transfer", data={"action": "execute", "bank": "FinPick", "to_account": r_no,
                                "amount": "30000", "memo": "확인테스트"}, follow_redirects=True)
bal_u = bank_db.get_accounts(bank_db.verify_user(u, "pw1234")["id"])[0]["balance"]
bal_r = bank_db.get_accounts(bank_db.verify_user(r, "pw1234")["id"])[0]["balance"]
print(f"  송금인 잔액: {bal_u:,} (1,000,000-30,000=970,000 기대)")
print(f"  수취인 잔액: {bal_r:,} (1,000,000+30,000=1,030,000 기대)")

print("\n=== 없는 본행 계좌 → 확인 단계에서 차단 ===")
s = c.post("/transfer", data={"action": "lookup", "bank": "FinPick", "to_account": "100-999-999999"},
           follow_redirects=True).get_data(as_text=True)
print("  '찾을 수 없습니다' 안내:", "찾을 수 없" in s)

print("\n=== 타행(국민은행) 이체: 데모 처리 ===")
s = c.post("/transfer", data={"action": "lookup", "bank": "국민은행", "to_account": "123-45-678901"}).get_data(as_text=True)
print("  예금주 '확인 불가(데모)' 표시:", "확인 불가" in s)
c.post("/transfer", data={"action": "execute", "bank": "국민은행", "to_account": "123-45-678901",
                          "amount": "20000", "memo": "타행"}, follow_redirects=True)
bal_u2 = bank_db.get_accounts(bank_db.verify_user(u, "pw1234")["id"])[0]["balance"]
print(f"  송금인 잔액: {bal_u2:,} (970,000-20,000=950,000 기대, 출금만)")
