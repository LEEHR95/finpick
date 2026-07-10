# -*- coding: utf-8 -*-
"""동시 이체 안전성 테스트: 잔액 초과 출금(오버드로우)이 발생하는지."""
import uuid
from concurrent.futures import ThreadPoolExecutor
import bank_db

bank_db.init_db()
sender = "c_" + uuid.uuid4().hex[:8]
recv = "c_" + uuid.uuid4().hex[:8]
bank_db.create_user(sender, "pw1234", "보내는이", opening_balance=100_000)  # 잔액 10만원
bank_db.create_user(recv, "pw1234", "받는이", opening_balance=0)
sid = bank_db.verify_user(sender, "pw1234")["id"]
rno = bank_db.get_accounts(bank_db.verify_user(recv, "pw1234")["id"])[0]["account_no"]

# 10만원 계좌에서 3만원씩 20번 동시 이체 시도 → 정상이라면 최대 3번(9만원)만 성공해야 함
def attempt(_):
    ok, _ = bank_db.transfer(sid, rno, 30_000)
    return ok

with ThreadPoolExecutor(max_workers=20) as ex:
    results = list(ex.map(attempt, range(20)))

success = sum(results)
sbal = bank_db.get_accounts(sid)[0]["balance"]
rbal = bank_db.get_accounts(bank_db.verify_user(recv, "pw1234")["id"])[0]["balance"]

print(f"동시 이체 시도 20건 중 성공: {success}건")
print(f"보내는 계좌 잔액: {sbal:,}원   받는 계좌 잔액: {rbal:,}원")
print(f"총액 보존(합=100,000): {'OK' if sbal + rbal == 100_000 else '깨짐!'}")
print(f"마이너스 잔액 없음: {'OK' if sbal >= 0 else '오버드로우 발생!'}")
print(f"성공건수×3만 = 받은금액 일치: {'OK' if success * 30_000 == rbal else '불일치!'}")
ok_overdraw = sbal >= 0 and sbal + rbal == 100_000 and success * 30_000 == rbal
print("\n결과:", "✅ 동시성 안전" if ok_overdraw else "❌ 동시성 결함 — 수정 필요")
