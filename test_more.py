# -*- coding: utf-8 -*-
"""추가 검증: 이체/인증 엣지케이스, 캐시 정합성, 데이터 무결성, 챗봇 견고성."""
import uuid
import bank_db
import bank_web
import cache
import search_es
from elasticsearch import Elasticsearch

if __name__ != "__main__":
    import pytest
    pytest.skip("script-style integration audit; run directly with python test_more.py", allow_module_level=True)

es = Elasticsearch("http://localhost:9200", request_timeout=60)
app = bank_web.app
app.config["TESTING"] = True

PASS, FAIL = 0, 0
def check(name, cond):
    global PASS, FAIL
    print(("  ✅" if cond else "  ❌") + " " + name)
    if cond: PASS += 1
    else: FAIL += 1


print("=" * 60)
print("A. 이체 엣지케이스")
bank_db.init_db()
u1 = "t_" + uuid.uuid4().hex[:8]
u2 = "t_" + uuid.uuid4().hex[:8]
bank_db.create_user(u1, "pw1234", "앨리스")
bank_db.create_user(u2, "pw1234", "밥")
id1 = bank_db.verify_user(u1, "pw1234")["id"]
id2 = bank_db.verify_user(u2, "pw1234")["id"]
no1 = bank_db.get_accounts(id1)[0]["account_no"]
no2 = bank_db.get_accounts(id2)[0]["account_no"]

ok, _ = bank_db.transfer(id1, no2, 200000); check("정상 이체 성공", ok)
ok, msg = bank_db.transfer(id1, no2, 99_999_999); check("잔액부족 거부", not ok and "부족" in msg)
ok, msg = bank_db.transfer(id1, no1, 1000); check("본인계좌 거부", not ok)
ok, msg = bank_db.transfer(id1, "100-000-000000", 1000); check("없는계좌 거부", not ok)
ok, msg = bank_db.transfer(id1, no2, 0); check("0원 거부", not ok)
ok, msg = bank_db.transfer(id1, no2, -5000); check("음수 거부", not ok)
# 보존 법칙: 두 계좌 잔액 합이 200만(초기 100만*2) 유지
bal1 = bank_db.get_accounts(id1)[0]["balance"]
bal2 = bank_db.get_accounts(id2)[0]["balance"]
check("총액 보존(합=2,000,000)", bal1 + bal2 == 2_000_000)

print("\nB. 인증 엣지케이스 (웹 라우트)")
c = app.test_client()
r = c.post("/register", data={"username": u1, "password": "pw1234", "name": "중복"})
check("중복 아이디 가입 거부", "이미 사용" in r.get_data(as_text=True))
r = c.post("/register", data={"username": "t_" + uuid.uuid4().hex[:6], "password": "12", "name": "짧은비번"})
check("짧은 비밀번호 거부", "4자 이상" in r.get_data(as_text=True))
r = c.post("/login", data={"username": u1, "password": "wrongpw"})
check("틀린 비밀번호 로그인 실패", "올바르지 않" in r.get_data(as_text=True))
r = c.get("/accounts", follow_redirects=False)
check("비로그인 계좌접근 차단(리다이렉트)", r.status_code in (301, 302))

print("\nC. 캐시 정합성")
cache.flush()
q = "이자 높은 적금"
r_off, _ = search_es.semantic_search(q, use_cache=False)
r_on1, hit1 = search_es.semantic_search(q, use_cache=True)
r_on2, hit2 = search_es.semantic_search(q, use_cache=True)
check("캐시 on/off 결과 동일", [x["name"] for x in r_off] == [x["name"] for x in r_on1])
check("첫 호출은 미스", hit1 is False)
check("두번째 호출은 히트", hit2 is True)
n = cache.flush_kind("search")
check("flush_kind('search') 동작", n >= 1)

print("\nD. 데이터 무결성 (ES)")
for idx in ["fss-products", "bok-keystat"]:
    miss = es.count(index=idx, query={"bool": {"must_not": {"exists": {"field": "embedding"}}}})["count"]
    total = es.count(index=idx)["count"]
    check(f"{idx}: 임베딩 누락 0건 (총 {total})", miss == 0)

print("\nE. 챗봇 견고성")
r = c.post("/api/ask", json={"question": ""})
check("빈 질문 400 반환", r.status_code == 400)
r = c.post("/api/ask", json={"question": "오늘 점심 뭐 먹지?"})
check("엉뚱한 질문도 크래시 없이 200", r.status_code == 200)

print("\n" + "=" * 60)
print(f"결과: {PASS} PASS / {FAIL} FAIL")
