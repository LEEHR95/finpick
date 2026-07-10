# -*- coding: utf-8 -*-
"""성능/에이전트 테스트용 시뮬레이션 데이터 시드.

- sim_* 사용자 12명을 고정 프로필로 재생성한다.
- DB 거래내역/구독/포인트 로그를 멱등하게 다시 만든다.
- Elasticsearch bank-transfers 인덱스에 테스트 이체 이벤트 100건을 넣는다.

실데이터가 아닌 데모용 데이터다.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta

import bank_db
from elasticsearch import Elasticsearch, helpers

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
INDEX = "bank-transfers"
SIM_PASSWORD = "simpass123"
SIM_ADMIN_USERNAME = "sim_admin"
SIM_ADMIN_PASSWORD = "adminpass123"
RND = random.Random(42)

USER_PROFILES = [
    {"username": "sim_01", "name": "김민준", "balance": 30_000, "points": 40, "streak": 1, "txn_count": 4},
    {"username": "sim_02", "name": "이서연", "balance": 480_000, "points": 110, "streak": 3, "txn_count": 6},
    {"username": "sim_03", "name": "박도윤", "balance": 1_200_000, "points": 160, "streak": 4, "txn_count": 11},
    {"username": "sim_04", "name": "최지우", "balance": 3_500_000, "points": 220, "streak": 5, "txn_count": 10},
    {"username": "sim_05", "name": "정하은", "balance": 8_900_000, "points": 310, "streak": 8, "txn_count": 12},
    {"username": "sim_06", "name": "강시우", "balance": 15_000_000, "points": 280, "streak": 6, "txn_count": 9},
    {"username": "sim_07", "name": "조유나", "balance": 600_000, "points": 90, "streak": 2, "txn_count": 5},
    {"username": "sim_08", "name": "윤예준", "balance": 2_100_000, "points": 130, "streak": 2, "txn_count": 8},
    {"username": "sim_09", "name": "장서아", "balance": 25_000_000, "points": 450, "streak": 10, "txn_count": 14},
    {"username": "sim_10", "name": "임건우", "balance": 150_000, "points": 20, "streak": 0, "txn_count": 3},
    {"username": "sim_11", "name": "한지호", "balance": 4_700_000, "points": 240, "streak": 7, "txn_count": 7},
    {"username": "sim_12", "name": "오채원", "balance": 900_000, "points": 70, "streak": 1, "txn_count": 5},
]

LOAN_SUBSCRIPTIONS = [
    {"username": "sim_04", "code": "FP-CREDIT", "name": "핀픽 직장인신용대출", "ptype": "개인신용대출",
     "rate": 5.4, "term": 12, "principal": 8_000_000},
    {"username": "sim_05", "code": "FP-MTG", "name": "핀픽 내집마련대출", "ptype": "주택담보대출",
     "rate": 3.4, "term": 360, "principal": 120_000_000},
    {"username": "sim_11", "code": "FP-RENT", "name": "핀픽 전세자금대출", "ptype": "전세자금대출",
     "rate": 3.5, "term": 24, "principal": 30_000_000},
]

DEPOSIT_SUBSCRIPTIONS = [
    {"username": "sim_02", "code": "FP-S12", "name": "핀픽 매일적금", "ptype": "적금",
     "rate": 4.2, "term": 12, "principal": 200_000},
    {"username": "sim_06", "code": "FP-D24", "name": "핀픽 오래든예금", "ptype": "정기예금",
     "rate": 3.9, "term": 24, "principal": 5_000_000},
    {"username": "sim_09", "code": "FP-D12", "name": "핀픽 월급예금", "ptype": "정기예금",
     "rate": 3.6, "term": 12, "principal": 10_000_000},
]

EXT_BANKS = ["국민은행", "신한은행", "우리은행", "하나은행", "카카오뱅크", "토스뱅크"]
TX_COUNTERPARTS = ["급여", "카드값", "이체", "ATM", "공과금", "쇼핑"]
TRANSFER_MAPPING = {"properties": {
    "event_id": {"type": "keyword"},
    "type": {"type": "keyword"},
    "kind": {"type": "keyword"},
    "status": {"type": "keyword"},
    "ok": {"type": "boolean"},
    "user_id": {"type": "long"},
    "user_name": {"type": "keyword"},
    "from_bank": {"type": "keyword"},
    "from_account": {"type": "keyword"},
    "to_bank": {"type": "keyword"},
    "to_account": {"type": "keyword"},
    "to_account_masked": {"type": "keyword"},
    "recipient_name": {"type": "keyword"},
    "is_external": {"type": "boolean"},
    "amount": {"type": "long"},
    "fee": {"type": "long"},
    "amount_band": {"type": "keyword"},
    "currency": {"type": "keyword"},
    "balance_after": {"type": "long"},
    "channel": {"type": "keyword"},
    "async": {"type": "boolean"},
    "message": {"type": "text"},
    "memo": {"type": "text"},
    "client_ip": {"type": "ip"},
    "user_agent": {"type": "keyword", "ignore_above": 1024},
    "ts": {"type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss||strict_date_optional_time||epoch_millis"},
    "date": {"type": "date", "format": "yyyy-MM-dd"},
    "hour": {"type": "integer"},
}}


def es_client() -> Elasticsearch:
    return Elasticsearch(ES_URL, request_timeout=30)


def _now() -> datetime:
    return datetime.now().replace(microsecond=0)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _amount_band(amount: int) -> str:
    if amount >= 10_000_000:
        return "초고액(1천만↑)"
    if amount >= 1_000_000:
        return "고액(100만↑)"
    if amount >= 100_000:
        return "중액(10만↑)"
    return "소액(10만↓)"


def ensure_transfer_index(es: Elasticsearch) -> None:
    if not es.indices.exists(index=INDEX):
        es.indices.create(index=INDEX, mappings=TRANSFER_MAPPING)


def ensure_sim_admin() -> dict:
    admin = bank_db.verify_user(SIM_ADMIN_USERNAME, SIM_ADMIN_PASSWORD)
    if admin:
        return admin
    bank_db.create_admin_user(SIM_ADMIN_USERNAME, SIM_ADMIN_PASSWORD, "시뮬관리자")
    admin = bank_db.verify_user(SIM_ADMIN_USERNAME, SIM_ADMIN_PASSWORD)
    if not admin:
        raise RuntimeError("sim_admin 계정 생성에 실패했다.")
    return admin


def ensure_users() -> list[dict]:
    bank_db.init_db()
    ensure_sim_admin()
    out = []
    for profile in USER_PROFILES:
        user = bank_db.verify_user(profile["username"], SIM_PASSWORD)
        if not user:
            ok, msg = bank_db.create_user(profile["username"], SIM_PASSWORD, profile["name"])
            if not ok:
                raise RuntimeError(f"{profile['username']} 생성 실패: {msg}")
            user = bank_db.verify_user(profile["username"], SIM_PASSWORD)
        accounts = bank_db.get_accounts(user["id"])
        main = next((a for a in accounts if a["acct_type"] == "main"), None)
        if not main:
            raise RuntimeError(f"{profile['username']}의 main 계좌를 찾을 수 없다.")
        out.append({
            **profile,
            "user_id": user["id"],
            "account_id": main["id"],
            "account_no": main["account_no"],
        })
    return out


def reset_sim_state(users: list[dict]) -> None:
    account_ids = [u["account_id"] for u in users]
    user_ids = [u["user_id"] for u in users]
    with bank_db.get_db() as db:
        for account_id in account_ids:
            db.execute("DELETE FROM transactions WHERE account_id=?", (account_id,))
        for user_id in user_ids:
            db.execute("DELETE FROM subscriptions WHERE user_id=?", (user_id,))
            db.execute("DELETE FROM point_log WHERE user_id=?", (user_id,))
            db.execute("DELETE FROM stock_holdings WHERE user_id=?", (user_id,))
            db.execute("DELETE FROM stock_predictions WHERE user_id=?", (user_id,))
            db.execute("UPDATE users SET points=0 WHERE id=?", (user_id,))
            db.execute("DELETE FROM accounts WHERE user_id=? AND acct_type='point'", (user_id,))


def seed_transactions(users: list[dict], base_now: datetime) -> None:
    with bank_db.get_db() as db:
        for idx, user in enumerate(users):
            balance = user["balance"]
            for step in range(user["txn_count"]):
                amount = RND.choice([10_000, 30_000, 50_000, 120_000, 300_000, 550_000])
                kind = "입금" if (step + idx) % 3 == 0 else "출금"
                if kind == "입금":
                    balance_after = balance + amount
                else:
                    balance_after = max(0, balance - amount)
                created_at = (base_now - timedelta(days=idx * 2 + step, hours=step % 5)).strftime("%Y-%m-%d %H:%M:%S")
                db.execute(
                    "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        user["account_id"],
                        kind,
                        TX_COUNTERPARTS[(idx + step) % len(TX_COUNTERPARTS)],
                        amount,
                        balance_after,
                        "sim-seed",
                        created_at,
                    ),
                )
            db.execute("UPDATE accounts SET balance=? WHERE id=?", (user["balance"], user["account_id"]))


def seed_points(users: list[dict], base_now: datetime) -> None:
    with bank_db.get_db() as db:
        for idx, user in enumerate(users):
            total = 0
            streak = user["streak"]
            for day_offset in range(streak):
                day = (base_now.date() - timedelta(days=day_offset)).strftime("%Y-%m-%d")
                created_at = f"{day} 09:{10 + idx:02d}:00"
                delta = 20 if day_offset == 0 else 10
                total += delta
                db.execute(
                    "INSERT INTO point_log(user_id,delta,reason,label,day,created_at) VALUES(?,?,?,?,?,?)",
                    (user["user_id"], delta, f"learn-{day_offset}", "학습 보상", day, created_at),
                )
            if user["points"] > total:
                extra = user["points"] - total
                extra_day = (base_now.date() - timedelta(days=14 + idx)).strftime("%Y-%m-%d")
                db.execute(
                    "INSERT INTO point_log(user_id,delta,reason,label,day,created_at) VALUES(?,?,?,?,?,?)",
                    (user["user_id"], extra, "event-bonus", "이벤트 보상", extra_day, f"{extra_day} 14:00:00"),
                )
                total += extra
            db.execute("UPDATE users SET points=? WHERE id=?", (total, user["user_id"]))


def seed_subscriptions(users: list[dict], base_now: datetime) -> None:
    user_by_name = {u["username"]: u for u in users}
    rows = LOAN_SUBSCRIPTIONS + DEPOSIT_SUBSCRIPTIONS
    with bank_db.get_db() as db:
        for idx, sub in enumerate(rows):
            user = user_by_name[sub["username"]]
            created_at = (base_now - timedelta(days=30 + idx)).strftime("%Y-%m-%d %H:%M:%S")
            maturity = sub["principal"] if "대출" in sub["ptype"] else int(
                sub["principal"] + sub["principal"] * sub["rate"] / 100 * sub["term"] / 12
            )
            db.execute(
                "INSERT INTO subscriptions(user_id,product_code,product_name,ptype,rate,term_months,"
                "principal,maturity_amount,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    user["user_id"],
                    sub["code"],
                    sub["name"],
                    sub["ptype"],
                    sub["rate"],
                    sub["term"],
                    sub["principal"],
                    maturity,
                    "가입",
                    created_at,
                ),
            )


def make_event(user: dict, amount: int, when: datetime, sequence: int, *,
               ok: bool = True, external: bool = False, memo: str = "sim-seed") -> dict:
    to_bank = EXT_BANKS[sequence % len(EXT_BANKS)] if external else "FinPick"
    prefix = "200" if external else "100"
    to_account = f"{prefix}-{200 + (sequence % 700):03d}-{(sequence * 7919) % 1_000_000:06d}"
    return {
        "_index": INDEX,
        "_id": f"sim-{sequence:03d}",
        "_source": {
            "event_id": f"sim-{sequence:03d}",
            "type": "transfer",
            "kind": "sim-external" if external else "sim-internal",
            "status": "success" if ok else "failed",
            "ok": ok,
            "user_id": user["user_id"],
            "user_name": user["name"],
            "from_bank": "FinPick",
            "from_account": user["account_no"],
            "to_bank": to_bank,
            "to_account": to_account,
            "to_account_masked": to_account[:8] + "***" + to_account[-3:],
            "recipient_name": None if external else "테스트수취인",
            "is_external": external,
            "amount": amount,
            "fee": 0,
            "amount_band": _amount_band(amount),
            "currency": "KRW",
            "balance_after": max(0, user["balance"] - amount) if ok else user["balance"],
            "channel": "web",
            "async": False,
            "message": "이체 완료" if ok else "잔액 부족",
            "memo": memo,
            "client_ip": f"175.223.{sequence % 255}.{(sequence * 7) % 254 + 1}",
            "user_agent": "Mozilla/5.0 (sim-seed)",
            "ts": _iso(when),
            "date": when.strftime("%Y-%m-%d"),
            "hour": when.hour,
        },
    }


def build_events(users: list[dict], base_now: datetime) -> tuple[list[dict], dict]:
    user_by_name = {u["username"]: u for u in users}
    normal_users = [u for u in users if u["username"] not in {"sim_03", "sim_06", "sim_08", "sim_09", "sim_11"}]
    events = []
    seq = 1

    for idx in range(76):
        user = normal_users[idx % len(normal_users)]
        amount = [5_000, 12_000, 30_000, 55_000, 120_000, 250_000, 480_000][idx % 7]
        when = base_now - timedelta(days=idx % 10, hours=(idx * 3) % 24, minutes=(idx * 11) % 60)
        events.append(make_event(user, amount, when, seq, ok=True, external=(idx % 3 == 0), memo="normal-flow"))
        seq += 1

    high_value_user = user_by_name["sim_09"]
    for idx, amount in enumerate([15_000_000, 18_500_000, 27_000_000], start=1):
        when = base_now - timedelta(days=idx, hours=1)
        events.append(make_event(high_value_user, amount, when, seq, ok=True, external=True, memo="high-value"))
        seq += 1

    structuring_user = user_by_name["sim_06"]
    for idx, amount in enumerate([9_100_000, 9_350_000, 9_700_000, 9_900_000], start=1):
        when = base_now - timedelta(days=2, minutes=idx * 6)
        events.append(make_event(structuring_user, amount, when, seq, ok=True, external=True, memo="structuring"))
        seq += 1

    velocity_user = user_by_name["sim_03"]
    burst_start = base_now - timedelta(hours=2)
    for idx, amount in enumerate([90_000, 200_000, 350_000, 90_000, 200_000, 350_000, 90_000, 200_000]):
        when = burst_start + timedelta(minutes=idx)
        events.append(make_event(velocity_user, amount, when, seq, ok=True, external=True, memo="velocity-burst"))
        seq += 1

    failure_user = user_by_name["sim_08"]
    for idx, amount in enumerate([500_000, 800_000, 1_000_000, 1_500_000, 2_000_000], start=1):
        when = base_now - timedelta(days=1, hours=idx)
        events.append(make_event(failure_user, amount, when, seq, ok=False, external=(idx % 2 == 0), memo="failed-attempt"))
        seq += 1

    night_user = user_by_name["sim_11"]
    for idx, amount in enumerate([300_000, 800_000, 1_500_000, 800_000], start=1):
        when = (base_now - timedelta(days=idx)).replace(hour=2 + (idx % 3), minute=idx * 9)
        events.append(make_event(night_user, amount, when, seq, ok=True, external=True, memo="night-transfer"))
        seq += 1

    if len(events) != 100:
        raise RuntimeError(f"이체 이벤트 수가 100건이 아니다: {len(events)}")

    targets = {
        "high_value": {"user_id": high_value_user["user_id"], "name": high_value_user["name"]},
        "structuring": {"user_id": structuring_user["user_id"], "name": structuring_user["name"]},
        "velocity": {"user_id": velocity_user["user_id"], "name": velocity_user["name"]},
        "failures": {"user_id": failure_user["user_id"], "name": failure_user["name"]},
        "night": {"user_id": night_user["user_id"], "name": night_user["name"]},
        "credit_good": {"user_id": high_value_user["user_id"], "name": high_value_user["name"]},
        "credit_risky": {"user_id": user_by_name["sim_05"]["user_id"], "name": user_by_name["sim_05"]["name"]},
    }
    return events, targets


def seed_es(events: list[dict], es: Elasticsearch) -> int:
    ensure_transfer_index(es)
    es.delete_by_query(
        index=INDEX,
        query={"prefix": {"event_id": "sim-"}},
        conflicts="proceed",
        refresh=True,
        ignore_unavailable=True,
        wait_for_completion=True,
    )
    helpers.bulk(es, events, raise_on_error=True)
    es.indices.refresh(index=INDEX)
    return es.count(index=INDEX, query={"prefix": {"event_id": "sim-"}})["count"]


def seed() -> dict:
    users = ensure_users()
    reset_sim_state(users)
    base_now = _now()
    seed_transactions(users, base_now)
    seed_points(users, base_now)
    seed_subscriptions(users, base_now)

    es = es_client()
    events, anomaly_targets = build_events(users, base_now)
    sim_event_count = seed_es(events, es)

    return {
        "user_count": len(users),
        "event_count": len(events),
        "sim_event_count": sim_event_count,
        "users": users,
        "anomaly_targets": anomaly_targets,
        "admin": {
            "username": SIM_ADMIN_USERNAME,
            "password": SIM_ADMIN_PASSWORD,
        },
    }


def main() -> None:
    summary = seed()
    print(f"sim users: {summary['user_count']}명")
    print(f"sim transfer events: {summary['event_count']}건 적재, sim-* 기준 ES 총 {summary['sim_event_count']}건")
    for label, info in summary["anomaly_targets"].items():
        print(f"{label}: user {info['user_id']} / {info['name']}")
    print(f"admin: {summary['admin']['username']} / {summary['admin']['password']}")


if __name__ == "__main__":
    main()
