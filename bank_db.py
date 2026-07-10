"""
은행 웹 데이터베이스 (SQLite) — 회원/계좌/거래내역.

테이블:
  users        : 회원 (아이디, 비밀번호 해시, 이름)
  accounts     : 계좌 (계좌번호, 잔액)  — 회원당 1개 자동 생성
  transactions : 거래내역 (입금/출금, 상대, 금액, 거래후잔액)

DB 파일: data/bank.db (data/ 는 git 제외)
"""

import os
import random
import sqlite3
from datetime import datetime, timedelta
from contextlib import contextmanager

from werkzeug.security import generate_password_hash, check_password_hash

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "bank.db")


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT NOT NULL,
            points INTEGER NOT NULL DEFAULT 0,
            role TEXT NOT NULL DEFAULT 'user',   -- user / admin
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            account_no TEXT UNIQUE NOT NULL,
            balance INTEGER NOT NULL DEFAULT 0,
            acct_type TEXT NOT NULL DEFAULT 'main',   -- main(입출금) / point(포인트머니)
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL REFERENCES accounts(id),
            kind TEXT NOT NULL,            -- 입금 / 출금
            counterpart TEXT,              -- 상대 계좌/이름
            amount INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            memo TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            product_code TEXT NOT NULL,
            product_name TEXT NOT NULL,
            ptype TEXT NOT NULL,
            rate REAL NOT NULL,
            term_months INTEGER NOT NULL,
            principal INTEGER NOT NULL,
            maturity_amount INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT '가입',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS point_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            delta INTEGER NOT NULL,         -- 적립(+)/사용(-)
            reason TEXT NOT NULL,           -- 적립 사유 키 (learn-daily, quiz-3 등)
            label TEXT,                     -- 사람이 읽는 설명
            day TEXT NOT NULL,              -- YYYY-MM-DD (하루 1회 중복 적립 방지용)
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stocks (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            market TEXT NOT NULL,           -- 코스피 / 코스닥
            price REAL NOT NULL,            -- 오늘 종가(데모)
            prev_price REAL NOT NULL,       -- 어제 종가
            updated_day TEXT NOT NULL       -- 마지막 갱신일(YYYY-MM-DD)
        );
        CREATE TABLE IF NOT EXISTS market_index (
            code TEXT PRIMARY KEY,          -- KOSPI / KOSDAQ / FX
            value REAL NOT NULL,
            prev_value REAL NOT NULL,
            updated_day TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stock_holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            code TEXT NOT NULL REFERENCES stocks(code),
            shares REAL NOT NULL DEFAULT 0,
            avg_cost REAL NOT NULL DEFAULT 0,   -- 평균 매입단가(원)
            UNIQUE(user_id, code)
        );
        CREATE TABLE IF NOT EXISTS stock_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            code TEXT NOT NULL REFERENCES stocks(code),
            day TEXT NOT NULL,              -- 예측을 건 날짜
            guess TEXT NOT NULL,            -- up / down
            resolved INTEGER NOT NULL DEFAULT 0,
            correct INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, code, day)
        );
        CREATE TABLE IF NOT EXISTS prompts (
            key TEXT PRIMARY KEY,           -- rag_system / consult_deposit 등
            label TEXT NOT NULL,            -- 관리자 화면에 보일 이름
            content TEXT NOT NULL,          -- {{변수명}} 형태로 아래 prompt_variables 값을 넣을 수 있다
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prompt_variables (
            name TEXT PRIMARY KEY,          -- 프롬프트 안에서 {{name}}으로 참조
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS agent_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            route TEXT NOT NULL DEFAULT 'admin',
            source TEXT NOT NULL DEFAULT 'web',
            user_id INTEGER,
            ok INTEGER NOT NULL DEFAULT 1,
            elapsed_ms REAL NOT NULL DEFAULT 0,
            question TEXT,
            scenario_label TEXT,
            answer_chars INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL
        );
        """)
        # 기존 DB에 points 컬럼이 없으면 추가 (마이그레이션)
        cols = [r["name"] for r in db.execute("PRAGMA table_info(users)").fetchall()]
        if "points" not in cols:
            db.execute("ALTER TABLE users ADD COLUMN points INTEGER NOT NULL DEFAULT 0")
        acct_cols = [r["name"] for r in db.execute("PRAGMA table_info(accounts)").fetchall()]
        if "acct_type" not in acct_cols:
            db.execute("ALTER TABLE accounts ADD COLUMN acct_type TEXT NOT NULL DEFAULT 'main'")
        user_cols = [r["name"] for r in db.execute("PRAGMA table_info(users)").fetchall()]
        if "role" not in user_cols:
            db.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'")
        _seed_market(db)


# 모의 투자용 종목 시드 (실제 상장사명 사용, 가격은 데모용 가상값 — 실제 시세 아님)
STOCK_SEED = [
    ("005930", "삼성전자", "코스피", 71000),
    ("000660", "SK하이닉스", "코스피", 180000),
    ("005380", "현대차", "코스피", 230000),
    ("035420", "NAVER", "코스피", 190000),
    ("035720", "카카오", "코스피", 40000),
    ("247540", "에코프로비엠", "코스닥", 150000),
    ("196170", "알테오젠", "코스닥", 300000),
    ("028300", "HLB", "코스닥", 60000),
]
INDEX_SEED = [("KOSPI", 2650.0), ("KOSDAQ", 850.0), ("FX", 1372.5)]
_NEVER = "1970-01-01"


def _seed_market(db):
    for code, name, market, price in STOCK_SEED:
        db.execute(
            "INSERT OR IGNORE INTO stocks(code,name,market,price,prev_price,updated_day) "
            "VALUES(?,?,?,?,?,?)", (code, name, market, price, price, _NEVER))
    for code, value in INDEX_SEED:
        db.execute(
            "INSERT OR IGNORE INTO market_index(code,value,prev_value,updated_day) "
            "VALUES(?,?,?,?)", (code, value, value, _NEVER))


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _gen_account_no(db):
    """100-XXX-XXXXXX 형식의 고유 계좌번호 생성."""
    while True:
        no = f"100-{random.randint(100,999)}-{random.randint(0,999999):06d}"
        if not db.execute("SELECT 1 FROM accounts WHERE account_no=?", (no,)).fetchone():
            return no


# --------------------------- 회원 ---------------------------
def create_user(username, password, name, opening_balance=1_000_000):
    """회원 생성 + 계좌 자동 개설(가입 축하금 기본 100만원). (성공여부, 메시지)."""
    with get_db() as db:
        if db.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
            return False, "이미 사용 중인 아이디입니다."
        cur = db.execute(
            "INSERT INTO users(username,password_hash,name,created_at) VALUES(?,?,?,?)",
            (username, generate_password_hash(password), name, _now()))
        uid = cur.lastrowid
        no = _gen_account_no(db)
        db.execute(
            "INSERT INTO accounts(user_id,account_no,balance,created_at) VALUES(?,?,?,?)",
            (uid, no, opening_balance, _now()))
    return True, "가입 완료"


def verify_user(username, password):
    """로그인 검증. 성공 시 user dict, 실패 시 None."""
    with get_db() as db:
        row = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if row and check_password_hash(row["password_hash"], password):
        return {"id": row["id"], "username": row["username"], "name": row["name"],
                "role": row["role"]}
    return None


def create_admin_user(username, password, name):
    """관리자 계정 생성 (일반 계좌는 열지 않음, role='admin'). (성공여부, 메시지)."""
    with get_db() as db:
        if db.execute("SELECT 1 FROM users WHERE username=?", (username,)).fetchone():
            return False, "이미 사용 중인 아이디입니다."
        db.execute(
            "INSERT INTO users(username,password_hash,name,role,created_at) VALUES(?,?,?,?,?)",
            (username, generate_password_hash(password), name, "admin", _now()))
    return True, "관리자 계정 생성 완료"


# --------------------------- 계좌 ---------------------------
def get_accounts(user_id):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM accounts WHERE user_id=? ORDER BY id", (user_id,)).fetchall()
    return [dict(r) for r in rows]


def get_transactions(account_id, limit=30):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM transactions WHERE account_id=? ORDER BY id DESC LIMIT ?",
            (account_id, limit)).fetchall()
    return [dict(r) for r in rows]


def get_point_account(user_id):
    """픽앤업 전환 통장 조회. 아직 개설 안 했으면 None.
    (내부적으로 acct_type='point'로 '전환/우대 대상' 통장을 표시한다.)"""
    with get_db() as db:
        r = db.execute(
            "SELECT * FROM accounts WHERE user_id=? AND acct_type='point'", (user_id,)).fetchone()
    return dict(r) if r else None


def open_pickup_account(user_id):
    """픽앤업 전환 통장(입출금 통장, 우대금리 적용 대상)을 개설. 이미 있으면 그대로 반환.
    (계좌 dict, 새로_개설_여부)."""
    with get_db() as db:
        existing = db.execute(
            "SELECT * FROM accounts WHERE user_id=? AND acct_type='point'", (user_id,)).fetchone()
        if existing:
            return dict(existing), False
        no = _gen_account_no(db)
        db.execute(
            "INSERT INTO accounts(user_id,account_no,balance,acct_type,created_at) VALUES(?,?,?,?,?)",
            (user_id, no, 0, "point", _now()))
        row = db.execute(
            "SELECT * FROM accounts WHERE user_id=? AND acct_type='point'", (user_id,)).fetchone()
    return dict(row), True


def convert_points(user_id, points, won_per_point):
    """포인트를 실제 현금으로 환전해 포인트머니 통장(acct_type='point')에 입금.
    통장이 없으면 자동 개설. (성공여부, 메시지, 남은 포인트, 통장 잔액)."""
    if points <= 0:
        return False, "전환할 포인트를 확인하세요.", None, None
    amount = points * won_per_point

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "UPDATE users SET points=points-? WHERE id=? AND points>=?",
            (points, user_id, points))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            bal = conn.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()["points"]
            return False, "포인트가 부족합니다.", bal, None

        acct = conn.execute(
            "SELECT * FROM accounts WHERE user_id=? AND acct_type='point'", (user_id,)).fetchone()
        if not acct:
            no = _gen_account_no(conn)
            conn.execute(
                "INSERT INTO accounts(user_id,account_no,balance,acct_type,created_at) VALUES(?,?,?,?,?)",
                (user_id, no, 0, "point", _now()))
            acct = conn.execute(
                "SELECT * FROM accounts WHERE user_id=? AND acct_type='point'", (user_id,)).fetchone()

        conn.execute("UPDATE accounts SET balance=balance+? WHERE id=?", (amount, acct["id"]))
        acct_bal = conn.execute("SELECT balance FROM accounts WHERE id=?", (acct["id"],)).fetchone()["balance"]
        conn.execute(
            "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (acct["id"], "입금", "포인트 전환", amount, acct_bal, f"{points:,}P 전환", _now()))
        conn.execute(
            "INSERT INTO point_log(user_id,delta,reason,label,day,created_at) VALUES(?,?,?,?,?,?)",
            (user_id, -points, "convert", f"포인트머니 전환 ({amount:,}원)", _today(), _now()))

        new_points = conn.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()["points"]
        conn.execute("COMMIT")
        return True, f"{points:,}P를 {amount:,}원으로 전환했어요.", new_points, acct_bal
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def move_point_to_main(user_id, amount):
    """포인트머니 통장 → 입출금 통장으로 본인 계좌 간 자금 이동. (성공여부, 메시지, 포인트머니잔액, 입출금잔액)."""
    if amount <= 0:
        return False, "금액을 확인하세요.", None, None

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN IMMEDIATE")
        point_acct = conn.execute(
            "SELECT * FROM accounts WHERE user_id=? AND acct_type='point'", (user_id,)).fetchone()
        main_acct = conn.execute(
            "SELECT * FROM accounts WHERE user_id=? AND acct_type='main' ORDER BY id LIMIT 1",
            (user_id,)).fetchone()
        if not point_acct or not main_acct:
            conn.execute("ROLLBACK")
            return False, "계좌를 찾을 수 없습니다.", None, None

        cur = conn.execute(
            "UPDATE accounts SET balance=balance-? WHERE id=? AND balance>=?",
            (amount, point_acct["id"], amount))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            return False, "포인트머니 잔액이 부족합니다.", point_acct["balance"], main_acct["balance"]
        conn.execute("UPDATE accounts SET balance=balance+? WHERE id=?", (amount, main_acct["id"]))

        p_after = conn.execute("SELECT balance FROM accounts WHERE id=?", (point_acct["id"],)).fetchone()["balance"]
        m_after = conn.execute("SELECT balance FROM accounts WHERE id=?", (main_acct["id"],)).fetchone()["balance"]
        conn.execute(
            "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (point_acct["id"], "출금", "입출금통장", amount, p_after, "포인트머니 이동", _now()))
        conn.execute(
            "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (main_acct["id"], "입금", "포인트머니통장", amount, m_after, "포인트머니 이동", _now()))
        conn.execute("COMMIT")
        return True, f"포인트머니 {amount:,}원을 입출금 통장으로 옮겼어요.", p_after, m_after
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


# --------------------------- 이체 ---------------------------
def _resolve_source(conn, from_user_id, from_account_id):
    """출금 계좌를 고른다. from_account_id 지정 시 본인 소유 확인 후 그 계좌, 없으면 첫 계좌."""
    if from_account_id:
        return conn.execute(
            "SELECT * FROM accounts WHERE id=? AND user_id=?",
            (from_account_id, from_user_id)).fetchone()
    return conn.execute(
        "SELECT * FROM accounts WHERE user_id=? ORDER BY id LIMIT 1",
        (from_user_id,)).fetchone()


def transfer(from_user_id, to_account_no, amount, memo="", from_account_id=None):
    """본인 계좌 → 같은 은행 다른 계좌로 이체. from_account_id로 출금 계좌 선택. (성공여부, 메시지).

    동시성 안전: BEGIN IMMEDIATE로 이체 전체를 한 트랜잭션으로 잠그고(동시 이체는 순차 처리),
    차감은 'balance>=amount'를 만족할 때만 원자적으로 수행해 오버드로우/이중지급을 차단한다.
    """
    if amount <= 0:
        return False, "이체 금액을 확인하세요."

    # 트랜잭션을 직접 제어하기 위해 autocommit(isolation_level=None) 연결을 연다.
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN IMMEDIATE")  # 쓰기 잠금 선점 → 동시 이체 직렬화
        src = _resolve_source(conn, from_user_id, from_account_id)
        if not src:
            conn.execute("ROLLBACK"); return False, "출금 계좌가 없습니다."
        dst = conn.execute(
            "SELECT * FROM accounts WHERE account_no=?", (to_account_no,)).fetchone()
        if not dst:
            conn.execute("ROLLBACK"); return False, "받는 계좌번호를 찾을 수 없습니다."
        if dst["id"] == src["id"]:
            conn.execute("ROLLBACK"); return False, "같은 계좌로는 이체할 수 없습니다."

        # 잔액이 충분할 때만 원자적으로 차감 (rowcount==0 이면 잔액 부족)
        cur = conn.execute(
            "UPDATE accounts SET balance=balance-? WHERE id=? AND balance>=?",
            (amount, src["id"], amount))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK"); return False, "잔액이 부족합니다."
        conn.execute("UPDATE accounts SET balance=balance+? WHERE id=?", (amount, dst["id"]))

        src_after = conn.execute("SELECT balance FROM accounts WHERE id=?", (src["id"],)).fetchone()["balance"]
        dst_after = conn.execute("SELECT balance FROM accounts WHERE id=?", (dst["id"],)).fetchone()["balance"]
        conn.execute(
            "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (src["id"], "출금", to_account_no, amount, src_after, memo, _now()))
        conn.execute(
            "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (dst["id"], "입금", src["account_no"], amount, dst_after, memo, _now()))
        conn.execute("COMMIT")
        return True, f"{to_account_no}로 {amount:,}원 이체 완료"
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"이체 처리 중 오류: {e}"
    finally:
        conn.close()


def subscribe(user_id, product, amount):
    """핀픽 자체 상품 가입.
    - 예금형: 계좌에서 금액을 원자적으로 차감(예치).
    - 대출형: 계좌로 금액을 입금(차입) + 대출내역 기록.
    product는 카탈로그 dict(code,name,type,kind,rate,term,min). (성공여부, 메시지)."""
    if amount <= 0:
        return False, "금액을 확인하세요."
    if amount < product.get("min", 0):
        label = "대출" if product.get("kind") == "대출" else "가입"
        return False, f"최소 {label} 금액은 {product['min']:,}원입니다."
    term = int(product.get("term", 0))
    rate = float(product.get("rate", 0))
    is_loan = product.get("kind") == "대출"
    # 예금: 만기 원리금 / 대출: 원금(대출금)
    maturity = amount if is_loan else (amount + int(amount * rate / 100 * term / 12) if term > 0 else amount)

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        src = conn.execute(
            "SELECT * FROM accounts WHERE user_id=? ORDER BY id LIMIT 1", (user_id,)).fetchone()
        if not src:
            conn.execute("ROLLBACK"); return False, "계좌가 없습니다."

        if is_loan:  # 대출 실행 → 입금
            conn.execute("UPDATE accounts SET balance=balance+? WHERE id=?", (amount, src["id"]))
            bal_after = conn.execute("SELECT balance FROM accounts WHERE id=?", (src["id"],)).fetchone()["balance"]
            conn.execute(
                "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (src["id"], "입금", f"{product['name']} 대출실행", amount, bal_after, "대출", _now()))
            msg = f"{product['name']} 대출 실행 완료 ({amount:,}원 입금)"
        else:  # 예금 가입 → 출금(잔액 확인)
            cur = conn.execute(
                "UPDATE accounts SET balance=balance-? WHERE id=? AND balance>=?",
                (amount, src["id"], amount))
            if cur.rowcount == 0:
                conn.execute("ROLLBACK"); return False, "잔액이 부족합니다."
            bal_after = conn.execute("SELECT balance FROM accounts WHERE id=?", (src["id"],)).fetchone()["balance"]
            conn.execute(
                "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
                " VALUES(?,?,?,?,?,?,?)",
                (src["id"], "출금", f"{product['name']} 가입", amount, bal_after, "상품가입", _now()))
            msg = f"{product['name']} 가입 완료 ({amount:,}원)"

        conn.execute(
            "INSERT INTO subscriptions(user_id,product_code,product_name,ptype,rate,term_months,"
            "principal,maturity_amount,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (user_id, product["code"], product["name"], product["type"], rate, term,
             amount, maturity, _now()))
        conn.execute("COMMIT")
        return True, msg
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"처리 중 오류: {e}"
    finally:
        conn.close()


def get_user(user_id):
    """회원 기본정보 (마이페이지용)."""
    with get_db() as db:
        r = db.execute("SELECT id, username, name, created_at FROM users WHERE id=?",
                       (user_id,)).fetchone()
    return dict(r) if r else None


def get_subscriptions(user_id):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM subscriptions WHERE user_id=? ORDER BY id DESC", (user_id,)).fetchall()
    return [dict(r) for r in rows]


def lookup_account_name(account_no):
    """FinPick(본행) 계좌번호로 예금주 이름 조회. 없으면 None (이체 전 '받는분 확인'용)."""
    with get_db() as db:
        row = db.execute(
            "SELECT u.name FROM accounts a JOIN users u ON u.id=a.user_id WHERE a.account_no=?",
            (account_no,)).fetchone()
    return row["name"] if row else None


def transfer_external(from_user_id, bank, to_account_no, amount, memo="", from_account_id=None):
    """타행(데모) 이체 — 우리 DB에 없는 계좌라 '출금'만 원자적으로 처리(외부로 나감).
    from_account_id로 출금 계좌 선택. (성공여부, 메시지)."""
    if amount <= 0:
        return False, "이체 금액을 확인하세요."
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN IMMEDIATE")
        src = _resolve_source(conn, from_user_id, from_account_id)
        if not src:
            conn.execute("ROLLBACK"); return False, "출금 계좌가 없습니다."
        cur = conn.execute(
            "UPDATE accounts SET balance=balance-? WHERE id=? AND balance>=?",
            (amount, src["id"], amount))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK"); return False, "잔액이 부족합니다."
        src_after = conn.execute("SELECT balance FROM accounts WHERE id=?", (src["id"],)).fetchone()["balance"]
        conn.execute(
            "INSERT INTO transactions(account_id,kind,counterpart,amount,balance_after,memo,created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (src["id"], "출금", f"{bank} {to_account_no}", amount, src_after, memo, _now()))
        conn.execute("COMMIT")
        return True, f"{bank} {to_account_no}로 {amount:,}원 이체 완료 (타행·데모)"
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        return False, f"이체 처리 중 오류: {e}"
    finally:
        conn.close()


# --------------------------- 경제공부 포인트 ---------------------------
def get_points(user_id):
    """회원의 현재 포인트 잔액."""
    with get_db() as db:
        r = db.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()
    return r["points"] if r else 0


def _today():
    return datetime.now().strftime("%Y-%m-%d")


def award_once_per_day(user_id, delta, reason, label=""):
    """하루 1회만 적립. 같은 (user, reason)이 오늘 이미 적립됐으면 건너뜀.

    반환: (적립여부, 새 잔액, 실제 적립액). 이미 받은 날이면 (False, 잔액, 0).
    동시 요청에도 안전하도록 BEGIN IMMEDIATE로 직렬화한다.
    """
    day = _today()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN IMMEDIATE")
        dup = conn.execute(
            "SELECT 1 FROM point_log WHERE user_id=? AND reason=? AND day=?",
            (user_id, reason, day)).fetchone()
        if dup:
            bal = conn.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()["points"]
            conn.execute("ROLLBACK")
            return False, bal, 0
        conn.execute("UPDATE users SET points=points+? WHERE id=?", (delta, user_id))
        conn.execute(
            "INSERT INTO point_log(user_id,delta,reason,label,day,created_at) VALUES(?,?,?,?,?,?)",
            (user_id, delta, reason, label, day, _now()))
        bal = conn.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()["points"]
        conn.execute("COMMIT")
        return True, bal, delta
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def get_point_log(user_id, limit=20):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM point_log WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (user_id, limit)).fetchall()
    return [dict(r) for r in rows]


def rewarded_reasons_today(user_id):
    """오늘 이미 적립받은 reason 집합 (화면에서 '완료' 표시용)."""
    with get_db() as db:
        rows = db.execute(
            "SELECT DISTINCT reason FROM point_log WHERE user_id=? AND day=?",
            (user_id, _today())).fetchall()
    return {r["reason"] for r in rows}


def get_streak(user_id):
    """연속 학습(포인트 적립) 일수. 오늘 아직 안 했으면 어제까지 이어진 연속일수를 보여준다."""
    with get_db() as db:
        rows = db.execute(
            "SELECT DISTINCT day FROM point_log WHERE user_id=?", (user_id,)).fetchall()
    days = {r["day"] for r in rows}
    d = datetime.strptime(_today(), "%Y-%m-%d")
    if d.strftime("%Y-%m-%d") not in days:
        d -= timedelta(days=1)
    streak = 0
    while d.strftime("%Y-%m-%d") in days:
        streak += 1
        d -= timedelta(days=1)
    return streak


# --------------------------- 모의 투자 (실시간 아님, 하루 1회 장마감 갱신) ---------------------------
def refresh_market(predict_reward=30):
    """오늘 날짜 기준으로 아직 안 갱신된 종목·지수를 하루치씩 갱신(모의 장마감).
    갱신 시점에, 어제 건 예측(주가 예측 게임)을 오늘 가격과 비교해 정산한다."""
    today = _today()
    with get_db() as db:
        for s in db.execute("SELECT * FROM stocks").fetchall():
            if s["updated_day"] == today:
                continue
            prev_day = s["updated_day"]
            rnd = random.Random(f"{s['code']}-{today}")
            new_price = max(1, round(s["price"] * (1 + rnd.uniform(-0.05, 0.05))))
            db.execute("UPDATE stocks SET prev_price=?, price=?, updated_day=? WHERE code=?",
                       (s["price"], new_price, today, s["code"]))
            direction = "up" if new_price > s["price"] else ("down" if new_price < s["price"] else "flat")
            preds = db.execute(
                "SELECT * FROM stock_predictions WHERE code=? AND day=? AND resolved=0",
                (s["code"], prev_day)).fetchall()
            for p in preds:
                correct = 1 if p["guess"] == direction else 0
                db.execute("UPDATE stock_predictions SET resolved=1, correct=? WHERE id=?",
                           (correct, p["id"]))
                if correct:
                    db.execute("UPDATE users SET points=points+? WHERE id=?",
                               (predict_reward, p["user_id"]))
                    db.execute(
                        "INSERT INTO point_log(user_id,delta,reason,label,day,created_at) VALUES(?,?,?,?,?,?)",
                        (p["user_id"], predict_reward, f"predict-{s['code']}",
                         f"{s['name']} 주가 예측 성공", today, _now()))
        for m in db.execute("SELECT * FROM market_index").fetchall():
            if m["updated_day"] == today:
                continue
            rnd = random.Random(f"{m['code']}-{today}")
            spread = 0.01 if m["code"] == "FX" else 0.02
            new_val = round(m["value"] * (1 + rnd.uniform(-spread, spread)), 2)
            db.execute("UPDATE market_index SET prev_value=?, value=?, updated_day=? WHERE code=?",
                       (m["value"], new_val, today, m["code"]))


def get_stocks():
    """모의 투자 종목 목록 (전일 대비 등락 포함)."""
    with get_db() as db:
        rows = db.execute("SELECT * FROM stocks ORDER BY market, name").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["change_pct"] = round((d["price"] - d["prev_price"]) / d["prev_price"] * 100, 2) if d["prev_price"] else 0.0
        out.append(d)
    return out


def get_stock(code):
    with get_db() as db:
        r = db.execute("SELECT * FROM stocks WHERE code=?", (code,)).fetchone()
    return dict(r) if r else None


def get_market_index():
    with get_db() as db:
        rows = db.execute("SELECT * FROM market_index").fetchall()
    out = {}
    for r in rows:
        d = dict(r)
        d["change_pct"] = round((d["value"] - d["prev_value"]) / d["prev_value"] * 100, 2) if d["prev_value"] else 0.0
        out[d["code"]] = d
    return out


def get_holdings(user_id):
    """보유 종목 (평가금액·평가손익 포함)."""
    with get_db() as db:
        rows = db.execute(
            "SELECT h.*, s.name, s.market, s.price FROM stock_holdings h "
            "JOIN stocks s ON s.code=h.code WHERE h.user_id=? AND h.shares>0", (user_id,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["value"] = round(d["shares"] * d["price"])
        d["cost"] = round(d["shares"] * d["avg_cost"])
        d["pl"] = d["value"] - d["cost"]
        d["pl_pct"] = round(d["pl"] / d["cost"] * 100, 2) if d["cost"] else 0.0
        out.append(d)
    return out


def buy_stock(user_id, code, points, won_per_point=10):
    """포인트로 모의 주식을 매수. 포인트 잔액에서 바로 차감하고, 보유 수량에 소수점 주식으로 반영."""
    if points <= 0:
        return False, "매수할 포인트를 확인하세요.", None
    stock = get_stock(code)
    if not stock:
        return False, "존재하지 않는 종목입니다.", None
    won = points * won_per_point
    shares = won / stock["price"]

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "UPDATE users SET points=points-? WHERE id=? AND points>=?",
            (points, user_id, points))
        if cur.rowcount == 0:
            conn.execute("ROLLBACK")
            bal = conn.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()["points"]
            return False, "포인트가 부족합니다.", bal

        existing = conn.execute(
            "SELECT * FROM stock_holdings WHERE user_id=? AND code=?", (user_id, code)).fetchone()
        if existing:
            new_shares = existing["shares"] + shares
            new_cost = (existing["shares"] * existing["avg_cost"] + won) / new_shares
            conn.execute("UPDATE stock_holdings SET shares=?, avg_cost=? WHERE id=?",
                         (new_shares, new_cost, existing["id"]))
        else:
            conn.execute(
                "INSERT INTO stock_holdings(user_id,code,shares,avg_cost) VALUES(?,?,?,?)",
                (user_id, code, shares, stock["price"]))

        conn.execute(
            "INSERT INTO point_log(user_id,delta,reason,label,day,created_at) VALUES(?,?,?,?,?,?)",
            (user_id, -points, "stock-buy", f"{stock['name']} {shares:.4f}주 매수", _today(), _now()))
        new_points = conn.execute("SELECT points FROM users WHERE id=?", (user_id,)).fetchone()["points"]
        conn.execute("COMMIT")
        return True, f"{stock['name']} {shares:.4f}주를 매수했어요.", new_points
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def submit_prediction(user_id, code, guess):
    """오늘의 주가 예측(상승/하락) 제출. 종목당 하루 1회, 다음 장마감 때 정산된다."""
    if guess not in ("up", "down"):
        return False, "잘못된 예측입니다."
    with get_db() as db:
        dup = db.execute(
            "SELECT 1 FROM stock_predictions WHERE user_id=? AND code=? AND day=?",
            (user_id, code, _today())).fetchone()
        if dup:
            return False, "오늘은 이미 이 종목을 예측했어요."
        db.execute(
            "INSERT INTO stock_predictions(user_id,code,day,guess,created_at) VALUES(?,?,?,?,?)",
            (user_id, code, _today(), guess, _now()))
    return True, "예측을 제출했어요. 다음 장마감 때 결과를 확인하세요."


def get_predictions_today(user_id):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM stock_predictions WHERE user_id=? AND day=?",
            (user_id, _today())).fetchall()
    return {r["code"]: r["guess"] for r in rows}


def get_recent_predictions(user_id, limit=10):
    with get_db() as db:
        rows = db.execute(
            "SELECT p.*, s.name FROM stock_predictions p JOIN stocks s ON s.code=p.code "
            "WHERE p.user_id=? AND p.resolved=1 ORDER BY p.id DESC LIMIT ?",
            (user_id, limit)).fetchall()
    return [dict(r) for r in rows]


# --------------------------- 챗봇 프롬프트 매니저 ---------------------------
def ensure_prompt(key, label, default_content):
    """프롬프트가 아직 없으면 기본값으로 등록(코드의 fallback 문구를 최초 시드로 사용).
    이미 있으면 건드리지 않고 현재 저장된 값을 그대로 반환한다."""
    with get_db() as db:
        db.execute(
            "INSERT OR IGNORE INTO prompts(key,label,content,updated_at) VALUES(?,?,?,?)",
            (key, label, default_content, _now()))
        r = db.execute("SELECT content FROM prompts WHERE key=?", (key,)).fetchone()
    return r["content"] if r else default_content


def get_prompt(key, default=""):
    with get_db() as db:
        r = db.execute("SELECT content FROM prompts WHERE key=?", (key,)).fetchone()
    return r["content"] if r else default


def set_prompt(key, content):
    with get_db() as db:
        db.execute("UPDATE prompts SET content=?, updated_at=? WHERE key=?",
                   (content, _now(), key))


def get_all_prompts():
    with get_db() as db:
        rows = db.execute("SELECT * FROM prompts ORDER BY key").fetchall()
    return [dict(r) for r in rows]


def ensure_variable(name, default_value):
    """변수가 아직 없으면 기본값으로 등록. 이미 있으면 저장된 값을 그대로 반환."""
    with get_db() as db:
        db.execute(
            "INSERT OR IGNORE INTO prompt_variables(name,value,updated_at) VALUES(?,?,?)",
            (name, default_value, _now()))
        r = db.execute("SELECT value FROM prompt_variables WHERE name=?", (name,)).fetchone()
    return r["value"] if r else default_value


def set_variable(name, value):
    with get_db() as db:
        db.execute(
            "INSERT INTO prompt_variables(name,value,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (name, value, _now()))


def get_all_variables():
    with get_db() as db:
        rows = db.execute("SELECT * FROM prompt_variables ORDER BY name").fetchall()
    return [dict(r) for r in rows]


def render_prompt(content):
    """프롬프트 안의 {{변수명}}을 저장된 변수값으로 치환."""
    with get_db() as db:
        rows = db.execute("SELECT * FROM prompt_variables").fetchall()
    for r in rows:
        content = content.replace("{{" + r["name"] + "}}", r["value"])
    return content


def add_agent_log(domain, route="admin", source="web", user_id=None, ok=True, elapsed_ms=0,
                  question="", scenario_label="", answer_chars=0, error="", created_at=None):
    """에이전트 실행 로그를 영속 저장한다."""
    created_at = created_at or _now()
    with get_db() as db:
        db.execute(
            "INSERT INTO agent_logs(domain,route,source,user_id,ok,elapsed_ms,question,"
            "scenario_label,answer_chars,error,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (domain, route, source, user_id, 1 if ok else 0, float(elapsed_ms or 0),
             question, scenario_label, int(answer_chars or 0), error, created_at))


def recent_agent_logs(limit=50):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM agent_logs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def agent_logs_in_window(minutes=60):
    cutoff = (datetime.now() - timedelta(minutes=int(minutes))).strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM agent_logs WHERE created_at >= ? ORDER BY id DESC",
            (cutoff,)).fetchall()
    return [dict(r) for r in rows]


# --------------------------- 관리자 통계 ---------------------------
def admin_stats():
    """관리자 대시보드용 DB 현황 요약."""
    with get_db() as db:
        user_count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_balance = db.execute(
            "SELECT COALESCE(SUM(balance),0) FROM accounts WHERE acct_type='main'").fetchone()[0]
        transaction_count = db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        active_subscription_count = db.execute(
            "SELECT COUNT(*) FROM subscriptions WHERE status='가입'").fetchone()[0]
        net_points_issued = db.execute(
            "SELECT COALESCE(SUM(delta),0) FROM point_log").fetchone()[0]
    return {
        "user_count": user_count,
        "total_balance": total_balance,
        "transaction_count": transaction_count,
        "active_subscription_count": active_subscription_count,
        "net_points_issued": net_points_issued,
    }


def recent_transactions(limit=20):
    """관리자 대시보드용: 전체 회원의 최근 거래내역(누구 계좌인지 포함)."""
    with get_db() as db:
        rows = db.execute(
            "SELECT t.created_at, t.kind, t.counterpart, t.amount, t.balance_after, "
            "       u.name AS user_name, a.account_no "
            "FROM transactions t "
            "JOIN accounts a ON a.id = t.account_id "
            "JOIN users u ON u.id = a.user_id "
            "ORDER BY t.id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
    print("DB 초기화 완료:", DB_PATH)
