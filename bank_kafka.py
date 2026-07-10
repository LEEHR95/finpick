# -*- coding: utf-8 -*-
"""
FinPick 카푸카(Kafka) 연동 — 이체 이벤트 발행 + 상담채팅 메시지 파이프.

kafChat(Node)과 '같은 카푸카 브로커'를 공유한다. 언어는 달라도 우체국(브로커)이
같아서 서로의 토픽에 편지를 넣고 꺼낼 수 있다.

토픽:
  bank-transfers : 이체가 일어날 때마다 '거래 이벤트'가 쌓임 (감사로그·실시간 알림용)
  bank-consult   : 실시간 상담채팅 메시지 (사람/봇 대화)

설계 원칙:
  - 카푸카가 꺼져 있거나 느려도 '이체 자체'는 절대 막지 않는다.
    (producer 는 지연 로딩 + 발행 실패는 조용히 로그만 남김)
"""
import os
import re
import json
import uuid
import atexit
import threading
import time
from datetime import datetime

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
except Exception:  # 라이브러리 미설치 등 — 연동 없이도 앱은 돌아가야 함
    KafkaProducer = None
    KafkaConsumer = None
    KafkaError = Exception

BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_TRANSFERS = os.environ.get("KAFKA_TOPIC_TRANSFERS", "bank-transfers")
TOPIC_CONSULT = os.environ.get("KAFKA_TOPIC_CONSULT", "bank-consult")
TOPIC_TRANSFER_REQ = os.environ.get("KAFKA_TOPIC_TRANSFER_REQ", "bank-transfer-requests")

_producer = None
_producer_lock = threading.Lock()
_producer_failed = False   # 한 번 연결에 실패하면 매 요청마다 재시도하지 않는다
_runtime_lock = threading.Lock()
_worker_threads = {}
_worker_runtime = {
    "consult-consumer": {"label": "상담 consumer", "topic": TOPIC_CONSULT,
                          "alive": False, "started_at": None, "last_seen": None,
                          "processed": 0, "restarts": 0, "last_error": None, "status": "idle"},
    "transfer-consumer": {"label": "이체 요청 consumer", "topic": TOPIC_TRANSFER_REQ,
                           "alive": False, "started_at": None, "last_seen": None,
                           "processed": 0, "restarts": 0, "last_error": None, "status": "idle"},
    "transfer-indexer": {"label": "이체 색인 consumer", "topic": TOPIC_TRANSFERS,
                          "alive": False, "started_at": None, "last_seen": None,
                          "processed": 0, "restarts": 0, "last_error": None, "status": "idle"},
}
_topic_runtime = {}


def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _ensure_topic_runtime(topic):
    return _topic_runtime.setdefault(topic, {
        "published_ok": 0,
        "published_fail": 0,
        "consumed": 0,
        "last_published_ts": None,
        "last_consumed_ts": None,
        "last_error": None,
    })


def _mark_publish(topic, ok, error=None):
    with _runtime_lock:
        rt = _ensure_topic_runtime(topic)
        if ok:
            rt["published_ok"] += 1
            rt["last_published_ts"] = _now_iso()
        else:
            rt["published_fail"] += 1
            rt["last_error"] = str(error)[:180] if error else rt.get("last_error")


def _mark_consume(topic, worker_name):
    with _runtime_lock:
        rt = _ensure_topic_runtime(topic)
        rt["consumed"] += 1
        rt["last_consumed_ts"] = _now_iso()
        wk = _worker_runtime.get(worker_name)
        if wk:
            wk["processed"] += 1
            wk["last_seen"] = _now_iso()
            wk["alive"] = True
            wk["status"] = "running"


def _mark_worker(worker_name, *, alive=None, status=None, error=None, restart=False, started=False):
    with _runtime_lock:
        wk = _worker_runtime.setdefault(worker_name, {
            "label": worker_name, "topic": None, "alive": False, "started_at": None,
            "last_seen": None, "processed": 0, "restarts": 0, "last_error": None, "status": "idle"
        })
        if alive is not None:
            wk["alive"] = alive
        if status is not None:
            wk["status"] = status
        if started and not wk.get("started_at"):
            wk["started_at"] = _now_iso()
        if restart:
            wk["restarts"] += 1
        if error:
            wk["last_error"] = str(error)[:180]
        wk["last_seen"] = _now_iso()


def kafka_runtime_metrics():
    with _runtime_lock:
        topics = []
        for t in TOPIC_INFO:
            rt = _ensure_topic_runtime(t["name"]).copy()
            rt["topic"] = t["name"]
            rt["purpose"] = t["purpose"]
            topics.append(rt)
        workers = []
        for name, meta in _worker_runtime.items():
            row = dict(meta)
            th = _worker_threads.get(name)
            row["alive"] = bool(th and th.is_alive()) if th is not None else bool(row.get("alive"))
            row["name"] = name
            workers.append(row)
        summary = {
            "workers_total": len(workers),
            "workers_alive": sum(1 for w in workers if w.get("alive")),
            "published_fail_total": sum(t["published_fail"] for t in topics),
            "consumed_total": sum(t["consumed"] for t in topics),
        }
    return {"topics": topics, "workers": workers, "summary": summary}


def get_producer():
    """카푸카 producer 지연 생성(싱글턴). 실패하면 None 을 돌려주고 앱은 계속 간다."""
    global _producer, _producer_failed
    if _producer is not None or _producer_failed or KafkaProducer is None:
        return _producer
    with _producer_lock:
        if _producer is not None or _producer_failed:
            return _producer
        try:
            _producer = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: (k or "").encode("utf-8"),
                acks=1,
                retries=3,
                linger_ms=20,
                request_timeout_ms=4000,
                max_block_ms=3000,   # 브로커가 죽어 있어도 3초 이상 이체를 막지 않는다
            )
            print(f"[kafka] producer 연결됨 → {BROKER}")
        except Exception as e:
            _producer_failed = True
            print(f"[kafka] producer 연결 실패(무시하고 계속): {e}")
    return _producer


TOPIC_INFO = [
    {"name": TOPIC_TRANSFERS, "purpose": "이체 감사로그 (성공/실패 이벤트, 소비자 없음·tail용)"},
    {"name": TOPIC_CONSULT, "purpose": "실시간 상담채팅 메시지 (사람/봇 대화)"},
    {"name": TOPIC_TRANSFER_REQ, "purpose": "비동기 이체 요청 큐 (consumer group: finpick-transfer-workers)"},
]


def kafka_status():
    """관리자 대시보드용 카프카 연결 상태 요약. 실패해도 예외를 던지지 않는다."""
    try:
        get_producer()  # 아직 시도 안 했으면 여기서 연결을 한 번 시도(최대 3초)
        return {
            "broker": BROKER,
            "connected": _producer is not None and not _producer_failed,
            "topics": TOPIC_INFO,
        }
    except Exception as e:
        return {"broker": BROKER, "connected": False, "topics": TOPIC_INFO, "error": str(e)}


# 이체 요청 큐의 컨슈머 그룹(랙 측정 대상). start_transfer_request_consumer와 동일해야 함.
TRANSFER_GROUP = "finpick-transfer-workers"


def kafka_topic_metrics():
    """토픽별 메시지 수(실측: end offset − begin offset 합)와 이체요청 큐 컨슈머 랙(밀린 건수).

    카프카 admin/consumer API 실측이라, 브로커가 느리거나 꺼져 있으면 예외 없이 부분값을 돌려준다.
    관리자 페이지가 느려지지 않게 bank_web에서 짧게 캐시해 호출한다.
    """
    result = {"topics": [], "group": TRANSFER_GROUP, "lag": None, "connected": False}
    if KafkaConsumer is None:
        return result
    try:
        from kafka import TopicPartition
    except Exception:
        return result

    consumer = None
    end_by_tp = {}
    try:
        consumer = KafkaConsumer(bootstrap_servers=BROKER, request_timeout_ms=4000,
                                 consumer_timeout_ms=4000, api_version_auto_timeout_ms=4000)
        purpose = {t["name"]: t["purpose"] for t in TOPIC_INFO}
        for topic in [TOPIC_TRANSFERS, TOPIC_CONSULT, TOPIC_TRANSFER_REQ]:
            parts = consumer.partitions_for_topic(topic)
            if not parts:
                result["topics"].append({"topic": topic, "messages": 0,
                                         "purpose": purpose.get(topic, "")})
                continue
            tps = [TopicPartition(topic, p) for p in parts]
            end = consumer.end_offsets(tps)
            beg = consumer.beginning_offsets(tps)
            end_by_tp.update(end)
            msgs = sum(end[tp] - beg[tp] for tp in tps)
            result["topics"].append({"topic": topic, "messages": int(msgs),
                                     "partitions": len(tps), "purpose": purpose.get(topic, "")})
        result["connected"] = True
    except Exception as e:
        result["error"] = str(e)
    finally:
        if consumer is not None:
            try:
                consumer.close(autocommit=False)
            except Exception:
                pass

    # 이체요청 큐 컨슈머 랙 = 큐에 쌓인 끝 오프셋 − 그룹이 커밋한 오프셋
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=BROKER, request_timeout_ms=4000)
        try:
            committed = admin.list_consumer_group_offsets(TRANSFER_GROUP)
            lag = 0
            for tp, meta in (committed or {}).items():
                end = end_by_tp.get(tp)
                if end is not None and meta is not None and meta.offset >= 0:
                    lag += max(0, end - meta.offset)
            result["lag"] = int(lag)
        finally:
            admin.close()
    except Exception:
        pass

    return result


def _publish(topic, key, event):
    """공통 발행. 실패해도 예외를 호출자에게 던지지 않는다(이체를 막지 않기 위함)."""
    prod = get_producer()
    if prod is None:
        _mark_publish(topic, False, "producer-unavailable")
        return False
    try:
        prod.send(topic, key=str(key), value=event)
        _mark_publish(topic, True)
        return True
    except KafkaError as e:
        print(f"[kafka] 발행 실패({topic}): {e}")
        _mark_publish(topic, False, e)
        return False
    except Exception as e:
        print(f"[kafka] 발행 오류({topic}): {e}")
        _mark_publish(topic, False, e)
        return False


def _mask_acct(no):
    """계좌번호 중간 자리를 가려 이벤트에 남긴다(원본은 to_account에 유지)."""
    m = re.match(r"^(\d{3}-\d{3}-)(\d{6})$", no or "")
    return f"{m.group(1)}***{m.group(2)[-3:]}" if m else no


def _amount_band(amount):
    """금액 구간 라벨(대시보드 집계·이상탐지용)."""
    if amount >= 10_000_000:
        return "초고액(1천만↑)"
    if amount >= 1_000_000:
        return "고액(100만↑)"
    if amount >= 100_000:
        return "중액(10만↑)"
    return "소액(10만↓)"


def publish_transfer(*, user_id, from_account, to_bank, to_account,
                     amount, ok, kind, message, memo="",
                     user_name=None, from_bank="FinPick", balance_after=None,
                     recipient_name=None, channel="web", async_mode=False,
                     client_ip=None, user_agent=None, fee=0):
    """이체 시도 결과를 'bank-transfers' 토픽에 이벤트로 발행.

    kind: internal(본행) / external(타행) / point-move(포인트머니 이동) 등 구분용 라벨.
    ok  : 이체 성공 여부(실패 이벤트도 남겨 감사/이상탐지에 쓸 수 있게 함).

    감사·이상탐지·키바나 대시보드에서 쓰기 좋게 필드를 풍부하게 담는다
    (고유ID·금액구간·시간대·채널·접속정보·잔액 등).
    """
    ts = _now_iso()                              # 'YYYY-MM-DDTHH:MM:SS'
    date_part = ts.split("T")[0]
    time_part = ts.split("T")[1] if "T" in ts else ""
    try:
        hour = int(time_part[:2]) if time_part else None
    except ValueError:
        hour = None
    event = {
        "event_id": str(uuid.uuid4()),           # 이벤트 추적/중복제거용 고유 ID
        "type": "transfer",
        "kind": kind,
        "status": "success" if ok else "failed",
        "ok": bool(ok),
        # 보낸/받는 주체
        "user_id": user_id,
        "user_name": user_name,
        "from_bank": from_bank,
        "from_account": from_account,
        "to_bank": to_bank,
        "to_account": to_account,
        "to_account_masked": _mask_acct(to_account),
        "recipient_name": recipient_name,
        "is_external": to_bank != "FinPick",
        # 금액
        "amount": amount,
        "fee": fee,
        "amount_band": _amount_band(amount),
        "currency": "KRW",
        "balance_after": balance_after,
        # 처리 경로/맥락
        "channel": channel,                      # web 등
        "async": bool(async_mode),               # 비동기(카프카 큐) 처리 여부
        "message": message,
        "memo": memo,
        # 접속 정보(이상탐지·감사)
        "client_ip": client_ip,
        "user_agent": user_agent,
        # 시간(키바나 필터·집계용)
        "ts": ts,
        "date": date_part,
        "hour": hour,
    }
    # key=user_id → 같은 사용자의 거래는 순서가 보장됨
    return _publish(TOPIC_TRANSFERS, user_id, event)


def publish_consult(msg: dict):
    """상담채팅 메시지를 'bank-consult' 토픽에 발행. key=channel 로 방별 순서 보장."""
    return _publish(TOPIC_CONSULT, msg.get("channel", "lobby"), msg)


# --------------------------- 비동기 이체 요청 큐 ---------------------------
def publish_transfer_request(req: dict):
    """이체 '요청(명령)'을 큐 토픽에 발행. 실제 처리는 백그라운드 consumer가 순차 수행.
    key=user_id 로 같은 사용자의 요청 순서를 보장한다."""
    return _publish(TOPIC_TRANSFER_REQ, req.get("user_id"), req)


_processed_req_ids = set()  # 한 프로세스 실행 내 중복 처리 방지(추가 안전장치)


def start_transfer_request_consumer(handler):
    """'bank-transfer-requests' 를 소비해 이체를 순차 실행(별도 스레드).

    상담 consumer 와 결정적으로 다른 점: 이체는 '재처리=이중이체'라 절대 replay 하면 안 된다.
      - 안정적 group_id + 수동 오프셋 커밋 → '새 요청만 정확히 1회' 처리
      - auto_offset_reset='latest' → 최초 구독 시 밀려 있던 과거 요청을 실행하지 않음
      - 처리 완료 후에만 commit, 요청 id 중복 방지까지 이중 안전장치
    """
    if KafkaConsumer is None:
        print("[kafka] 이체 consumer 불가(kafka 라이브러리 없음) — 비동기 이체 비활성")
        return

    def _run():
        while True:
            consumer = None
            try:
                _mark_worker("transfer-consumer", alive=True, status="connecting", restart=True, started=True)
                consumer = KafkaConsumer(
                    TOPIC_TRANSFER_REQ,
                    bootstrap_servers=BROKER,
                    group_id="finpick-transfer-workers",
                    enable_auto_commit=False,           # 처리 후 수동 커밋
                    auto_offset_reset="latest",         # 최초엔 새 요청만
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=-1,
                    request_timeout_ms=15000,
                    api_version_auto_timeout_ms=4000,
                )
                print(f"[kafka] 이체 요청 consumer 시작 → {TOPIC_TRANSFER_REQ}")
                _mark_worker("transfer-consumer", alive=True, status="running", started=True)
                for m in consumer:
                    req = m.value if isinstance(m.value, dict) else {}
                    rid = req.get("id")
                    try:
                        _mark_consume(TOPIC_TRANSFER_REQ, "transfer-consumer")
                        if rid and rid in _processed_req_ids:
                            consumer.commit()             # 이미 처리한 요청 → 건너뜀
                            continue
                        handler(req)                       # 실제 이체 실행(handler는 예외를 삼킴)
                        if rid:
                            _processed_req_ids.add(rid)
                    except Exception as e:
                        print(f"[kafka] 이체 요청 처리 오류(스킵): {e}")
                    finally:
                        try:
                            consumer.commit()              # 처리했든 실패했든 재실행 방지
                        except Exception:
                            pass
            except Exception as e:
                print(f"[kafka] 이체 consumer 중단, 5초 후 재시도: {e}")
                _mark_worker("transfer-consumer", alive=False, status="error", error=e)
            finally:
                if consumer is not None:
                    try:
                        consumer.close(autocommit=False)
                    except Exception:
                        pass
                _mark_worker("transfer-consumer", alive=False, status="retry-wait")
            time.sleep(5)

    t = threading.Thread(target=_run, name="transfer-consumer", daemon=True)
    _worker_threads["transfer-consumer"] = t
    t.start()
    return t


def start_transfer_indexer(handler):
    """'bank-transfers'(감사로그)를 소비해 각 이벤트를 handler(event, doc_id)로 넘긴다.
    handler가 Elasticsearch에 색인 → 키바나에서 이체 내역을 조회/집계할 수 있게 하는 다리.

    감사로그라 재색인(replay)해도 문제없다: doc_id로 멱등 색인하고, earliest로 과거 이벤트까지
    백필한다(이체 '실행' consumer와 달리 여기선 재처리가 이중이체를 유발하지 않음).
    """
    if KafkaConsumer is None:
        print("[kafka] 이체 색인 consumer 불가(kafka 라이브러리 없음)")
        return

    def _run():
        while True:
            consumer = None
            try:
                _mark_worker("transfer-indexer", alive=True, status="connecting", restart=True, started=True)
                consumer = KafkaConsumer(
                    TOPIC_TRANSFERS,
                    bootstrap_servers=BROKER,
                    group_id="finpick-transfer-indexer",
                    enable_auto_commit=True,
                    auto_offset_reset="earliest",       # 기존 이벤트도 ES로 백필
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=-1,
                    request_timeout_ms=15000,
                    api_version_auto_timeout_ms=4000,
                )
                print(f"[kafka] 이체 색인 consumer 시작 → {TOPIC_TRANSFERS} → ES")
                _mark_worker("transfer-indexer", alive=True, status="running", started=True)
                for m in consumer:
                    ev = m.value if isinstance(m.value, dict) else {}
                    # 신규 이벤트는 event_id로, 구버전(없음)은 오프셋 기반 id로 멱등 색인
                    doc_id = ev.get("event_id") or f"{m.topic}-{m.partition}-{m.offset}"
                    try:
                        _mark_consume(TOPIC_TRANSFERS, "transfer-indexer")
                        handler(ev, doc_id)
                    except Exception as e:
                        print(f"[kafka] 이체 색인 오류(스킵): {e}")
            except Exception as e:
                print(f"[kafka] 이체 색인 consumer 중단, 5초 후 재시도: {e}")
                _mark_worker("transfer-indexer", alive=False, status="error", error=e)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                _mark_worker("transfer-indexer", alive=False, status="retry-wait")
            time.sleep(5)

    t = threading.Thread(target=_run, name="transfer-indexer", daemon=True)
    _worker_threads["transfer-indexer"] = t
    t.start()
    return t


# --------------------------- 상담채팅 소비자 + 히스토리 ---------------------------
HISTORY_LIMIT = int(os.environ.get("CONSULT_HISTORY_LIMIT", "100"))
_consult_history = {}          # {channel: [msg, ...]}  최근 대화 캐시(메모리)
_consult_history_lock = threading.Lock()
_consult_seen_ids = set()


def _push_history(msg):
    ch = msg.get("channel", "lobby")
    with _consult_history_lock:
        lst = _consult_history.setdefault(ch, [])
        lst.append(msg)
        if len(lst) > HISTORY_LIMIT:
            del lst[0]


def get_consult_history(channel):
    with _consult_history_lock:
        return list(_consult_history.get(channel, []))


def consult_status():
    """관리자 대시보드용 실시간 상담 현황. '활동'은 이 서버 프로세스가 켜진 이후 기준
    (별도 접속유지 추적이 없어 '지금 접속 중'과는 다름 — 화면에도 그렇게 표기)."""
    with _consult_history_lock:
        channels = [
            {"channel": ch, "message_count": len(msgs),
             "last_ts": msgs[-1]["ts"] if msgs else None}
            for ch, msgs in _consult_history.items()
        ]
        total_messages = sum(c["message_count"] for c in channels)
    channels.sort(key=lambda c: c["last_ts"] or "", reverse=True)
    return {"active_channels": len(channels), "total_messages": total_messages,
            "channels": channels[:10]}


def start_consult_consumer(on_message):
    """'bank-consult' 토픽을 처음부터 읽어 히스토리를 채우고, 새 메시지마다 on_message 콜백.

    kafChat 과 동일한 패턴: 서버가 켜질 때마다 새 group_id 로 fromBeginning 전체를 다시 읽어
    채널별 지난 대화를 복원한다. 별도 스레드에서 무한 루프로 돈다.
    """
    if KafkaConsumer is None:
        print("[kafka] consumer 불가(kafka 라이브러리 없음) — 상담채팅 비활성")
        return

    def _run():
        while True:
            consumer = None
            try:
                _mark_worker("consult-consumer", alive=True, status="connecting", restart=True, started=True)
                consumer = KafkaConsumer(
                    TOPIC_CONSULT,
                    bootstrap_servers=BROKER,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    group_id=None,  # 그룹 없음 → 항상 처음부터 전체 replay
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=-1,  # 계속 대기
                    request_timeout_ms=15000,
                    api_version_auto_timeout_ms=4000,
                )
                print(f"[kafka] 상담 consumer 시작 → {TOPIC_CONSULT}")
                _mark_worker("consult-consumer", alive=True, status="running", started=True)
                for m in consumer:
                    try:
                        msg = m.value
                        _mark_consume(TOPIC_CONSULT, "consult-consumer")
                        mid = msg.get("id") if isinstance(msg, dict) else None
                        with _consult_history_lock:
                            if mid and mid in _consult_seen_ids:
                                continue
                            if mid:
                                _consult_seen_ids.add(mid)
                        _push_history(msg)
                        on_message(msg)
                    except Exception as e:
                        print(f"[kafka] 상담 메시지 처리 오류: {e}")
            except Exception as e:
                print(f"[kafka] 상담 consumer 중단, 5초 후 재시도: {e}")
                _mark_worker("consult-consumer", alive=False, status="error", error=e)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                _mark_worker("consult-consumer", alive=False, status="retry-wait")
            time.sleep(5)

    t = threading.Thread(target=_run, name="consult-consumer", daemon=True)
    _worker_threads["consult-consumer"] = t
    t.start()
    return t


def _close():
    if _producer is not None:
        try:
            _producer.flush(timeout=3)
            _producer.close(timeout=3)
        except Exception:
            pass


atexit.register(_close)
