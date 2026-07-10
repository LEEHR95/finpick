# -*- coding: utf-8 -*-
"""
관리자 대시보드용 실측 시계열 수집기.

30초마다 실제 지표를 메모리 링버퍼(최근 1시간)에 쌓는다. 전부 실측치이며, 영속 저장은
하지 않으므로 서버를 재시작하면 초기화된다(그때부터 다시 쌓임).

수집 지표:
  cache_hit   : 구간(직전 샘플 대비) 캐시 히트율 %  — 누적 카운터의 델타로 계산
  cache_mem   : Redis 사용 메모리(MB)
  req_p95     : 최근 요청들의 응답시간 p95(ms)  — Flask 훅이 record_request로 적재
  req_rate    : 구간 요청 수(분당 환산은 화면에서)
  cpu_<컨테이너>: docker stats 실측 CPU %

이상 이벤트 로그(detect_incidents)도 지어내지 않고, 쌓인 실측 시계열에서 임계치를 넘긴
구간을 스캔해 만든다(없으면 '이상 없음').
"""
import threading
import time
import subprocess
from collections import deque

MAXLEN = 120            # 30초 × 120 = 최근 1시간
SAMPLE_INTERVAL = 30    # 초

CONTAINERS = [("phoenix", "피닉스"), ("redis-bank", "Redis"),
              ("es-bank", "Elasticsearch"), ("kafchat-kafka", "카프카")]

# 임계치(이상 이벤트 판정). 실측 시계열에 이 선을 넘긴 점이 있으면 이벤트로 기록.
THRESHOLDS = {
    "cache_hit": ("below", 40, "info", "캐시 히트율 40% 아래로 하락"),
    "req_p95":   ("above", 1500, "warning", "요청 응답시간 p95 1.5초 초과"),
}

_series = {}                    # metric -> deque[(ts, value)]
_lock = threading.Lock()

_req_durations = deque(maxlen=500)   # 최근 요청 소요(ms) 롤링 윈도우
_req_lock = threading.Lock()
_req_counter = 0                     # 구간 요청 수(샘플마다 리셋)

_prev = {"hits": None, "misses": None}
_started = False

AGENT_DOMAINS = ("customer", "fraud", "credit", "compliance")
AGENT_LABELS = {
    "customer": "고객",
    "fraud": "이상거래탐지",
    "credit": "여신심사",
    "compliance": "컴플라이언스",
}
_agent_logs = deque(maxlen=200)
_agent_history = deque(maxlen=1500)
_agent_interval = deque(maxlen=500)
_agent_lock = threading.Lock()


def _push(metric, value):
    with _lock:
        _series.setdefault(metric, deque(maxlen=MAXLEN)).append((time.time(), value))


def record_request(ms):
    """Flask after_request 훅에서 호출 — 요청 하나의 소요시간(ms)을 적재."""
    global _req_counter
    with _req_lock:
        _req_durations.append(ms)
        _req_counter += 1


def record_agent_call(domain, ok, elapsed_ms, route="admin", user_id=None, message="",
                      error=None, answer_chars=0):
    """에이전트 호출 결과를 최근 로그와 구간 집계 버퍼에 적재."""
    now = time.time()
    row = {
        "ts": now,
        "when": time.strftime("%m-%d %H:%M:%S", time.localtime(now)),
        "domain": domain,
        "label": AGENT_LABELS.get(domain, domain),
        "route": route,
        "user_id": user_id,
        "ok": bool(ok),
        "status": "success" if ok else "error",
        "elapsed_ms": round(elapsed_ms, 1),
        "message": (message or "").strip().replace("\n", " ")[:180],
        "error": (error or "").strip().replace("\n", " ")[:180],
        "answer_chars": int(answer_chars or 0),
    }
    with _agent_lock:
        _agent_logs.appendleft(row)
        _agent_history.append(row)
        _agent_interval.append(row)
    try:
        import bank_db
        bank_db.add_agent_log(domain, route=route, source="web", user_id=user_id, ok=ok,
                              elapsed_ms=elapsed_ms, question=message,
                              answer_chars=answer_chars, error=error or "")
    except Exception:
        pass


def _drain_request_stats():
    global _req_counter
    with _req_lock:
        durs = list(_req_durations)
        cnt = _req_counter
        _req_counter = 0
    return durs, cnt


def _drain_agent_stats():
    with _agent_lock:
        rows = list(_agent_interval)
        _agent_interval.clear()
    return rows


def _p95(values):
    if not values:
        return None
    vals = sorted(values)
    idx = min(len(vals) - 1, int(len(vals) * 0.95))
    return vals[idx]


def series(metric):
    """[{t: 'HH:MM', v: float}, ...] — 화면 렌더용."""
    with _lock:
        dq = _series.get(metric)
        if not dq:
            return []
        return [{"t": time.strftime("%H:%M", time.localtime(ts)), "v": round(v, 1)}
                for ts, v in dq]


def latest(metric):
    with _lock:
        dq = _series.get(metric)
        return round(dq[-1][1], 1) if dq else None


def _docker_cpu():
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}"],
            capture_output=True, text=True, timeout=6)
        d = {}
        for line in out.stdout.strip().splitlines():
            parts = line.split("|")
            if len(parts) == 2:
                d[parts[0]] = float(parts[1].strip("% \n"))
        return d
    except Exception:
        return {}


def _sample_once():
    import cache
    # 캐시 히트율(구간) + 메모리
    try:
        s = cache.stats()
        hits, misses = s.get("hits") or 0, s.get("misses") or 0
        if _prev["hits"] is not None:
            dh, dm = hits - _prev["hits"], misses - _prev["misses"]
            if dh + dm > 0:
                _push("cache_hit", dh / (dh + dm) * 100)
        _prev["hits"], _prev["misses"] = hits, misses
        mem = s.get("used_memory")  # 바이트(있으면)
        if mem:
            _push("cache_mem", mem / 1024 / 1024)
    except Exception:
        pass

    # 요청 p95 + 구간 처리량
    durs, cnt = _drain_request_stats()
    if durs:
        ds = sorted(durs)
        idx = min(len(ds) - 1, int(len(ds) * 0.95))
        _push("req_p95", ds[idx])
    _push("req_rate", cnt)

    # 에이전트 호출 구간 집계
    agent_rows = _drain_agent_stats()
    _push("agent_calls", len(agent_rows))
    for domain in AGENT_DOMAINS:
        domain_rows = [r for r in agent_rows if r["domain"] == domain]
        _push(f"agent_calls_{domain}", len(domain_rows))
    if agent_rows:
        ok_count = sum(1 for r in agent_rows if r["ok"])
        fail_count = len(agent_rows) - ok_count
        elapsed = [r["elapsed_ms"] for r in agent_rows]
        _push("agent_success_rate", ok_count / len(agent_rows) * 100)
        _push("agent_error_rate", fail_count / len(agent_rows) * 100)
        _push("agent_p95", _p95(elapsed))
        _push("agent_avg", sum(elapsed) / len(elapsed))
        for domain in AGENT_DOMAINS:
            domain_rows = [r for r in agent_rows if r["domain"] == domain]
            if not domain_rows:
                continue
            domain_ok = sum(1 for r in domain_rows if r["ok"])
            domain_fail = len(domain_rows) - domain_ok
            domain_elapsed = [r["elapsed_ms"] for r in domain_rows]
            _push(f"agent_success_rate_{domain}", domain_ok / len(domain_rows) * 100)
            _push(f"agent_error_rate_{domain}", domain_fail / len(domain_rows) * 100)
            _push(f"agent_p95_{domain}", _p95(domain_elapsed))
            _push(f"agent_avg_{domain}", sum(domain_elapsed) / len(domain_rows))

    # 컨테이너 CPU 실측
    cpu = _docker_cpu()
    for cid, _label in CONTAINERS:
        if cid in cpu:
            _push(f"cpu_{cid}", cpu[cid])

    # 카프카 토픽 유입량 / 랙 / 워커 상태
    try:
        import bank_kafka
        km = bank_kafka.kafka_topic_metrics()
        kr = bank_kafka.kafka_runtime_metrics()
        _push("kafka_lag", float(km.get("lag") or 0))
        _push("kafka_workers_alive", float(kr.get("summary", {}).get("workers_alive") or 0))
        topics = {t["topic"]: int(t.get("messages") or 0) for t in km.get("topics", [])}
        prev_topics = _prev.setdefault("kafka_topics", {})
        total_delta = 0
        for topic, current in topics.items():
            prev = prev_topics.get(topic)
            delta = max(0, current - prev) if prev is not None else 0
            _push(f"kafka_in_{topic.replace('-', '_')}", float(delta))
            total_delta += delta
            prev_topics[topic] = current
        _push("kafka_in_total", float(total_delta))
    except Exception:
        pass


def detect_incidents():
    """실측 시계열에서 임계치를 넘긴 구간을 이벤트로. 지어내지 않음(없으면 빈 리스트)."""
    events = []
    with _lock:
        for metric, (direction, limit, level, label) in THRESHOLDS.items():
            dq = _series.get(metric)
            if not dq:
                continue
            for ts, v in dq:
                breach = (v < limit) if direction == "below" else (v > limit)
                if breach:
                    events.append({"level": level, "message": label,
                                   "when": time.strftime("%m-%d %H:%M", time.localtime(ts)),
                                   "value": round(v, 1)})
                    break  # 지표당 최근 1건만
    events.sort(key=lambda e: e["when"], reverse=True)
    return events


def agent_snapshot(window_sec=3600, limit=25):
    """최근 구간의 에이전트 호출 요약과 최신 로그."""
    now = time.time()
    cutoff = now - window_sec
    with _agent_lock:
        history = [r for r in _agent_history if r["ts"] >= cutoff]
        recent = list(_agent_logs)[:limit]

    total = len(history)
    ok_count = sum(1 for r in history if r["ok"])
    fail_count = total - ok_count
    elapsed = [r["elapsed_ms"] for r in history]
    avg_ms = round(sum(elapsed) / total, 1) if total else None
    p95_ms = round(_p95(elapsed), 1) if elapsed else None
    success_rate = round(ok_count / total * 100, 1) if total else None
    answer_avg = round(sum(r["answer_chars"] for r in history) / total, 1) if total else None

    by_domain = []
    for domain in AGENT_DOMAINS:
        rows = [r for r in history if r["domain"] == domain]
        if rows:
            d_elapsed = [r["elapsed_ms"] for r in rows]
            d_ok = sum(1 for r in rows if r["ok"])
            d_fail = len(rows) - d_ok
            by_domain.append({
                "domain": domain,
                "label": AGENT_LABELS.get(domain, domain),
                "calls": len(rows),
                "ok": d_ok,
                "fail": d_fail,
                "success_rate": round(d_ok / len(rows) * 100, 1),
                "avg_ms": round(sum(d_elapsed) / len(rows), 1),
                "p95_ms": round(_p95(d_elapsed), 1),
                "share_pct": round(len(rows) / total * 100, 1) if total else 0,
                "last_when": rows[-1]["when"],
                "last_status": rows[-1]["status"],
                "last_user_id": rows[-1]["user_id"],
                "last_message": rows[-1]["message"],
            })
        else:
            by_domain.append({
                "domain": domain,
                "label": AGENT_LABELS.get(domain, domain),
                "calls": 0,
                "ok": 0,
                "fail": 0,
                "success_rate": None,
                "avg_ms": None,
                "p95_ms": None,
                "share_pct": 0,
                "last_when": None,
                "last_status": None,
                "last_user_id": None,
                "last_message": "",
            })

    route_customer = sum(1 for r in history if r["route"] == "customer")
    route_admin = sum(1 for r in history if r["route"] == "admin")
    route_total = route_customer + route_admin or 1
    route_split = [
        {"label": "고객", "count": route_customer, "pct": round(route_customer / route_total * 100, 1)},
        {"label": "관리자", "count": route_admin, "pct": round(route_admin / route_total * 100, 1)},
    ]

    incidents = []
    if success_rate is not None and success_rate < 90:
        incidents.append({"level": "warning", "message": "에이전트 성공률 90% 미만", "value": success_rate})
    if p95_ms is not None and p95_ms > 8000:
        incidents.append({"level": "warning", "message": "에이전트 p95 지연 8초 초과", "value": p95_ms})
    if fail_count:
        incidents.append({"level": "info", "message": "최근 1시간 내 에이전트 실패 발생", "value": fail_count})

    return {
        "summary": {
            "calls": total,
            "ok": ok_count,
            "fail": fail_count,
            "success_rate": success_rate,
            "avg_ms": avg_ms,
            "p95_ms": p95_ms,
            "answer_avg": answer_avg,
            "window_label": f"최근 {window_sec // 60}분",
            "last_when": recent[0]["when"] if recent else None,
        },
        "domains": by_domain,
        "recent": recent,
        "route_split": route_split,
        "incidents": incidents,
    }


def _loop():
    while True:
        try:
            _sample_once()
        except Exception as e:
            print("[monitoring] 샘플 실패:", e)
        time.sleep(SAMPLE_INTERVAL)


def start_sampler():
    """앱 로드 시 1회 호출. 시작 즉시 한 점을 찍고(첫 화면이 비지 않게) 백그라운드 루프 시작."""
    global _started
    if _started:
        return
    _started = True
    _sample_once()
    threading.Thread(target=_loop, name="metrics-sampler", daemon=True).start()
