"""
Redis 캐시 레이어.

자주 반복되는 무거운 작업의 결과를 Redis에 저장해 재사용한다.
  - 질문 임베딩(Upstage API 호출)  → cached_embed_query
  - 검색 결과(ES 질의)            → cache_get/cache_set 로 직접 캐싱

연결 주소는 환경변수 REDIS_URL(기본 redis://localhost:6379/0)에서 읽는다.
캐시 정책(TTL 등)은 모듈 상단 상수로 관리한다.
"""

import os
import json
import time
import hashlib

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
_r = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=0.3,
    socket_timeout=0.3,
    retry_on_timeout=False,
)

# --------------------------- 캐시 정책 (3단계에서 조정) ---------------------------
TTL_EMBED = 60 * 60 * 24 * 7   # 질문 임베딩: 7일 (같은 질문은 의미 안 변함)
TTL_SEARCH = 60 * 60           # 검색 결과: 1시간 (금리 데이터 일 단위 갱신 고려)
KEY_PREFIX = "bankrag"          # 키 네임스페이스


def _swallow_redis_error(action, fallback):
    try:
        return action()
    except redis.RedisError:
        return fallback
    except OSError:
        return fallback


def ping():
    return _swallow_redis_error(lambda: _r.ping(), False)


def _key(kind, *parts):
    raw = "|".join(str(p) for p in parts)
    return f"{KEY_PREFIX}:{kind}:" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def cache_get(kind, *parts):
    """캐시 조회. 없으면 None."""
    v = _swallow_redis_error(lambda: _r.get(_key(kind, *parts)), None)
    return json.loads(v) if v is not None else None


def cache_set(kind, value, ttl, *parts):
    """캐시 저장 (ttl초)."""
    _swallow_redis_error(
        lambda: _r.set(_key(kind, *parts), json.dumps(value, ensure_ascii=False), ex=ttl),
        None,
    )


def cached_embed_query(text):
    """질문 임베딩을 Redis 캐시. (벡터 list 반환)
    캐시에 있으면 Upstage 호출 없이 즉시 반환."""
    hit = cache_get("embq", text)
    if hit is not None:
        return hit
    import rag_core
    vec = rag_core.embed_query(text).tolist()
    cache_set("embq", vec, TTL_EMBED, text)
    return vec


def cached_call(kind, ttl, producer, *key_parts):
    """범용 캐시 래퍼. 캐시에 있으면 (값, True[히트]) 반환, 없으면 producer() 실행 후 저장.
    검색 결과 등 무거운 호출을 한 줄로 캐싱할 때 사용."""
    hit = cache_get(kind, *key_parts)
    if hit is not None:
        return hit, True
    value = producer()
    cache_set(kind, value, ttl, *key_parts)
    return value, False


def flush_kind(kind):
    """특정 종류 캐시만 삭제 (예: 데이터 재적재 후 'search' 캐시 무효화)."""
    keys = _swallow_redis_error(lambda: list(_r.scan_iter(match=f"{KEY_PREFIX}:{kind}:*")), [])
    if keys:
        _swallow_redis_error(lambda: _r.delete(*keys), None)
    return len(keys)


def stats():
    """캐시 상태 요약."""
    info = _swallow_redis_error(lambda: _r.info(), None)
    if info is None:
        return {
            "keys": 0,
            "hits": None,
            "misses": None,
            "used_memory": None,
            "used_memory_human": None,
            "error": "redis unavailable",
        }
    return {
        "keys": _swallow_redis_error(lambda: _r.dbsize(), 0),
        "hits": info.get("keyspace_hits"),
        "misses": info.get("keyspace_misses"),
        "used_memory": info.get("used_memory"),          # 바이트 (시계열 수집용)
        "used_memory_human": info.get("used_memory_human"),
    }


def flush():
    """이 네임스페이스 키만 삭제 (테스트 초기화용)."""
    keys = _swallow_redis_error(lambda: list(_r.scan_iter(match=f"{KEY_PREFIX}:*")), [])
    if keys:
        _swallow_redis_error(lambda: _r.delete(*keys), None)
    return len(keys)
