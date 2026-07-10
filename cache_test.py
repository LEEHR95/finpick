# -*- coding: utf-8 -*-
"""캐시 기능 테스트: 같은 질문을 캐시 미스/히트로 각각 실행해 동작·속도 확인."""
import time
import cache

if __name__ != "__main__":
    import pytest
    pytest.skip("script-style cache benchmark; run directly with python cache_test.py", allow_module_level=True)

print("Redis PING:", cache.ping())
cache.flush()  # 깨끗한 상태에서 시작

query = "안전하게 목돈 굴리기 좋은 예금"

# 1) 캐시 미스 (Upstage 임베딩 호출)
t0 = time.perf_counter()
v1 = cache.cached_embed_query(query)
miss_ms = (time.perf_counter() - t0) * 1000
print(f"\n[1차 - 캐시 미스] {miss_ms:7.1f} ms  (Upstage 호출, 벡터 {len(v1)}차원)")

# 2) 캐시 히트 (Redis에서 즉시)
t0 = time.perf_counter()
v2 = cache.cached_embed_query(query)
hit_ms = (time.perf_counter() - t0) * 1000
print(f"[2차 - 캐시 히트] {hit_ms:7.1f} ms  (Redis에서 반환)")

print(f"\n결과 동일 여부: {v1 == v2}")
print(f"속도 향상     : {miss_ms / hit_ms:,.0f}배 빠름  ({miss_ms - hit_ms:,.1f} ms 절약)")
print("\n캐시 상태:", cache.stats())
