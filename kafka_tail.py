# -*- coding: utf-8 -*-
"""
카푸카 토픽 내용을 처음부터 읽어 출력 (확인용).

사용:
  python kafka_tail.py                # bank-transfers 전체 출력 후 종료
  python kafka_tail.py bank-consult   # 다른 토픽
  python kafka_tail.py bank-transfers follow   # 계속 대기하며 실시간 출력
"""
import sys
import json
from kafka import KafkaConsumer

topic = sys.argv[1] if len(sys.argv) > 1 else "bank-transfers"
follow = len(sys.argv) > 2 and sys.argv[2] == "follow"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,  # 그룹 없이 → 항상 처음부터 전체를 읽음
    consumer_timeout_ms=(None if follow else 3000),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"[tail] 토픽 '{topic}' 읽는 중{' (follow)' if follow else ''}...\n")
n = 0
for m in consumer:
    n += 1
    print(f"#{n} key={m.key.decode() if m.key else None}")
    print("   " + json.dumps(m.value, ensure_ascii=False))
print(f"\n[tail] 총 {n}건.")
consumer.close()
