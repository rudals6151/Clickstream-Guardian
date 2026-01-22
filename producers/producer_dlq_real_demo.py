"""
REAL DLQ DEMO
=============

목적:
- "클릭 토픽으로 보내려다 실패 → DLQ로 격리" 되는
  실제 운영과 동일한 흐름을 데모로 보여주기 위함

----------------------------------------------------
실행 방법 (중요)
----------------------------------------------------

1) (의도적 실패 준비)
   Avro schema 경로를 일부러 깨뜨린다

   예:
     mv schemas/click-event.avsc schemas/click-event.avsc1

2) DLQ 토픽 초기화 (선택)
   docker exec kafka-1 kafka-topics \
     --bootstrap-server kafka-1:29092 \
     --delete \
     --topic km.events.dlq.v1

   docker exec kafka-1 kafka-topics \
     --bootstrap-server kafka-1:29092 \
     --create \
     --topic km.events.dlq.v1 \
     --partitions 3 \
     --replication-factor 1

3) 실행
   python producer_dlq_real_demo.py

4) DLQ 메시지 확인
   docker exec kafka-1 kafka-console-consumer \
     --bootstrap-server kafka-1:29092 \
     --topic km.events.dlq.v1 \
     --from-beginning \
     --max-messages 5
"""

import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

from common.config import Config
from common.schema_registry import SchemaRegistry


def main():
    clicks_topic = Config.CLICKS_TOPIC
    dlq_topic = Config.DLQ_TOPIC

    # 메인 Producer (클릭 토픽용)
    producer = Producer({
        "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
    })

    # DLQ Producer
    dlq_producer = Producer({
        "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
    })

    # Schema Registry (의도적으로 실패 유도)
    schema_registry = SchemaRegistry(Config.SCHEMA_REGISTRY_URL)

    # 일부러 존재하지 않거나 깨진 경로
    broken_schema_path = "../schemas/click-event.avsc1"

    print("[INFO] Starting REAL DLQ demo (forced serialize failure)")

    for i in range(5):
        # 정상 이벤트처럼 보이는 클릭 이벤트
        event = {
            "session_id": 900000 + i,
            "event_ts": int(time.time() * 1000),
            "item_id": 1000 + i,
            "category": None,
            "event_type": "click",
        }

        key = str(event["session_id"]).encode("utf-8")

        try:
            # 여기서 무조건 실패해야 함
            value = schema_registry.serialize(
                broken_schema_path,
                event,
                clicks_topic,
            )

            # (실제로는 여기까지 오면 안 됨)
            producer.produce(
                topic=clicks_topic,
                key=key,
                value=value,
            )

        except Exception as e:
            # 실패 → DLQ로 격리
            dlq_message = {
                "producer": "real-dlq-demo",
                "source_topic": clicks_topic,
                "stage": "serialize",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "original_data": event,
                "event": event,
            }

            dlq_producer.produce(
                topic=dlq_topic,
                key=key,
                value=json.dumps(dlq_message, ensure_ascii=False).encode("utf-8"),
            )

            print(f"[DLQ] event {i} sent to DLQ")

        time.sleep(0.1)

    producer.flush(5)
    dlq_producer.flush(5)

    print("[OK] REAL DLQ demo finished (5 events → DLQ)")


if __name__ == "__main__":
    main()
