"""
cd c:/Users/USER/Desktop/bootcamp/project/Clickstream-Guardian/producers
cd c:/Users/USER/Desktop/bootcamp/project/Clickstream-Guardian/producers
python producer_clicks.py --file ../data/yoochoose-clicks-sorted.dat --anomaly-interval 10
"""

import csv
import time
import json
import logging
import random
from datetime import datetime
from confluent_kafka import Producer

from common.config import Config
from common.schema_registry import SchemaRegistry

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _get_first(row: dict, keys: list[str]):
    """row에서 가능한 키들 중 첫 번째로 존재/값있는 것을 반환"""
    for k in keys:
        if k in row and row[k] not in (None, ''):
            return row[k]
    return None


class ImprovedClickProducer:
    """Enhanced producer with time-ordering and anomaly injection + DLQ (STREAMING)"""

    def __init__(self):
        self.topic = Config.CLICKS_TOPIC
        self.dlq_topic = Config.DLQ_TOPIC

        # Initialize Schema Registry
        self.schema_registry = SchemaRegistry(Config.SCHEMA_REGISTRY_URL)
        self.schema_path = '../schemas/click-event.avsc'

        # Initialize Kafka Producer
        producer_config = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            **Config.PRODUCER_CONFIG
        }
        self.producer = Producer(producer_config)

        # DLQ Producer (JSON format)
        self.dlq_producer = Producer({
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
        })

        # Statistics
        self.stats = {
            'sent': 0,
            'failed': 0,
            'dlq': 0,
            'anomalous': 0,
            'start_time': time.time(),
            'last_anomaly_time': time.time()
        }

        self._produced_calls = 0
        self._dlq_calls = 0

        self.replay_speed = Config.REPLAY_SPEED
        logger.info(f"🚀 ImprovedClickProducer initialized (replay_speed={self.replay_speed}x)")
        logger.info(f"   📊 Topic: {self.topic}")
        logger.info(f"   🧯 DLQ Topic: {self.dlq_topic}")
        logger.info(f"   🔗 Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")

    def parse_row(self, row):
        """
        Parse CSV row to event dictionary.
        원본 데이터 컬럼명이 케이스/이름이 다른 경우도 대비해서 키 fallback 포함.
        """
        try:
            ts_str = _get_first(row, ['Timestamp', 'timestamp', 'event_ts', 'EventTs', 'eventTs'])
            sid = _get_first(row, ['Session ID', 'session_id', 'sessionId', 'SessionID'])
            item = _get_first(row, ['Item ID', 'item_id', 'itemId', 'ItemID'])
            cat = _get_first(row, ['Category', 'category'])

            if ts_str is None or sid is None or item is None:
                raise KeyError(f"Missing required columns. row keys={list(row.keys())}")

            event_dt = datetime.fromisoformat(str(ts_str).replace('Z', '+00:00'))

            event = {
                'session_id': int(sid),
                'event_ts': int(event_dt.timestamp() * 1000),  # ms
                'item_id': int(item),
                'category': cat if cat not in ('0', 0, None, '') else None,
                'event_type': 'click'
            }
            return event

        except Exception as e:
            logger.error(f"❌ Parse error: {e}, row: {row}")
            return None

    def send_to_dlq(self, original, error_msg, stage, event=None):
        """Send failed rows/events to DLQ as JSON."""
        try:
            dlq_message = {
                'producer': 'improved-click-producer',
                'source_topic': self.topic,
                'stage': stage,
                'error': str(error_msg),
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'original_data': original,
                'event': event
            }

            key = None
            try:
                sid = None
                if isinstance(original, dict):
                    sid = original.get('Session ID') or original.get('session_id')
                if event and isinstance(event, dict):
                    sid = sid or event.get('session_id')
                if sid is not None:
                    key = str(sid).encode('utf-8')
            except Exception:
                key = None

            self.dlq_producer.produce(
                topic=self.dlq_topic,
                key=key,
                value=json.dumps(dlq_message, ensure_ascii=False).encode('utf-8')
            )

            self.stats['dlq'] += 1
            self._dlq_calls += 1
            if self._dlq_calls % 100 == 0:
                self.dlq_producer.poll(0)

        except Exception as e:
            logger.error(f"❌ Failed to send to DLQ: {e}")

    def generate_anomalous_session(self):
        """Generate anomalous click events with CURRENT timestamp"""
        anomaly_session_id = random.randint(1_000_000, 9_999_999)
        num_events = random.randint(50, 150)

        logger.warning(f"⚠️  Generating ANOMALOUS session {anomaly_session_id} with {num_events} events")

        current_ts = int(time.time() * 1000)

        events = []
        # CRITICAL FIX: 간격을 10ms로 줄여서 더 밀집된 이상 세션 생성
        # 50개 이벤트 = 500ms, 100개 = 1초 안에 모두 발생
        for i in range(num_events):
            event = {
                'session_id': anomaly_session_id,
                'event_ts': current_ts + i * 10,  # 100ms -> 10ms
                'item_id': random.randint(1, 1000),
                'category': random.choice(['Electronics', 'Fashion', 'Books', None]),
                'event_type': 'click'
            }
            events.append(event)

        self.stats['anomalous'] += num_events
        return events

    def delivery_report(self, err, msg):
        """Callback for message delivery reports (main topic)"""
        if err:
            self.stats['failed'] += 1
            logger.error(f"❌ Delivery failed: {err}")
        else:
            self.stats['sent'] += 1
            if self.stats['sent'] % 1000 == 0:
                elapsed = time.time() - self.stats['start_time']
                rate = self.stats['sent'] / elapsed if elapsed > 0 else 0
                logger.info(
                    f"📈 Progress: {self.stats['sent']:,} events "
                    f"({rate:.0f} msg/sec, {self.stats['anomalous']} anomalous, {self.stats['dlq']} dlq)"
                )

    def send_event(self, event, original_row=None):
        """Send a single event to Kafka (Avro). On failure -> DLQ."""
        try:
            key = str(event['session_id']).encode('utf-8')

            # Serialize with Avro
            try:
                value = self.schema_registry.serialize(
                    self.schema_path,
                    event,
                    self.topic
                )
            except Exception as e:
                logger.error(f"❌ Serialize failed: {e}")
                self.stats['failed'] += 1
                self.send_to_dlq(
                    original=original_row if original_row is not None else event,
                    error_msg=e,
                    stage="serialize",
                    event=event
                )
                return

            # Produce to Kafka
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value,
                    on_delivery=self.delivery_report
                )
                self._produced_calls += 1

                if self._produced_calls % 100 == 0:
                    self.producer.poll(0)

            except Exception as e:
                logger.error(f"❌ Produce failed: {e}")
                self.stats['failed'] += 1
                self.send_to_dlq(
                    original=original_row if original_row is not None else event,
                    error_msg=e,
                    stage="produce",
                    event=event
                )

        except Exception as e:
            logger.error(f"❌ Failed to send event (unexpected): {e}")
            self.stats['failed'] += 1
            self.send_to_dlq(
                original=original_row if original_row is not None else event,
                error_msg=e,
                stage="serialize_or_produce",
                event=event
            )

    def produce_with_anomalies(self, csv_path, max_events=None, anomaly_interval=60):
        """
        STREAMING producer:
        - 파일을 한 줄씩 읽으며 즉시 produce (대용량 로딩 제거)
        - 첫 이벤트 timestamp를 기준으로 time_offset을 계산해 현재 시간대로 shift
        - replay_speed는 '원본 timestamp 간격' 기준으로 sleep
        """
        logger.info("=" * 80)
        logger.info("🎬 Starting STREAMING producer with ANOMALY INJECTION + DLQ + CURRENT TIMESTAMPS")
        logger.info(f"   📁 File: {csv_path}")
        logger.info(f"   ⏱️  Anomaly interval: {anomaly_interval}s")
        logger.info("=" * 80)

        produced = 0
        first_event_ts = None
        time_offset = None
        prev_original_ts = None

        last_anomaly_real_time = time.time()

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # (중요) 첫 유효 이벤트를 만나면 offset 계산
            for row in reader:
                event = self.parse_row(row)
                if not event:
                    self.send_to_dlq(original=row, error_msg="Failed to parse row", stage="parse")
                    continue

                first_event_ts = event['event_ts']
                current_time_ms = int(time.time() * 1000)
                time_offset = current_time_ms - first_event_ts

                logger.info(f"⏰ Time offset: +{time_offset/1000:.0f} seconds (shifting to current time)")
                # 첫 이벤트도 처리해야 하니까, 아래 루프로 “첫 이벤트 포함” 처리
                # 현재 row/event를 루프 처리로 넘기기 위해 break 후 별도 처리
                first_row = row
                first_event = event
                break
            else:
                logger.error("❌ No valid events found in file!")
                return

            # ---- 첫 이벤트 처리 ----
            original_ts = first_event['event_ts']
            first_event['event_ts'] = original_ts + time_offset
            self.send_event(first_event, original_row=first_row)
            produced += 1
            prev_original_ts = original_ts

            # ---- 나머지 이벤트 스트리밍 처리 ----
            for row in reader:
                if max_events and produced >= max_events:
                    break

                event = self.parse_row(row)
                if not event:
                    self.send_to_dlq(original=row, error_msg="Failed to parse row", stage="parse")
                    continue

                # anomaly injection (real time 기준)
                now_rt = time.time()
                if now_rt - last_anomaly_real_time >= anomaly_interval:
                    logger.warning(f"\n{'='*80}")
                    logger.warning(f"⚠️  ANOMALY INJECTION at produced={produced}")
                    logger.warning(f"{'='*80}\n")

                    for anom_event in self.generate_anomalous_session():
                        self.send_event(anom_event, original_row=None)

                    last_anomaly_real_time = now_rt

                # time shift
                original_ts = event['event_ts']
                event['event_ts'] = original_ts + time_offset

                # send
                self.send_event(event, original_row=row)
                produced += 1

                # replay speed control (원본 간격 기준)
                if prev_original_ts is not None and self.replay_speed < 100:
                    delay = (original_ts - prev_original_ts) / 1000.0 / self.replay_speed
                    if delay > 0:
                        time.sleep(min(delay, 0.1))

                prev_original_ts = original_ts

        # Final flush
        logger.info("\n⏳ Flushing remaining messages...")
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"⚠️  {remaining} messages failed to flush (main producer)")

        dlq_remaining = self.dlq_producer.flush(timeout=30)
        if dlq_remaining > 0:
            logger.warning(f"⚠️  {dlq_remaining} messages failed to flush (DLQ producer)")

        # Final statistics
        elapsed = time.time() - self.stats['start_time']
        logger.info("\n" + "=" * 80)
        logger.info("✅ Producer finished!")
        logger.info(f"   📤 Sent (delivered): {self.stats['sent']:,} events")
        logger.info(f"   🧯 DLQ: {self.stats['dlq']:,} messages")
        logger.info(f"   ⚠️  Anomalous generated: {self.stats['anomalous']:,} events")
        logger.info(f"   ❌ Failed (local errors): {self.stats['failed']:,} events")
        logger.info(f"   ⏱️  Duration: {elapsed:.2f} seconds")
        logger.info(f"   📊 Avg Rate: {self.stats['sent'] / elapsed:.0f} msg/sec" if elapsed > 0 else "   📊 Avg Rate: N/A")
        logger.info("=" * 80)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Improved Click Events Producer (STREAMING)')
    parser.add_argument(
        '--file',
        default=Config.CLICKS_FILE,
        help='Path to clicks CSV file'
    )
    parser.add_argument(
        '--max-events',
        type=int,
        default=None,
        help='Maximum number of events to produce'
    )
    parser.add_argument(
        '--anomaly-interval',
        type=int,
        default=60,
        help='Inject anomaly every N seconds (default: 60)'
    )
    args = parser.parse_args()

    producer = ImprovedClickProducer()
    producer.produce_with_anomalies(
        args.file,
        args.max_events,
        args.anomaly_interval
    )


if __name__ == '__main__':
    main()
