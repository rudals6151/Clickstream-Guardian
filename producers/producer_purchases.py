"""
Kafka Producer for Purchase Events

Features:
    - Sorts events by timestamp for realistic replay
    - Enhanced logging for monitoring
    - DLQ for parse/serialize/produce failures
    - Stable poll cadence based on produce-call counts
    - Revenue tracking via delivery callbacks

Usage:
    python producer_purchases.py --file ../data/yoochoose-buys-sorted.dat
"""

import csv
import time
import json
import logging
import hashlib
from datetime import datetime, timezone
from confluent_kafka import Producer

from common.config import Config
from common.schema_registry import SchemaRegistry


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def make_event_id(session_id: int, item_id: int, event_ts: int, quantity: int, price: float) -> str:
    raw = f"{session_id}|{item_id}|{event_ts}|{quantity}|{price}|purchase"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class ImprovedPurchaseProducer:
    """Enhanced producer with time-ordering, DLQ, and stable poll cadence"""

    def __init__(self):
        self.topic = Config.PURCHASES_TOPIC
        self.dlq_topic = Config.DLQ_TOPIC

        # Initialize Schema Registry
        self.schema_registry = SchemaRegistry(Config.SCHEMA_REGISTRY_URL)
        self.schema_path = '../schemas/purchase-event.avsc'

        # Initialize Kafka Producer (main)
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
            'sent': 0,           # delivery success count
            'failed': 0,         # local/serialize/produce errors + delivery errors
            'dlq': 0,
            'total_revenue': 0,  # delivered revenue (ONLY increments on delivery success)
            'start_time': time.time()
        }

        # Poll cadence counters (produce-call based)
        self._produced_calls = 0
        self._dlq_calls = 0

        self.replay_speed = Config.REPLAY_SPEED
        logger.info(f"🚀 ImprovedPurchaseProducer initialized (replay_speed={self.replay_speed}x)")
        logger.info(f"   📊 Topic: {self.topic}")
        logger.info(f"   🧯 DLQ Topic: {self.dlq_topic}")
        logger.info(f"   🔗 Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")

    def parse_row(self, row):
        """Parse CSV row to event dictionary"""
        try:
            event_ts = datetime.fromisoformat(row['Timestamp'].replace('Z', '+00:00'))

            price = float(row['Price'])
            quantity = int(row['Quantity'])

            event = {
                'session_id': int(row['Session ID']),
                'event_ts': int(event_ts.timestamp() * 1000),  # milliseconds
                'item_id': int(row['Item ID']),
                'price': float(price),
                'quantity': quantity,
                'event_type': 'purchase',
                'event_id': make_event_id(
                    int(row['Session ID']),
                    int(row['Item ID']),
                    int(event_ts.timestamp() * 1000),
                    quantity,
                    float(price)
                ),
                'ingest_ts': int(time.time() * 1000),
                'source': 'producer_purchases'
            }

            revenue = price * quantity
            return event, revenue
        except Exception as e:
            logger.error(f"❌ Parse error: {e}, row: {row}")
            return None, 0

    def send_to_dlq(self, original, error_msg, stage, event=None):
        """Send failed rows/events to DLQ as JSON."""
        try:
            dlq_message = {
                'producer': 'improved-purchase-producer',
                'source_topic': self.topic,
                'stage': stage,
                'error': str(error_msg),
                'timestamp': datetime.now(timezone.utc).isoformat(),
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

    def load_events(self, csv_path):
        """Load events from pre-sorted CSV file"""
        logger.info(f"📖 Loading purchase events from {csv_path} (pre-sorted)...")
        events = []
        total_revenue = 0

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                event, revenue = self.parse_row(row)
                if event:
                    events.append(event)
                    total_revenue += revenue
                else:
                    self.send_to_dlq(original=row, error_msg="Failed to parse row", stage="parse")

        logger.info(f"✅ Loaded {len(events):,} purchase events (already sorted)")
        logger.info(f"   💰 Total revenue (from file): ${total_revenue:,}")

        if events:
            first_ts = datetime.fromtimestamp(events[0]['event_ts'] / 1000)
            last_ts = datetime.fromtimestamp(events[-1]['event_ts'] / 1000)
            logger.info(f"   📅 Original time range: {first_ts} to {last_ts}")

        return events

    def _make_delivery_cb(self, revenue, event):
        """
        Create a delivery callback that closes over revenue (and optionally event).
        confluent_kafka callback signature is (err, msg).
        """
        def _cb(err, msg):
            if err:
                self.stats['failed'] += 1
                logger.error(f"❌ Delivery failed: {err}")

                # Optional: DLQ on delivery failure (useful)
                try:
                    self.send_to_dlq(
                        original={'delivery_failed': True},
                        error_msg=str(err),
                        stage="delivery",
                        event=event
                    )
                except Exception:
                    pass
                return

            # Delivery success
            self.stats['sent'] += 1
            self.stats['total_revenue'] += int(revenue)

            # Progress log every 10 purchases
            if self.stats['sent'] % 10 == 0:
                elapsed = time.time() - self.stats['start_time']
                rate = self.stats['sent'] / elapsed if elapsed > 0 else 0
                logger.info(
                    f"📈 Progress: {self.stats['sent']:,} purchases "
                    f"({rate:.1f} msg/sec, revenue: ${self.stats['total_revenue']:,}, dlq: {self.stats['dlq']:,})"
                )
        return _cb

    def send_event(self, event, original_row=None):
        """Send a single purchase event to Kafka (Avro). On failure -> DLQ."""
        try:
            event.setdefault('event_type', 'purchase')
            event.setdefault('ingest_ts', int(time.time() * 1000))
            event.setdefault('source', 'producer_purchases')
            if not event.get('event_id'):
                event['event_id'] = make_event_id(
                    int(event['session_id']),
                    int(event['item_id']),
                    int(event['event_ts']),
                    int(event['quantity']),
                    float(event['price'])
                )

            revenue = int(event['price'] * event['quantity'])
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
                    on_delivery=self._make_delivery_cb(revenue, event)
                )
                self._produced_calls += 1

                # Stable poll cadence based on produce-call count
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
            logger.error(f"❌ Failed to send purchase event (unexpected): {e}")
            self.stats['failed'] += 1
            self.send_to_dlq(
                original=original_row if original_row is not None else event,
                error_msg=e,
                stage="serialize_or_produce",
                event=event
            )

    def produce_from_sorted_csv(self, csv_path):
        """Produce purchase events from CSV file (time-ordered) using CURRENT timestamps."""
        logger.info("=" * 80)
        logger.info("🎬 Starting purchase producer with CURRENT TIMESTAMPS + DLQ")
        logger.info(f"   📁 File: {csv_path}")
        logger.info("=" * 80)

        events = self.load_events(csv_path)
        if not events:
            logger.error("❌ No events loaded!")
            return

        # Shift timestamps to now
        first_event_ts = events[0]['event_ts']
        current_time_ms = int(time.time() * 1000)
        time_offset = current_time_ms - first_event_ts
        logger.info(f"⏰ Time offset: +{time_offset/1000:.0f} seconds (shifting to current time)")

        prev_ts = None
        for event in events:
            original_ts = event['event_ts']
            event['event_ts'] = original_ts + time_offset

            self.send_event(event, original_row=None)

            # Replay speed control
            if prev_ts and self.replay_speed < 100:
                current_dt = datetime.fromtimestamp(original_ts / 1000)
                prev_dt = datetime.fromtimestamp(prev_ts / 1000)
                delay = (current_dt - prev_dt).total_seconds() / self.replay_speed
                if delay > 0:
                    time.sleep(min(delay, 0.1))

            prev_ts = original_ts

        # Final flush (drives callbacks too)
        logger.info("\n⏳ Flushing remaining messages...")
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"⚠️  {remaining} messages failed to flush (main producer)")

        dlq_remaining = self.dlq_producer.flush(timeout=30)
        if dlq_remaining > 0:
            logger.warning(f"⚠️  {dlq_remaining} messages failed to flush (DLQ producer)")

        elapsed = time.time() - self.stats['start_time']
        logger.info("\n" + "=" * 80)
        logger.info("✅ Purchase producer finished!")
        logger.info(f"   📤 Sent (delivered): {self.stats['sent']:,} purchases")
        logger.info(f"   💰 Total revenue (delivered): ${self.stats['total_revenue']:,}")
        logger.info(f"   🧯 DLQ: {self.stats['dlq']:,} messages")
        logger.info(f"   ❌ Failed (local + delivery errors): {self.stats['failed']:,} events")
        logger.info(f"   ⏱️  Duration: {elapsed:.2f} seconds")
        logger.info(f"   📊 Avg Rate: {self.stats['sent'] / elapsed:.1f} msg/sec" if elapsed > 0 else "   📊 Avg Rate: N/A")
        logger.info("=" * 80)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Improved Purchase Events Producer')
    parser.add_argument(
        '--file',
        default=Config.PURCHASES_FILE,
        help='Path to purchases CSV file'
    )
    args = parser.parse_args()

    producer = ImprovedPurchaseProducer()
    producer.produce_from_sorted_csv(args.file)


if __name__ == '__main__':
    main()
