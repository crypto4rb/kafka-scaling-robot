"""DLQ handler: consumes failed orders from the dead-letter topic, logs each
failure, and periodically prints a summary of failure reasons.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, KafkaException
from config.settings import settings
from utils.models import from_json
from utils.logger import get_logger

log = get_logger("DLQ-Handler")

consumer = Consumer({
    "bootstrap.servers":  settings.KAFKA_BROKER,
    "group.id":           settings.GROUP_DLQ,
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": False,
})

failure_stats: dict[str, int] = defaultdict(int)
total_failures = 0

def handle_failed_order(order: dict):
    global total_failures
    total_failures += 1

    reason      = order.get("dlq_reason", "unknown")
    order_id    = order.get("order_id",   "UNKNOWN")
    customer_id = order.get("customer_id","UNKNOWN")
    original_topic = order.get("dlq_topic", "unknown")

    failure_stats[reason] += 1

    log.error(
        f"DLQ | #{total_failures} | "
        f"order_id={order_id} | "
        f"customer={customer_id} | "
        f"reason='{reason}' | "
        f"original_topic='{original_topic}'"
    )

    # Print failure summary every 5 failures
    if total_failures % 5 == 0:
        log.warning("DLQ Failure Summary")
        for r, count in sorted(failure_stats.items(), key=lambda x: -x[1]):
            log.warning(f"   {r:<30} → {count} failures")
        log.warning(f"   TOTAL: {total_failures}")


def run():
    consumer.subscribe([settings.TOPIC_FAILED])
    log.info(f"DLQ Handler starting")
    log.info(f"   Group : '{settings.GROUP_DLQ}'")
    log.info(f"   Topic : '{settings.TOPIC_FAILED}'")
    log.info("   Waiting for failed orders...")
    log.info("   Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            order = from_json(msg.value())
            handle_failed_order(order)
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        log.info(f"\nDLQ Handler shutting down. Total failures processed: {total_failures}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
