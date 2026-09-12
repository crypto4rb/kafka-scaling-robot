"""Notification service: consumes processed orders and simulates sending a
customer notification for each one (logging in place of an actual send)."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import json
from confluent_kafka import Consumer, KafkaError, KafkaException
from config.settings import settings
from utils.models import from_json
from utils.logger import get_logger

log = get_logger("NotificationSvc")

# Consumer configs
consumer = Consumer({
    "bootstrap.servers":  settings.KAFKA_BROKER,
    "group.id":           settings.GROUP_NOTIFICATION,
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": False,
})


def simulate_send_notification(order: dict):
    priority_tag = "🚨 PRIORITY" if order.get("priority") else "📧"
    log.info(
        f"{priority_tag} Notification → "
        f"customer={order['customer_id']} | "
        f"order={order['order_id']} | "
        f"product='{order['product']}' | "
        f"total=${order['total']:.2f}"
    )


def run():
    consumer.subscribe([settings.TOPIC_PROCESSED])
    log.info(f"Notification Service starting")
    log.info(f"   Group : '{settings.GROUP_NOTIFICATION}'")
    log.info(f"   Topic : '{settings.TOPIC_PROCESSED}'")
    log.info("   Press Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug(f"End of partition {msg.partition()} offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
                continue

            # Process message
            order = from_json(msg.value())
            simulate_send_notification(order)

            # Manual commit
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        log.info("\nNotification Service shutting down...")
    finally:
        consumer.close()
        log.info("Consumer closed cleanly.")


if __name__ == "__main__":
    run()
