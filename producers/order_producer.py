"""Order producer: continuously generates fake orders (occasionally invalid, by
design) and publishes them to the orders topic for the pipeline to consume.
"""

import sys
import os
import time
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from confluent_kafka import Producer
from faker import Faker

from config.settings import settings
from utils.models import make_order, to_json, VALID_CATEGORIES
from utils.logger import get_logger

log  = get_logger("Producer")
fake = Faker()

# Kafka Producer Config
producer = Producer({
    "bootstrap.servers": settings.KAFKA_BROKER,
    "acks":              "all",
    "linger.ms":         50,
    "retries":           3,
})


def delivery_report(err, msg):
    if err:
        log.error(f"Delivery FAILED | {msg.topic()} | {err}")
    else:
        log.info(
            f"Delivered -> topic='{msg.topic()}' "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def make_valid_order() -> dict:
    category = random.choice(list(VALID_CATEGORIES))
    return make_order(
        customer_id = f"CUST-{random.randint(1, 50):03d}",
        product     = fake.bs().title()[:40],
        category    = category,
        quantity    = random.randint(1, 10),
        price       = round(random.uniform(5.0, 300.0), 2),
    )


def make_invalid_order() -> dict:
    order = make_valid_order()
    fault = random.choice(["bad_quantity", "bad_price", "bad_category"])

    if fault == "bad_quantity":
        order["quantity"] = -1
    elif fault == "bad_price":
        order["price"] = 0
    elif fault == "bad_category":
        order["category"] = "weapons"

    log.warning(f"Producing INVALID order (fault={fault}): {order['order_id']}")
    return order


def produce_orders():
    log.info(f"Producer starting -> topic='{settings.TOPIC_ORDERS}' broker='{settings.KAFKA_BROKER}'")
    log.info(f"   Invalid order rate: {int(settings.INVALID_ORDER_RATE * 100)}%")
    log.info("   Press Ctrl+C to stop.\n")

    count = 0
    try:
        while True:
            if random.random() < settings.INVALID_ORDER_RATE:
                order = make_invalid_order()
            else:
                order = make_valid_order()

            producer.produce(
                topic    = settings.TOPIC_ORDERS,
                key      = order["customer_id"].encode("utf-8"),
                value    = to_json(order),
                callback = delivery_report,
            )

            count += 1
            log.info(
                f"[{count}] Sent order {order['order_id']} | "
                f"customer={order['customer_id']} | "
                f"total=${order.get('total', 0):.2f}"
            )

            producer.poll(0)

            time.sleep(settings.PRODUCE_INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("\nShutting down producer...")
    finally:
        log.info("Flushing remaining messages...")
        producer.flush()
        log.info(f"Producer done. Total messages sent: {count}")


if __name__ == "__main__":
    produce_orders()
