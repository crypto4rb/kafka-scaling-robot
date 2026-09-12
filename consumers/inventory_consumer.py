"""Inventory service: consumes processed orders and deducts stock from an
in-memory inventory, warning when a category runs low.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import random
from confluent_kafka import Consumer, KafkaError, KafkaException
from config.settings import settings
from utils.models import from_json
from utils.logger import get_logger

log = get_logger("InventorySvc")

# In-memory inventory store
# Maps category -> available stock count
INVENTORY: dict[str, int] = {
    "electronics": 100,
    "clothing":    200,
    "food":        500,
    "books":       150,
    "furniture":   50,
}

consumer = Consumer({
    "bootstrap.servers":  settings.KAFKA_BROKER,
    "group.id":           settings.GROUP_INVENTORY,
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": False,
})


def update_inventory(order: dict):
    """Deduct the order's quantity from its category's stock, warning on unknown categories or low stock."""
    category = order.get("category", "unknown")
    quantity  = order.get("quantity", 0)

    current_stock = INVENTORY.get(category)
    if current_stock is None:
        log.warning(f"Unknown category '{category}' - skipping inventory update")
        return

    remaining = max(0, current_stock - quantity)
    INVENTORY[category] = remaining

    log.info(
        f"Inventory update | category={category} | "
        f"deducted={quantity} | remaining={remaining}"
    )

    if remaining < 20:
        log.warning(f"LOW STOCK ALERT: '{category}' has only {remaining} units left!")


def run():
    """Poll the processed-orders topic forever, updating inventory and committing offsets manually."""
    consumer.subscribe([settings.TOPIC_PROCESSED])
    log.info(f"Inventory Service starting")
    log.info(f"   Group : '{settings.GROUP_INVENTORY}'")
    log.info(f"   Topic : '{settings.TOPIC_PROCESSED}'")
    log.info(f"   Initial stock: {INVENTORY}")
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
            update_inventory(order)

            # Manual commit after successful processing
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        log.info("\nInventory Service shutting down...")
    finally:
        consumer.close()
        log.info("Consumer closed cleanly.")


if __name__ == "__main__":
    run()
