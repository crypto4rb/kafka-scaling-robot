import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from confluent_kafka import Consumer, Producer
from config.settings import settings
from utils.models import validate_order, enrich_order, from_json, to_json
from utils.logger import get_logger

log = get_logger("StreamProcessor")

producer = Producer({"bootstrap.servers": settings.KAFKA_BROKER})

consumer = Consumer({
    "bootstrap.servers": settings.KAFKA_BROKER,
    "group.id": "order-processor",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([settings.TOPIC_ORDERS])

log.info("Stream Processor starting...")
log.info(f"   Reading from  : '{settings.TOPIC_ORDERS}'")
log.info(f"   Valid   -> '{settings.TOPIC_PROCESSED}'")
log.info(f"   Invalid -> '{settings.TOPIC_FAILED}' (DLQ)")
log.info("   Press Ctrl+C to stop.")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.error(f"Consumer error: {msg.error()}")
            continue

        order = from_json(msg.value())
        order_id = order.get("order_id", "UNKNOWN")
        customer_id = order.get("customer_id", "UNKNOWN")

        is_valid, error_reason = validate_order(order)
        if is_valid:
            enriched = enrich_order(order)
            producer.produce(
                settings.TOPIC_PROCESSED,
                key=customer_id.encode("utf-8"),
                value=to_json(enriched),
            )
            log.info(f"VALID order_id={order_id} customer={customer_id} total={enriched.get('total', 0):.2f}")
        else:
            order["dlq_reason"] = error_reason
            order["dlq_topic"] = settings.TOPIC_ORDERS
            producer.produce(
                settings.TOPIC_FAILED,
                key=customer_id.encode("utf-8"),
                value=to_json(order),
            )
            log.warning(f"INVALID order_id={order_id} reason={error_reason}")

        producer.flush()

except KeyboardInterrupt:
    log.info("Shutting down...")
finally:
    consumer.close()
