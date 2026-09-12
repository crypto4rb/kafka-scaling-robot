import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import time
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException

from config.settings import settings
from utils.models import from_json
from utils.logger import get_logger

log = get_logger("PG-Sink")

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {settings.PG_TABLE_ORDERS} (
    order_id     TEXT        PRIMARY KEY,
    customer_id  TEXT        NOT NULL,
    product      TEXT        NOT NULL,
    category     TEXT        NOT NULL,
    quantity     INTEGER     NOT NULL,
    price        NUMERIC(10,2) NOT NULL,
    total        NUMERIC(10,2) NOT NULL,
    status       TEXT        NOT NULL,
    priority     BOOLEAN     DEFAULT FALSE,
    created_at   TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    inserted_at  TIMESTAMPTZ DEFAULT NOW()   -- when this row landed in PG
);
"""

UPSERT_SQL = f"""
INSERT INTO {settings.PG_TABLE_ORDERS} (
    order_id, customer_id, product, category,
    quantity, price, total, status,
    priority, created_at, processed_at
)
VALUES %s
ON CONFLICT (order_id) DO UPDATE SET
    status       = EXCLUDED.status,
    priority     = EXCLUDED.priority,
    processed_at = EXCLUDED.processed_at;
"""


def connect_to_postgres() -> psycopg2.extensions.connection:
    log.info(f"Connecting to Aiven PostgreSQL at {settings.AIVEN_PG_HOST}:{settings.AIVEN_PG_PORT}...")
    conn = psycopg2.connect(
        host     = settings.AIVEN_PG_HOST,
        port     = settings.AIVEN_PG_PORT,
        dbname   = settings.AIVEN_PG_DBNAME,
        user     = settings.AIVEN_PG_USER,
        password = settings.AIVEN_PG_PASSWORD,
        sslmode  = settings.AIVEN_PG_SSLMODE,
        connect_timeout = 10,
    )
    conn.autocommit = False
    log.info("PostgreSQL connected.")
    return conn


def ensure_table(conn: psycopg2.extensions.connection):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    log.info(f"Table '{settings.PG_TABLE_ORDERS}' is ready.")


def order_to_row(order: dict) -> tuple:
    return (
        order.get("order_id"),
        order.get("customer_id"),
        order.get("product"),
        order.get("category"),
        int(order.get("quantity", 0)),
        float(order.get("price", 0)),
        float(order.get("total", 0)),
        order.get("status", "processing"),
        bool(order.get("priority", False)),
        order.get("created_at"),
        order.get("processed_at"),
    )


def flush_batch(
    conn:    psycopg2.extensions.connection,
    batch:   list[tuple],
    consumer: Consumer,
) -> int:
    if not batch:
        return 0

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, UPSERT_SQL, batch, page_size=100)
        conn.commit()

        consumer.commit(asynchronous=False)

        return len(batch)

    except Exception as e:
        conn.rollback()
        raise e


def run():
    # Kafka Consumer
    kafka_consumer = Consumer({
        "bootstrap.servers":  settings.KAFKA_BROKER,
        "group.id":           settings.GROUP_PG_SINK,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False
    })
    kafka_consumer.subscribe([settings.TOPIC_PROCESSED])

    pg_conn = connect_to_postgres()
    ensure_table(pg_conn)

    log.info(f"PG Sink starting")
    log.info(f"   Kafka group  : '{settings.GROUP_PG_SINK}'")
    log.info(f"   Reading from : '{settings.TOPIC_PROCESSED}'")
    log.info(f"   Writing to   : '{settings.AIVEN_PG_HOST}/{settings.AIVEN_PG_DBNAME}'.'{settings.PG_TABLE_ORDERS}'")
    log.info(f"   Batch size   : {settings.PG_BATCH_SIZE} messages")
    log.info(f"   Batch timeout: {settings.PG_BATCH_TIMEOUT_SEC}s")
    log.info("   Press Ctrl+C to stop.\n")

    batch: list[tuple] = []
    last_flush = time.monotonic()
    total_inserted = 0

    try:
        while True:
            msg = kafka_consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                order = from_json(msg.value())
                batch.append(order_to_row(order))
                log.info(
                    f"Buffered [{len(batch)}/{settings.PG_BATCH_SIZE}] "
                    f"order_id={order.get('order_id', '?')[:8]}... "
                    f"customer={order.get('customer_id')} "
                    f"total=${order.get('total', 0):.2f}"
                )

            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                raise KafkaException(msg.error())

            elapsed       = time.monotonic() - last_flush
            batch_full    = len(batch) >= settings.PG_BATCH_SIZE
            timeout_hit   = elapsed >= settings.PG_BATCH_TIMEOUT_SEC and len(batch) > 0

            if batch_full or timeout_hit:
                reason = "batch full" if batch_full else f"timeout ({elapsed:.1f}s)"
                log.info(f"Flushing {len(batch)} rows to PostgreSQL ({reason})...")

                try:
                    count = flush_batch(pg_conn, batch, kafka_consumer)
                    total_inserted += count
                    log.info(
                        f"Flushed {count} rows | "
                        f"total inserted so far: {total_inserted}"
                    )
                except Exception as e:
                    log.error(f"Flush failed: {e}")
                    log.error("   Rows will be retried on next batch (Kafka offset not committed)")
                    # Attempt to reconnect to PostgreSQL on next flush
                    try:
                        pg_conn.close()
                    except Exception:
                        pass
                    pg_conn = connect_to_postgres()

                batch.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        log.info(f"\nPG Sink shutting down...")

        # Final flush of any remaining messages in buffer
        if batch:
            log.info(f"Flushing final {len(batch)} buffered rows...")
            try:
                count = flush_batch(pg_conn, batch, kafka_consumer)
                total_inserted += count
                log.info(f"Final flush done.")
            except Exception as e:
                log.error(f"Final flush failed: {e}")

    finally:
        kafka_consumer.close()
        pg_conn.close()
        log.info(f"PG Sink stopped. Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    run()
