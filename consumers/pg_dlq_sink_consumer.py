"""PG DLQ sink: batches failed orders from the DLQ topic and upserts them into
a PostgreSQL table, flushing on batch size or a timeout.
"""

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

log = get_logger("PG-DLQ-Sink")

ID_PREVIEW_LEN = 8  # characters of order_id shown in log lines

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {settings.PG_TABLE_FAILED} (
    order_id     TEXT          PRIMARY KEY,
    customer_id  TEXT,
    product      TEXT,
    category     TEXT,
    quantity     INTEGER,
    price        NUMERIC(10,2),
    total        NUMERIC(10,2),
    status       TEXT,

    -- DLQ-specific columns
    dlq_reason   TEXT          NOT NULL,
    dlq_topic    TEXT,
    failed_at    TIMESTAMPTZ   DEFAULT NOW(),

    -- Original timestamps from the order
    created_at   TIMESTAMPTZ
);

-- Index for fast queries by failure reason (e.g. GROUP BY dlq_reason)
CREATE INDEX IF NOT EXISTS idx_{settings.PG_TABLE_FAILED}_reason
    ON {settings.PG_TABLE_FAILED} (dlq_reason);

-- Index for time-based queries (e.g. failures in the last hour)
CREATE INDEX IF NOT EXISTS idx_{settings.PG_TABLE_FAILED}_failed_at
    ON {settings.PG_TABLE_FAILED} (failed_at DESC);
"""

UPSERT_SQL = f"""
INSERT INTO {settings.PG_TABLE_FAILED} (
    order_id, customer_id, product, category,
    quantity, price, total, status,
    dlq_reason, dlq_topic, created_at
)
VALUES %s
ON CONFLICT (order_id) DO UPDATE SET
    dlq_reason = EXCLUDED.dlq_reason,
    failed_at  = NOW();
"""


def connect_to_postgres() -> psycopg2.extensions.connection:
    """Open a new connection to the Aiven PostgreSQL instance with autocommit disabled."""
    log.info(f"Connecting to Aiven PostgreSQL at {settings.AIVEN_PG_HOST}:{settings.AIVEN_PG_PORT}...")
    conn = psycopg2.connect(
        host=settings.AIVEN_PG_HOST,   port=settings.AIVEN_PG_PORT,
        dbname=settings.AIVEN_PG_DBNAME, user=settings.AIVEN_PG_USER,
        password=settings.AIVEN_PG_PASSWORD, sslmode=settings.AIVEN_PG_SSLMODE,
        connect_timeout=10,
    )
    conn.autocommit = False
    log.info("PostgreSQL connected.")
    return conn


def ensure_table(conn: psycopg2.extensions.connection):
    """Create the failed-orders table and its indexes if they don't already exist."""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    log.info(f"Table '{settings.PG_TABLE_FAILED}' is ready.")


def failed_order_to_row(order: dict) -> tuple:
    """Convert a failed-order dict into a tuple matching the failed-orders table's column order."""
    return (
        order.get("order_id"),
        order.get("customer_id"),
        order.get("product"),
        order.get("category"),
        _safe_int(order.get("quantity")),
        _safe_float(order.get("price")),
        _safe_float(order.get("total")),
        order.get("status", "failed"),
        order.get("dlq_reason", "unknown"),
        order.get("dlq_topic"),
        order.get("created_at"),
    )


def _safe_int(val) -> int | None:
    """Coerce `val` to int, returning None if it can't be converted."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_float(val) -> float | None:
    """Coerce `val` to float, returning None if it can't be converted."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def flush_batch(
    conn:     psycopg2.extensions.connection,
    batch:    list[tuple],
    consumer: Consumer,
) -> int:
    """Upsert `batch` into PostgreSQL and commit the Kafka offset, rolling back on failure."""
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
    """Consume failed orders, buffer them into batches, and flush to PostgreSQL on size or timeout."""
    kafka_consumer = Consumer({
        "bootstrap.servers":  settings.KAFKA_BROKER,
        "group.id":           settings.GROUP_PG_DLQ_SINK,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    kafka_consumer.subscribe([settings.TOPIC_FAILED])

    pg_conn = connect_to_postgres()
    ensure_table(pg_conn)

    log.info(f"PG DLQ Sink starting")
    log.info(f"   Kafka group  : '{settings.GROUP_PG_DLQ_SINK}'")
    log.info(f"   Reading from : '{settings.TOPIC_FAILED}'")
    log.info(f"   Writing to   : '{settings.AIVEN_PG_HOST}/{settings.AIVEN_PG_DBNAME}'.'{settings.PG_TABLE_FAILED}'")
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
                row = failed_order_to_row(order)
                batch.append(row)

                log.warning(
                    f"Buffered [{len(batch)}/{settings.PG_BATCH_SIZE}] "
                    f"order_id={str(order.get('order_id','?'))[:ID_PREVIEW_LEN]}... "
                    f"reason='{order.get('dlq_reason', '?')}'"
                )

            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                raise KafkaException(msg.error())

            elapsed    = time.monotonic() - last_flush
            batch_full = len(batch) >= settings.PG_BATCH_SIZE
            timeout_hit = elapsed >= settings.PG_BATCH_TIMEOUT_SEC and len(batch) > 0

            if batch_full or timeout_hit:
                reason = "batch full" if batch_full else f"timeout ({elapsed:.1f}s)"
                log.info(f"Flushing {len(batch)} failed orders to PostgreSQL ({reason})...")

                try:
                    count = flush_batch(pg_conn, batch, kafka_consumer)
                    total_inserted += count
                    log.info(f"Flushed {count} rows | total: {total_inserted}")
                except Exception as e:
                    log.error(f"Flush failed: {e}")
                    try:
                        pg_conn.close()
                    except Exception:
                        pass
                    pg_conn = connect_to_postgres()

                batch.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        log.info(f"\nPG DLQ Sink shutting down...")
        if batch:
            log.info(f"Flushing final {len(batch)} buffered rows...")
            try:
                count = flush_batch(pg_conn, batch, kafka_consumer)
                total_inserted += count
                log.info("Final flush done.")
            except Exception as e:
                log.error(f"Final flush failed: {e}")
    finally:
        kafka_consumer.close()
        pg_conn.close()
        log.info(f"PG DLQ Sink stopped. Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    run()
