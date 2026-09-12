import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import psycopg2
import psycopg2.extras
from config.settings import settings

def main():
    print("  Aiven PostgreSQL Inspector")

    print(f"\nConnecting to {settings.AIVEN_PG_HOST}:{settings.AIVEN_PG_PORT}/{settings.AIVEN_PG_DBNAME}...")
    try:
        conn = psycopg2.connect(
            host=settings.AIVEN_PG_HOST, port=settings.AIVEN_PG_PORT,
            dbname=settings.AIVEN_PG_DBNAME, user=settings.AIVEN_PG_USER,
            password=settings.AIVEN_PG_PASSWORD, sslmode=settings.AIVEN_PG_SSLMODE,
            connect_timeout=10,
        )
        print("Connected successfully!\n")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (settings.PG_TABLE_ORDERS,))
        table_exists = cur.fetchone()["exists"]

        if not table_exists:
            print(f"Table '{settings.PG_TABLE_ORDERS}' does not exist yet.")
            print("   Start pg_sink_consumer.py and produce some orders first.")
            conn.close()
            return

        cur.execute(f"SELECT COUNT(*) AS total FROM {settings.PG_TABLE_ORDERS}")
        total = cur.fetchone()["total"]

        cur.execute(f"""
            SELECT
                category,
                COUNT(*)                        AS orders,
                SUM(quantity)                   AS units_sold,
                ROUND(AVG(total)::numeric, 2)   AS avg_order_value,
                ROUND(SUM(total)::numeric, 2)   AS revenue,
                COUNT(*) FILTER (WHERE priority) AS priority_orders
            FROM {settings.PG_TABLE_ORDERS}
            GROUP BY category
            ORDER BY revenue DESC
        """)
        category_stats = cur.fetchall()

        cur.execute(f"""
            SELECT order_id, customer_id, product, category,
                   quantity, total, priority, processed_at
            FROM {settings.PG_TABLE_ORDERS}
            ORDER BY inserted_at DESC
            LIMIT 5
        """)
        latest = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) AS cnt FROM {settings.PG_TABLE_ORDERS} WHERE priority = TRUE
        """)
        priority_count = cur.fetchone()["cnt"]

        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (settings.PG_TABLE_FAILED,))
        dlq_table_exists = cur.fetchone()["exists"]

        dlq_total = 0
        dlq_by_reason = []
        dlq_latest = []
        if dlq_table_exists:
            cur.execute(f"SELECT COUNT(*) AS total FROM {settings.PG_TABLE_FAILED}")
            dlq_total = cur.fetchone()["total"]
 
            cur.execute(f"""
                SELECT
                    dlq_reason,
                    COUNT(*) AS occurrences,
                    MAX(failed_at) AS last_seen
                FROM {settings.PG_TABLE_FAILED}
                GROUP BY dlq_reason
                ORDER BY occurrences DESC
            """)
            dlq_by_reason = cur.fetchall()
 
            cur.execute(f"""
                SELECT order_id, customer_id, category,
                       quantity, price, dlq_reason, failed_at
                FROM {settings.PG_TABLE_FAILED}
                ORDER BY failed_at DESC
                LIMIT 5
            """)
            dlq_latest = cur.fetchall()

    conn.close()

    print(f"  Table: {settings.PG_TABLE_ORDERS}")
    print(f"  Total rows     : {total:,}")
    print(f"  Priority orders: {priority_count:,}  ({priority_count/max(total,1)*100:.1f}%)")
    print()

    print("  Revenue by category")
    print(f"  {'Category':<14} {'Orders':>7} {'Units':>7} {'Avg $':>8} {'Revenue $':>11} {'Priority':>9}")
    for row in category_stats:
        print(
            f"  {row['category']:<14} "
            f"{row['orders']:>7,} "
            f"{row['units_sold']:>7,} "
            f"{row['avg_order_value']:>8.2f} "
            f"{row['revenue']:>11,.2f} "
            f"{row['priority_orders']:>9,}"
        )
    print()

    print("  Latest 5 orders (most recent first)")
    for row in latest:
        print(
            f"  {str(row['order_id'])[:8]}... "
            f"| {row['customer_id']:<10} "
            f"| {row['category']:<12} "
            f"| qty={row['quantity']} "
            f"| ${row['total']:.2f}"
        )
    print()

    print("  Failed Orders (DLQ)")
    if not dlq_table_exists:
        print(f"  Table '{settings.PG_TABLE_FAILED}' does not exist yet.")
        print("   Start pg_dlq_sink_consumer.py to create it.")
    else:
        print(f"  Total failed rows: {dlq_total:,}")
        print()
 
        if dlq_by_reason:
            print("  Failures by reason")
            print(f"  {'Reason':<35} {'Count':>8}  Last seen")
            for row in dlq_by_reason:
                last = str(row['last_seen'])[:19] if row['last_seen'] else "—"
                print(
                    f"  {str(row['dlq_reason']):<35} "
                    f"{row['occurrences']:>8,}  {last}"
                )
 
        if dlq_latest:
            print("  Latest 5 failures (most recent first)")
            for row in dlq_latest:
                print(
                    f"  {str(row['order_id'])[:8]}... "
                    f"| {str(row['customer_id'] or '?'):<10} "
                    f"| {str(row['category'] or '?'):<12} "
                    f"| qty={row['quantity']} "
                    f"| reason='{row['dlq_reason']}'"
                )
    print()


if __name__ == "__main__":
    main()
