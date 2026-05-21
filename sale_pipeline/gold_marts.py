from sqlalchemy import create_engine, text
from datetime import datetime

DB_URL = "postgresql+psycopg2://postgres:admin@postgres:5432/retail_db"

def refresh_mart_sales(execution_date: datetime):
    """
    Upsert doanh thu theo store + category + giờ.
    Dùng ON CONFLICT để idempotent — chạy lại không bị duplicate.
    """
    engine = create_engine(DB_URL)
    run_date = str(execution_date.date())

    with engine.begin() as conn:
        # Đảm bảo unique constraint tồn tại
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uix_mart_sales_hour
            ON gold.mart_sales(store_id, category, sale_hour)
        """))

        # Upsert — chạy lại nhiều lần vẫn đúng
        conn.execute(text("""
            INSERT INTO gold.mart_sales
                (store_id, category, sale_hour, total_revenue, order_count, avg_order_value)
            SELECT
                store_id,
                COALESCE(category, 'Unknown')       AS category,
                date_trunc('hour', sale_date)        AS sale_hour,
                ROUND(SUM(amount)::NUMERIC, 2)       AS total_revenue,
                COUNT(*)                             AS order_count,
                ROUND(AVG(amount)::NUMERIC, 2)       AS avg_order_value
            FROM silver.sales_enriched
            WHERE sale_date_only = CAST(:run_date AS date)
            GROUP BY store_id, category, date_trunc('hour', sale_date)
            ON CONFLICT (store_id, category, sale_hour)
            DO UPDATE SET
                total_revenue   = EXCLUDED.total_revenue,
                order_count     = EXCLUDED.order_count,
                avg_order_value = EXCLUDED.avg_order_value,
                updated_at      = NOW()
        """), {"run_date": run_date})

    print(f"[Gold] mart_sales upserted for {run_date}")


def refresh_mart_account(execution_date: datetime):
    """
    Upsert RFM snapshot — giữ lịch sử theo snapshot_date.
    Mỗi ngày 1 snapshot, không xóa ngày cũ.
    """
    engine = create_engine(DB_URL)
    snapshot_date = str(execution_date.date())

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uix_mart_account_snapshot
            ON gold.mart_account(customer_id, snapshot_date)
        """))

        conn.execute(text("""
            INSERT INTO gold.mart_account
                (customer_id, segment, frequency, monetary, recency_days, snapshot_date)
            SELECT
                customer_id, segment, frequency,
                ROUND(monetary::NUMERIC, 2),
                recency_days, snapshot_date
            FROM silver.rfm_snapshot
            WHERE snapshot_date = CAST(:snap_date AS date)
            ON CONFLICT (customer_id, snapshot_date)
            DO UPDATE SET
                segment      = EXCLUDED.segment,
                frequency    = EXCLUDED.frequency,
                monetary     = EXCLUDED.monetary,
                recency_days = EXCLUDED.recency_days,
                updated_at   = NOW()
        """), {"snap_date": snapshot_date})

    print(f"[Gold] mart_account upserted for {snapshot_date}")


def refresh_mart_inventory(execution_date: datetime):
    """
    Upsert trạng thái tồn kho hiện tại theo product_id.
    Chỉ giữ snapshot mới nhất — không cần historical.
    """
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS uix_mart_inventory_product
            ON gold.mart_inventory(product_id)
        """))

        conn.execute(text("""
            INSERT INTO gold.mart_inventory
                (product_id, product_name, category,
                 stock_qty, restock_threshold, status, alert_count)
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                i.stock_qty,
                i.restock_threshold,
                CASE
                    WHEN i.stock_qty = 0                    THEN 'CRITICAL'
                    WHEN i.stock_qty <= i.restock_threshold THEN 'LOW'
                    ELSE 'OK'
                END AS status,
                COUNT(a.alert_id) AS alert_count
            FROM inventory i
            JOIN products p ON p.product_id = i.product_id
            LEFT JOIN inventory_alerts a
                ON a.product_id = i.product_id
               AND a.resolved = FALSE
            GROUP BY p.product_id, p.product_name, p.category,
                     i.stock_qty, i.restock_threshold
            ON CONFLICT (product_id)
            DO UPDATE SET
                stock_qty         = EXCLUDED.stock_qty,
                status            = EXCLUDED.status,
                alert_count       = EXCLUDED.alert_count,
                updated_at        = NOW()
        """))

    print(f"[Gold] mart_inventory upserted")