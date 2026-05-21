from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

DB_URL = "postgresql+psycopg2://postgres:admin@postgres:5432/retail_db"


def update_inventory_and_alert(execution_date: datetime):
    """
    Đọc sales trong giờ từ silver.sales_enriched
    → trừ stock → tạo alert nếu cần
    """
    engine = create_engine(DB_URL)

    # Đọc từ Silver thay vì nhận df trực tiếp
    df = pd.read_sql("""
        SELECT product_id, COUNT(*) AS sold_qty
        FROM silver.sales_enriched
        WHERE sale_date >= %(from_dt)s
          AND sale_date <  %(to_dt)s
        GROUP BY product_id
    """, engine, params={
        "from_dt": execution_date.replace(minute=0, second=0, microsecond=0),
        "to_dt":   execution_date
    })

    if df.empty:
        print("[Inventory] No sales in this period.")
        return

    with engine.begin() as conn:
        for _, row in df.iterrows():
            pid  = int(row['product_id'])
            sold = int(row['sold_qty'])

            conn.execute(text("""
                UPDATE inventory
                SET stock_qty    = GREATEST(stock_qty - :sold, 0),
                    last_updated = NOW()
                WHERE product_id = :pid
            """), {"sold": sold, "pid": pid})

            result = conn.execute(text("""
                SELECT stock_qty, restock_threshold
                FROM inventory
                WHERE product_id = :pid
            """), {"pid": pid}).fetchone()

            if result and result.stock_qty <= result.restock_threshold:
                existing = conn.execute(text("""
                    SELECT 1 FROM inventory_alerts
                    WHERE product_id = :pid
                      AND resolved   = FALSE
                      AND alert_time > NOW() - INTERVAL '1 hour'
                """), {"pid": pid}).fetchone()

                if not existing:
                    conn.execute(text("""
                        INSERT INTO inventory_alerts (product_id, stock_qty, threshold)
                        VALUES (:pid, :qty, :threshold)
                    """), {
                        "pid":       pid,
                        "qty":       result.stock_qty,
                        "threshold": result.restock_threshold
                    })
                    print(f"[Alert] Product {pid}: {result.stock_qty} <= {result.restock_threshold}")

    print(f"[Inventory] Updated {len(df)} products")


def generate_daily_report(execution_date: datetime):
    engine = create_engine(DB_URL)
    df = pd.read_sql("""
        SELECT p.product_name, a.stock_qty, a.threshold, a.alert_time
        FROM inventory_alerts a
        JOIN products p ON p.product_id = a.product_id
        WHERE resolved = FALSE
          AND alert_time::date = CURRENT_DATE
        ORDER BY a.stock_qty ASC
    """, engine)

    if df.empty:
        print("[Report] No alerts today.")
        return

    print(f"\n{'='*40}")
    print(f"INVENTORY ALERT REPORT — {execution_date.date()}")
    print(f"{'='*40}")
    print(df.to_string(index=False))
    print(f"\nTotal: {len(df)} products need restocking")