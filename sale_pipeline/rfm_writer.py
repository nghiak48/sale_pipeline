# sale_pipeline/rfm_writer.py

from sqlalchemy import create_engine, text
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime

DB_URL = "postgresql+psycopg2://postgres:admin@postgres:5432/retail_db"

def _get_latest_snapshot(engine) -> pd.DataFrame | None:
    """Đọc snapshot RFM mới nhất từ silver.rfm_snapshot."""
    try:
        df = pd.read_sql("""
            SELECT *
            FROM silver.rfm_snapshot
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) FROM silver.rfm_snapshot
            )
        """, engine)
        return df if not df.empty else None
    except Exception as e:
        print(f"[RFM] No previous snapshot: {e}")
        return None


def _assign_segment(row) -> str:
    if row['recency_days'] <= 7 and row['frequency'] >= 5:
        return 'Champions'
    elif row['recency_days'] <= 14:
        return 'Loyal'
    elif row['recency_days'] <= 30:
        return 'At Risk'
    else:
        return 'Lost'


def _write_snapshot(df: pd.DataFrame, engine):
    """Ghi snapshot vào silver.rfm_snapshot."""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE silver.rfm_snapshot"))
    df.to_sql('rfm_snapshot', engine, schema='silver', if_exists='append', index=False)
    print(f"[RFM] Written {len(df)} rows to silver.rfm_snapshot")


def export_rfm_snapshot(execution_date: datetime):
    engine = create_engine(DB_URL)
    old_df = _get_latest_snapshot(engine)

    if old_df is not None:
        # last_snapshot lấy từ snapshot cũ nhất đọc được
        last_snapshot = pd.Timestamp(old_df['snapshot_date'].max())
        print(f"[RFM] Incremental from {last_snapshot}")

        new_sales = pd.read_sql("""
            SELECT customer_id, sale_date, amount
            FROM silver.sales_enriched
            WHERE sale_date > %(since)s
        """, engine, params={"since": last_snapshot})

        if new_sales.empty:
            print("[RFM] No new sales, skipping.")
            return old_df

        delta = new_sales.groupby('customer_id').agg(
            last_purchase=('sale_date', 'max'),
            frequency=('sale_date', 'count'),
            monetary=('amount', 'sum')
        ).reset_index()

        # Merge delta vào snapshot cũ
        merged = old_df.set_index('customer_id').copy()
        merged['last_purchase'] = pd.to_datetime(merged['last_purchase'])
        delta = delta.set_index('customer_id')
        delta['last_purchase'] = pd.to_datetime(delta['last_purchase'])

        for cid in delta.index:
            if cid in merged.index:
                merged.at[cid, 'frequency'] += delta.at[cid, 'frequency']
                merged.at[cid, 'monetary']  += delta.at[cid, 'monetary']
                merged.at[cid, 'last_purchase'] = max(
                    pd.Timestamp(merged.at[cid, 'last_purchase']),
                    pd.Timestamp(delta.at[cid, 'last_purchase'])
                )
            else:
                merged.loc[cid] = delta.loc[cid]

        df = merged.reset_index()

    else:
        # Lần đầu tiên — full scan từ Silver
        print("[RFM] No previous snapshot, full scan from silver...")
        df = pd.read_sql("""
            SELECT
                customer_id,
                MAX(sale_date)        AS last_purchase,
                COUNT(*)              AS frequency,
                ROUND(SUM(amount), 2) AS monetary
            FROM silver.sales_enriched
            GROUP BY customer_id
        """, engine)

    # Tính recency và segment
    now = pd.Timestamp(execution_date)
    if now.tzinfo is not None:
        now = now.tz_localize(None)

    df['last_purchase'] = pd.to_datetime(df['last_purchase']).dt.tz_localize(None)
    df['recency_days']  = (now - df['last_purchase']).dt.days
    df['segment']       = df.apply(_assign_segment, axis=1)
    df['snapshot_date'] = execution_date.date()

    _write_snapshot(df, engine)
    print(df['segment'].value_counts().to_string())
    return df