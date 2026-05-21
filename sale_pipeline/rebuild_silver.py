"""
Script rebuild silver.sales_enriched từ Bronze (local disk)
Chạy 1 lần để recover data sau khi truncate.
"""
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from datetime import datetime

DB_URL = "postgresql+psycopg2://postgres:admin@postgres:5432/retail_db"
BRONZE_BASE = "/opt/airflow/data_lake/raw/sales"

def rebuild_silver():
    engine = create_engine(DB_URL)

    # Load products để enrich
    products = pd.read_sql(
        "SELECT product_id, product_name, category FROM products", engine
    )
    if products.empty:
        print("[Rebuild] ERROR: products table is empty! Run seed_db.py first.")
        return

    print(f"[Rebuild] Loaded {len(products)} products")

    # Tìm tất cả file parquet trong Bronze
    all_files = []
    for root, dirs, files in os.walk(BRONZE_BASE):
        for f in files:
            if f.endswith(".parquet"):
                all_files.append(os.path.join(root, f))

    if not all_files:
        print(f"[Rebuild] No parquet files found in {BRONZE_BASE}")
        return

    print(f"[Rebuild] Found {len(all_files)} parquet files, rebuilding silver...")

    total_rows = 0
    for i, filepath in enumerate(sorted(all_files)):
        try:
            df = pq.read_table(filepath).to_pandas()

            # Data quality
            df = df.dropna(subset=["sale_id", "customer_id", "product_id", "amount"])
            df = df.drop_duplicates(subset=["sale_id"])
            df = df[df["amount"] > 0]

            if df.empty:
                continue

            # Enrich với products
            df = df.merge(products, on="product_id", how="left")
            df["sale_date"]      = pd.to_datetime(df["sale_date"])
            df["sale_hour"]      = df["sale_date"].dt.hour
            df["sale_date_only"] = df["sale_date"].dt.date
            df["processed_at"]   = datetime.utcnow()

            cols = [
                "sale_id", "customer_id", "store_id", "product_id",
                "product_name", "category", "amount",
                "sale_date", "sale_hour", "sale_date_only", "processed_at"
            ]
            df[cols].to_sql(
                "sales_enriched", engine,
                schema="silver", if_exists="append", index=False
            )
            total_rows += len(df)

            if (i + 1) % 10 == 0:
                print(f"[Rebuild] Progress: {i+1}/{len(all_files)} files, {total_rows} rows so far...")

        except Exception as e:
            print(f"[Rebuild] Error processing {filepath}: {e}")
            continue

    print(f"[Rebuild] Done! Total {total_rows} rows written to silver.sales_enriched")

    # Verify
    result = pd.read_sql("""
        SELECT COUNT(*) as total,
               COUNT(product_name) as has_name,
               MIN(sale_date_only) as from_date,
               MAX(sale_date_only) as to_date
        FROM silver.sales_enriched
    """, engine)
    print(f"[Rebuild] Verification: {result.to_string(index=False)}")


if __name__ == "__main__":
    rebuild_silver()