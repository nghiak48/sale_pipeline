import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
from sqlalchemy import create_engine, text
from datetime import datetime

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "bronze"
DB_URL = "postgresql+psycopg2://postgres:admin@postgres:5432/retail_db"

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def read_from_bronze(execution_date: datetime) -> pd.DataFrame:
    """Đọc Parquet từ MinIO theo partition của execution_date."""
    client = get_minio_client()
    prefix = (
        f"sales"
        f"/year={execution_date.year}"
        f"/month={execution_date.month:02d}"
        f"/day={execution_date.day:02d}"
        f"/hour={execution_date.hour:02d}/"
    )

    response = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    files = response.get("Contents", [])

    if not files:
        print(f"[Silver] No files in Bronze for {prefix}")
        return pd.DataFrame()

    dfs = []
    for obj in files:
        body = client.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        df = pq.read_table(io.BytesIO(body)).to_pandas()
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def _check_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Data quality checks cơ bản."""
    before = len(df)

    # 1. Drop null ở các cột quan trọng
    df = df.dropna(subset=["sale_id", "customer_id", "product_id", "amount"])

    # 2. Drop duplicate sale_id
    df = df.drop_duplicates(subset=["sale_id"])

    # 3. Filter amount hợp lệ
    df = df[df["amount"] > 0]

    after = len(df)
    print(f"[Silver] Quality check: {before} → {after} rows ({before - after} dropped)")
    return df

def transform_to_silver(execution_date: datetime):
    """
    Đọc Bronze MinIO → quality check → enrich → ghi Silver PG
    """
    engine = create_engine(DB_URL)

    # 1. Đọc từ Bronze
    df = read_from_bronze(execution_date)
    if df.empty:
        print("[Silver] Nothing to transform.")
        return

    # 2. Data quality
    df = _check_quality(df)

    # 3. Enrich với products
    products = pd.read_sql(
        "SELECT product_id, product_name, category FROM products", engine
    )
    df = df.merge(products, on="product_id", how="left")

    # 4. Thêm derived columns
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["sale_hour"] = df["sale_date"].dt.hour
    df["sale_date_only"] = df["sale_date"].dt.date
    df["processed_at"] = datetime.utcnow()

    # 5. Ghi vào Silver
    cols = [
        "sale_id", "customer_id", "store_id", "product_id",
        "product_name", "category", "amount",
        "sale_date", "sale_hour", "sale_date_only", "processed_at"
    ]
    df[cols].to_sql(
        "sales_enriched", engine,
        schema="silver", if_exists="append", index=False
    )
    print(f"[Silver] Written {len(df)} rows to silver.sales_enriched")