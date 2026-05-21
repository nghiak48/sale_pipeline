import boto3
import os
from datetime import datetime

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "bronze"
LOCAL_BASE = "/opt/airflow/data_lake/raw"

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

def ensure_bucket():
    client = get_minio_client()
    existing = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if BUCKET not in existing:
        client.create_bucket(Bucket=BUCKET)
        print(f"[Bronze] Created bucket: {BUCKET}")

def upload_to_bronze(execution_date: datetime):
    """
    Tìm file Parquet trên disk theo execution_date
    → upload lên MinIO bucket 'bronze' giữ nguyên partition path
    """
    ensure_bucket()
    client = get_minio_client()

    # Path trên disk
    local_path = (
        f"{LOCAL_BASE}/sales"
        f"/year={execution_date.year}"
        f"/month={execution_date.month:02d}"
        f"/day={execution_date.day:02d}"
        f"/hour={execution_date.hour:02d}"
    )

    if not os.path.exists(local_path):
        print(f"[Bronze] No local file found at {local_path}, skipping.")
        return

    uploaded = 0
    for fname in os.listdir(local_path):
        if not fname.endswith(".parquet"):
            continue

        local_file = os.path.join(local_path, fname)
        # Key trên MinIO giữ nguyên partition structure
        s3_key = (
            f"sales"
            f"/year={execution_date.year}"
            f"/month={execution_date.month:02d}"
            f"/day={execution_date.day:02d}"
            f"/hour={execution_date.hour:02d}"
            f"/{fname}"
        )

        client.upload_file(local_file, BUCKET, s3_key)
        print(f"[Bronze] Uploaded: {s3_key}")
        uploaded += 1

    print(f"[Bronze] Total uploaded: {uploaded} files")
    return uploaded