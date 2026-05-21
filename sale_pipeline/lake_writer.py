import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
from datetime import datetime

LAKE_BASE = '/opt/airflow/data_lake/raw'

def write_batch_to_lake(df: pd.DataFrame, execution_date: datetime):
    """Airflow DAG gọi hàm này để ghi batch hourly vào lake."""
    partition_path = (
        f"{LAKE_BASE}/sales"
        f"/year={execution_date.year}"
        f"/month={execution_date.month:02d}"
        f"/day={execution_date.day:02d}"
        f"/hour={execution_date.hour:02d}"
    )
    os.makedirs(partition_path, exist_ok=True)

    filename = f"batch_{execution_date.strftime('%Y-%m-%dT%H')}.parquet"
    filepath = os.path.join(partition_path, filename)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, filepath, compression='snappy')
    print(f"[Lake] Batch written: {filepath} ({len(df)} rows)")
    return filepath


def write_fasttrack_to_lake(records: list, timestamp: datetime):
    """Kafka Consumer gọi hàm này để ghi fast track records."""
    partition_path = (
        f"{LAKE_BASE}/fast_track"
        f"/year={timestamp.year}"
        f"/month={timestamp.month:02d}"
        f"/day={timestamp.day:02d}"
    )
    os.makedirs(partition_path, exist_ok=True)

    filename = f"fast_track_{timestamp.strftime('%Y-%m-%dT%H-%M-%S')}.parquet"
    filepath = os.path.join(partition_path, filename)

    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, filepath, compression='snappy')
    print(f"[Lake] Fast track written: {filepath} ({len(records)} records)")
    return filepath