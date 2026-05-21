from gen_data import generate_sales_data, generate_hourly_sales
#from lake_writer import write_fasttrack_to_lake
import json
from kafka import KafkaProducer
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

#df = generate_hourly_sales(1000)

# 1. Kết nối Kafka (Dùng localhost:9092 vì code chạy ngoài Docker)


def dispatch_data(df):
    # Đẩy toàn bộ vào Postgres (Dữ liệu lịch sử)
    #df.to_sql('raw_sales', engine, if_exists='append', index=False)
    producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    fast_track_records = []
    # Đẩy vào Kafka những đơn hàng giá trị cao (>400) để xử lý nhanh
    for _, row in df.iterrows():
        if row['amount'] > 400:
            record = row.to_dict()
            producer.send('fast_track_sales', value=row.to_dict())
            fast_track_records.append(record)

    producer.flush()
    producer.close()
    # Ghi fast track vào Data Lake
    #if fast_track_records:
        #write_fasttrack_to_lake(fast_track_records, datetime.utcnow())
        #print(f"[Kafka] Sent + saved {len(fast_track_records)} fast track records")

    print("Done!")

#dispatch_data(df)