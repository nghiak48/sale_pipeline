from kafka import KafkaConsumer
import json
#import psycopg2

# 1. Kết nối Kafka & Postgres
consumer = KafkaConsumer(
    'fast_track_sales',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest', # Đọc từ đầu để không sót dữ liệu cũ
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
count = 0 
# 2. Logic nạp vào Bronze
for message in consumer:
    count +=1
    data = message.value
    # Thực hiện lệnh SQL INSERT vào bảng bronze của Postgres tại đây
    print(f"Đã nạp dữ liệu Bronze: {data['sale_id']}")
    print(count)
print("Done")