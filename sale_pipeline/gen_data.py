import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from sqlalchemy import create_engine

def generate_sales_data(num_records=1000):
    data = []
    start_date = datetime(2026, 1, 1)
    
    for _ in range(num_records):
        # 1. Mô phỏng theo Mùa (Tháng)
        # Xác suất mua cao ở đầu năm (tháng 1, 2) và cuối năm (tháng 11, 12)
        month_weights = [0.15, 0.12, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.08, 0.15, 0.15]
        month = np.random.choice(range(1, 13), p=month_weights)
        
        # 2. Mô phỏng theo Giờ (Thói quen trong ngày)
        # Trưa (11h-13h) và Tối (19h-21h) có trọng số cao
        hour_weights = [0.01]*7 + [0.03]*4 + [0.15]*3 + [0.03]*5 + [0.18]*3 + [0.02]*2
        # Đảm bảo tổng xác suất = 1.0 (Numpy yêu cầu khắt khe việc này)
        hour_weights = np.array(hour_weights)
        hour_weights /= hour_weights.sum()
        hour = np.random.choice(range(24), p=hour_weights)
        
        # Tạo ngày ngẫu nhiên dựa trên tháng và giờ đã chọn
        day = random.randint(1, 28)
        sale_time = start_date.replace(month=month, day=day, hour=hour, 
                                      minute=random.randint(0, 59))
        
        # 3. Các thông tin khác
        record = {
            "sale_id": str(uuid.uuid4()),
            "customer_id": random.randint(1, 500), # Giả lập 500 khách hàng
            "store_id": random.randint(1, 10),     # Giả lập 10 cửa hàng
            "product_id": random.randint(1, 100),   # Giả lập 100 sản phẩm
            "amount": round(random.uniform(50.0, 500.0), 2),
            "sale_date": sale_time
        }
        data.append(record)
    
    return pd.DataFrame(data).sort_values("sale_date")

def generate_hourly_sales(execution_date):
    # execution_date là thời điểm Airflow bắt đầu chạy (ví dụ 9h)
    # Chúng ta sẽ gen dữ liệu cho khoảng từ 8h đến 9h
    start_time = execution_date - timedelta(hours=1)
    end_time = execution_date
    
    # Tính toán số lượng bản ghi dựa trên hour_weights ban đầu
    # Ví dụ: Giờ cao điểm (Trưa/Tối) thì gen 200 đơn, giờ thấp điểm gen 20 đơn
    hour = start_time.hour
    
    # Trọng số giờ (copy từ logic cũ của bạn)
    hour_weights = [0.01]*7 + [0.03]*4 + [0.15]*3 + [0.03]*5 + [0.18]*3 + [0.05]*2
    base_volume = 500 # Số đơn hàng trung bình mỗi tiếng
    num_records = int(base_volume * hour_weights[hour] * 10) # Nhân 10 để ra số lượng thực tế
    
    data = []
    for _ in range(max(1, num_records)): # Đảm bảo luôn có ít nhất 1 record
        sale_time = start_time + timedelta(minutes=np.random.randint(0, 59))
        
        record = {
            "sale_id": np.random.randint(100000, 999999),
            "customer_id": np.random.randint(1, 500),
            "store_id": np.random.randint(1, 10),
            "product_id": np.random.randint(1, 100),
            "amount": round(np.random.uniform(50.0, 500.0), 2),
            "sale_date": sale_time
        }
        data.append(record)
    
    return pd.DataFrame(data)

class SalesPipeline:
    def __init__(self, db_config):
        # db_config = "postgresql://user:password@localhost:5432/your_db"
        self.engine = create_engine(db_config)

    def upload_to_bronze(self, df, table_name="raw_sales"):
        """Đổ dữ liệu thô vào lớp Bronze trong Postgres"""
        try:
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            print(f"Successfully uploaded {len(df)} records to {table_name}")
        except Exception as e:
            print(f"Error: {e}")

# --- Thực thi ---
if __name__ == "__main__":
    # 1. Khởi tạo cấu hình (Thay bằng thông tin của bạn)
    DB_URL = "postgresql://postgres:admin@localhost:5432/retail_db"
    
    # 2. Gen dữ liệu
    df_sales = generate_sales_data(2000)
    
    # 3. Chạy Pipeline
    pipeline = SalesPipeline(DB_URL)
    pipeline.upload_to_bronze(df_sales)