# dags/retail_ingestion_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os

# Trỏ đường dẫn để Airflow import được code từ folder pipeline của bạn
sys.path.append('/opt/airflow/sale_pipeline')
from gen_data import generate_hourly_sales
from kafka_ingest import dispatch_data # Hàm bạn đã viết để đẩy vào Postgres/Kafka
from lake_writer import write_batch_to_lake 
from inventory_manager import update_inventory_and_alert
#from db_writer import write_raw_sales   

def run_pipeline(**context):
    # Lấy execution_date từ Airflow context
    execution_date = context['logical_date'] 
    
    # 1. Gen dữ liệu cho đúng khung giờ đó
    df = generate_hourly_sales(execution_date)
    #write_raw_sales(df)

    # 2. Ghi toàn bộ batch vào Data Lake
    write_batch_to_lake(df, execution_date)
    
    # 3. Đẩy dữ liệu đi (vào Postgres và Kafka)
    #dispatch_data(df)

    #update_inventory_and_alert(df)

default_args = {
    'owner': 'nghia',
    'start_date': datetime(2026, 5, 1),
    'retries': 99,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hourly_retail_ingestion_v4',
    default_args=default_args,
    max_active_runs=2,
    max_active_tasks = 2,
    schedule_interval='@hourly', # Chạy mỗi tiếng
    catchup=True # Tự động chạy bù nếu bạn start DAG muộn
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_sales_to_lakehouse_v4',
        python_callable=run_pipeline,
    )

    # Tự động trigger bronze_to_silver với cùng execution_date
    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_to_silver',
        trigger_dag_id='bronze_to_silver',
        execution_date='{{ logical_date }}',  # truyền đúng execution_date
        wait_for_completion=False,  # không chờ, chạy song song
        reset_dag_run=True,         # nếu đã có run thì reset
        allowed_states=['success', 'failed'],  # thêm dòng này
    )

    ingest_task >> trigger_bronze