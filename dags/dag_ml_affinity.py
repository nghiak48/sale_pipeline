"""
DAG: daily_ml_product_affinity
Chạy sau silver_to_gold để có đủ data.
Schedule: 3:00 AM hàng ngày
"""

#import common
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/sale_pipeline')
from product_affinity import run_product_affinity

def run_affinity(**context):
    run_product_affinity()

with DAG(
    dag_id='daily_ml_product_affinity',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,   # triggered bởi silver_to_gold
    catchup=False,
    default_args={
        'owner': 'nghia',
        'retries': 99,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    ml_task = PythonOperator(
        task_id='run_product_affinity',
        python_callable=run_affinity
    )
