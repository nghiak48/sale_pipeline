from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/sale_pipeline')

from inventory_manager import generate_daily_report

def run_report(**context):
    generate_daily_report(context['logical_date'])

with DAG(
    dag_id='daily_inventory_report',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=True,
    default_args={
        'owner': 'nghia',
        'retries': 99,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    PythonOperator(
        task_id='inventory_report',
        python_callable=run_report
    )