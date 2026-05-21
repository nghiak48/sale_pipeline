#import common
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append('/opt/airflow/sale_pipeline')
from gold_marts import refresh_mart_sales, refresh_mart_account, refresh_mart_inventory

def run_mart_sales(**context):
    refresh_mart_sales(context['logical_date'])

def run_mart_account(**context):
    refresh_mart_account(context['logical_date'])

def run_mart_inventory(**context):
    refresh_mart_inventory(context['logical_date'])

with DAG(
    dag_id='silver_to_gold',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,   
    catchup=True,
    default_args={
        'owner': 'nghia',
        'retries': 99,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
    }
) as dag:

    sales_task = PythonOperator(
        task_id='refresh_mart_sales',
        python_callable=run_mart_sales
    )

    account_task = PythonOperator(
        task_id='refresh_mart_account',
        python_callable=run_mart_account
    )

    inventory_task = PythonOperator(
        task_id='refresh_mart_inventory',
        python_callable=run_mart_inventory
    )

    # Trigger ML sau khi Gold refresh xong
    trigger_ml = TriggerDagRunOperator(
        task_id='trigger_ml_affinity',
        trigger_dag_id='daily_ml_product_affinity',
        execution_date='{{ logical_date }}',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Sales + Account + Inventory chạy song song
    # Sau đó trigger ML
    [sales_task, account_task, inventory_task] >> trigger_ml