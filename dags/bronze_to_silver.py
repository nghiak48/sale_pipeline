#import common
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append('/opt/airflow/sale_pipeline')
from bronze_ingest import upload_to_bronze
from silver_transform import transform_to_silver
from inventory_manager import update_inventory_and_alert

def run_inventory(**context):
    update_inventory_and_alert(context['logical_date'])

def run_bronze(**context):
    upload_to_bronze(context['logical_date'])

def run_silver(**context):
    transform_to_silver(context['logical_date'])

def is_last_hour(**context) -> bool:
    """Chỉ trigger daily DAGs vào run của giờ 23:00."""
    return context['logical_date'].hour == 23

with DAG(
    dag_id='bronze_to_silver',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=2,
    max_active_tasks = 2,
    default_args={
        'owner': 'nghia',
        'retries': 99,
        'retry_delay': timedelta(minutes=2),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
    }
) as dag:

    bronze_task = PythonOperator(
        task_id='upload_to_bronze',
        python_callable=run_bronze
    )

    silver_task = PythonOperator(
        task_id='transform_to_silver',
        python_callable=run_silver
    )

    inventory_task = PythonOperator(
        task_id = 'update_inventory_and_alert',
        python_callable=run_inventory
    )

    # Trigger daily DAGs chỉ vào giờ cuối ngày
    trigger_rfm = TriggerDagRunOperator(
        task_id='trigger_daily_rfm',
        trigger_dag_id='daily_rfm_snapshot',
        execution_date='{{ logical_date }}',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    trigger_inventory_report = TriggerDagRunOperator(
        task_id='trigger_inventory_report',
        trigger_dag_id='daily_inventory_report',
        execution_date='{{ logical_date }}',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    bronze_task >> silver_task >> inventory_task

    # Chỉ trigger daily nếu là giờ 23
    from airflow.operators.python import BranchPythonOperator

    def check_last_hour(**context):
        if context['logical_date'].hour == 23:
            return ['trigger_daily_rfm', 'trigger_inventory_report']
        return ['skip']

    branch = BranchPythonOperator(
        task_id='check_if_last_hour',
        python_callable=check_last_hour
    )

    from airflow.operators.empty import EmptyOperator
    skip = EmptyOperator(task_id='skip')

    inventory_task >> branch
    branch >> [trigger_rfm, trigger_inventory_report]
    branch >> skip