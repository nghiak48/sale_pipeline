from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow/sale_pipeline')

from rfm_writer import export_rfm_snapshot


def run_rfm(**context):
    export_rfm_snapshot(context['logical_date'])

with DAG(
    dag_id='daily_rfm_snapshot',
    start_date=datetime(2026, 5, 1),
    schedule_interval=None, # chỉ chạy khi bronze_to_silver trigger
    catchup=True,
    default_args={
        'owner': 'nghia',
        'retries': 99,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    rfm_task = PythonOperator(
        task_id='export_rfm',
        python_callable=run_rfm
    )

    # Trigger silver_to_gold sau khi RFM xong
    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_silver_to_gold',
        trigger_dag_id='silver_to_gold',
        execution_date='{{ logical_date }}',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    rfm_task >> trigger_gold

    #rfm_task   # sync sau khi export xong