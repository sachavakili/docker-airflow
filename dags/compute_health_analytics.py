from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from health_analytics.main import download_compute_and_upload_health_analytics

args = {
    "owner": "health_analytics",
}

dag = DAG(
    dag_id="example_python_operator",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)


run_this = PythonOperator(
    task_id="etl_health_analytics",
    python_callable=download_compute_and_upload_health_analytics,
    dag=dag,
)
