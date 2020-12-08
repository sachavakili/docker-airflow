import os
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from health_analytics.main import download_compute_and_upload_health_analytics

args = {
    "owner": "health_analytics",
}

dag = DAG(
    dag_id="health_analytics",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)


etl_health_analytics = PythonOperator(
    task_id="etl_health_analytics",
    python_callable=download_compute_and_upload_health_analytics,
    dag=dag,
)
