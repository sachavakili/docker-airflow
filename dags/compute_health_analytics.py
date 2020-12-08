import os
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from health_analytics.main import extract as extract_callable
from health_analytics.main import load as load_callable
from health_analytics.main import transform as transform_callable

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


extract = PythonOperator(
    task_id="extract_dataset_to_local",
    python_callable=extract_callable,
    dag=dag,
)

transform = PythonOperator(
    task_id="transform_into_analytics",
    python_callable=transform_callable,
    dag=dag,
)

load = PythonOperator(
    task_id="load_into_s3",
    python_callable=load_callable,
    dag=dag,
)

extract >> transform >> load
