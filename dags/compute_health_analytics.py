import os
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from health_analytics.main import extract as extract_callable
from health_analytics.main import load_to_db as load_to_db_callable
from health_analytics.main import load_to_s3 as load_to_s3_callable
from health_analytics.main import transform as transform_callable
from health_analytics.utils.filesystem import \
    clear_local_cache as clear_local_cache_callable

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

load_to_s3 = PythonOperator(
    task_id="load_into_s3",
    python_callable=load_to_s3_callable,
    dag=dag,
)

load_to_db = PythonOperator(
    task_id="load_into_db",
    python_callable=load_to_db_callable,
    dag=dag,
)

clear_local_cache = PythonOperator(
    task_id="clear_local_cache",
    python_callable=clear_local_cache_callable,
    dag=dag,
)

extract >> transform >> [load_to_s3, load_to_db] >> clear_local_cache
