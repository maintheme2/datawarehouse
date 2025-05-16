import os 
import sys
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.python.generate_stg_data import generate_data

with DAG (
    dag_id='DAG_GENERATE_DATA',
    description='generate hourly data',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 28),
    default_args={'retries': 3, 'retry_delay':timedelta(minutes=15)},
    catchup=False
) as dag:

    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        dag=dag
    )

    generate_data