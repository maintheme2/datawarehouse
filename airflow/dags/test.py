from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

import sys
import os

def generate_data() -> None:
    print(os.getcwd())

with DAG (
    dag_id='test',
    description='test dag',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 23),
    catchup=True
) as dag:

    task1 = PythonOperator(
        task_id='task1',
        python_callable=generate_data,
        dag=dag
    ) 

    task1