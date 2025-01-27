from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

with DAG(
    'test',
    description='test dag',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 23),
    catchup=True
)as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World"',
        dag=dag
    )   

    task1  