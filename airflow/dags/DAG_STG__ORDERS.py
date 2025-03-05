from airflow.models import DAG
from airflow.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime

with DAG (
    dag_id='DAG_STG__ORDERS',
    description='load data into staging orders table',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 23),
    catchup=False
) as dag:

    load_data = PostgresOperator(
        task_id='generate_data',
        sql,
        dag=dag
    )