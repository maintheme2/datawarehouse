from airflow.models import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime

with DAG (
    dag_id='DAG_STG__ORDERS',
    description='load data into staging orders table',
    schedule_interval='@hourly',
    start_date=datetime(2025, 2, 23),
    catchup=False
) as dag:

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='postgres_store_db1',
        sql="""
            COPY orders 
            FROM '/opt/data/orders.csv' 
            DELIMITER ',' 
            CSV HEADER;
            """,
        dag=dag
    )

    load_data