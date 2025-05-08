from airflow.models import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.python.generate_inserts import generate_inserts

with DAG (
    dag_id='DAG_STORE_STG__CUSTOMERS',
    description='Load data into staging customers table',
    schedule_interval='@daily',
    start_date=datetime(2025, 2, 23),
    template_searchpath='/opt/scripts/sql',
    catchup=False
) as dag:

    truncate_table = PostgresOperator(
        task_id='truncate_target_table',
        postgres_conn_id='postgres_store_db1',
        sql="""
            truncate table store_stg.stg_customers;
            """,
        dag=dag
    )

    generate_inserts = PythonOperator(
        task_id='generate_inserts',
        python_callable=generate_inserts,
        op_kwargs={
            'target_schema_name': 'store_stg',
            'target_table_name': 'stg_customers',
            'file_name': 'customers.csv'
        },
        dag=dag
    )

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='postgres_store_db1',
        sql='insert_store_stg__stg_customers.sql',
        dag=dag
    )

    generate_inserts >> truncate_table >> load_data