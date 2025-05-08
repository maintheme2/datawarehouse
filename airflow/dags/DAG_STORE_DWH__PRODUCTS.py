from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG (
    dag_id = 'DAG_STORE_DWH__PRODUCTS',
    description = 'load data from stg into mart',
    schedule_interval = '@daily',
    start_date = datetime(2025, 2, 23),
    catchup = False
) as dag:

    dbt_products = BashOperator(
        task_id = 'dbt_products',
        bash_command = 'dbt run --project-dir /opt/dbt/dwh_models --profiles-dir /opt/dbt/ --select products'
    )

    dbt_products