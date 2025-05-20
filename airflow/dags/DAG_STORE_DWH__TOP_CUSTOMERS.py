from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime


with DAG (
    dag_id = 'DAG_STORE_DWH__TOP_CUSTOMERS',
    description = 'Aggregated data to support daily-level reporting',
    schedule_interval = '@daily',
    start_date = datetime(2025, 2, 23),
    catchup = False
) as dag:

    dbt_top_customers = BashOperator(
        task_id = 'dbt_top_customers',
        bash_command = 'dbt run --project-dir /opt/dbt/dwh_models --profiles-dir /opt/dbt/ --select top_customers'
    )

    top_customers_wait_dwh_orders = ExternalTaskSensor(
        task_id='top_customers_wait_dwh_orders',
        external_dag_id='DAG_STORE_DWH__ORDERS',
        external_task_id='dbt_orders',
        poke_interval=5*60,
        timeout=86400+100,
        mode='reschedule'
    )

    top_customers_wait_dwh_products = ExternalTaskSensor(
        task_id='top_customers_wait_dwh_products',
        external_dag_id='DAG_STORE_DWH__PRODUCTS',
        external_task_id='dbt_products',
        poke_interval=5*60,
        timeout=86400+100,
        mode='reschedule'
    )

    [top_customers_wait_dwh_orders, top_customers_wait_dwh_products] >> dbt_top_customers