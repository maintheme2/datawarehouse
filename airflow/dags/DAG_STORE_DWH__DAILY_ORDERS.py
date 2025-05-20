from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime


with DAG (
    dag_id = 'DAG_STORE_DWH__DAILY_ORDERS',
    description = 'Aggregated data to support daily-level reporting',
    schedule_interval = '@daily',
    start_date = datetime(2025, 2, 23),
    catchup = False
) as dag:

    dbt_daily_orders = BashOperator(
        task_id = 'dbt_daily_orders',
        bash_command = 'dbt run --project-dir /opt/dbt/dwh_models --profiles-dir /opt/dbt/ --select daily_orders'
    )

    daily_orders_wait_dwh_orders = ExternalTaskSensor(
        task_id='daily_orders_wait_dwh_orders',
        external_dag_id='DAG_STORE_DWH__ORDERS',
        external_task_id='dbt_orders',
        poke_interval=5*60,
        timeout=86400+100,
        mode='reschedule'
    )

    daily_orders_wait_dwh_products = ExternalTaskSensor(
        task_id='daily_orders_wait_dwh_products',
        external_dag_id='DAG_STORE_DWH__PRODUCTS',
        external_task_id='dbt_products',
        poke_interval=5*60,
        timeout=86400+100,
        mode='reschedule'
    )

    [daily_orders_wait_dwh_orders, daily_orders_wait_dwh_products] >> dbt_daily_orders