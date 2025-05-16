from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime


with DAG (
    dag_id = 'DAG_STORE_DWH__ORDERS',
    description = 'load data from stg into mart',
    schedule_interval = '@daily',
    start_date = datetime(2025, 2, 23),
    catchup = False
) as dag:

    dbt_orders = BashOperator(
        task_id = 'dbt_orders',
        bash_command = 'dbt run --project-dir /opt/dbt/dwh_models --profiles-dir /opt/dbt/ --select orders'
    )

    wait_stg_orders = ExternalTaskSensor(
        task_id='wait_stg_orders',
        external_dag_id='DAG_STORE_STG__ORDERS',
        external_task_id='load_data',
        poke_interval=5*60,
        timeout=86400+100,
        mode='reschedule'
    )

    wait_stg_orders >> dbt_orders