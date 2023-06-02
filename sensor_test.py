from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta
from airflow.utils.dates import days_ago

dag = DAG(
    'sensor_test',
    description='DAG with a sensor',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1)
)

sensor1 = ExternalTaskSensor(
    task_id='dag_sensor',
    external_dag_id='elt_pipeline_sample',
    external_task_id=None,
    dag=dag,
    mode='reschedule',
    timeout=2500
)

task1 = EmptyOperator(
    task_id='dummy_task',
    retries=1,
    dag=dag
)

sensor1 >> task1
