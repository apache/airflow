from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator

dag = DAG(
    'test_hbase_simple',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
)

task = DummyOperator(
    task_id='test_task',
    dag=dag
)
