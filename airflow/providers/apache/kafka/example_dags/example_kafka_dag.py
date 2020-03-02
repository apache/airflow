
from airflow import DAG
from datetime import timedelta

from airflow.utils.timezone import datetime
from airflow.providers.apache.kafka.sensors.kafka_sensor import KafkaSensor

DAG_ID = "example_kafka_dag"
dag_start_date = datetime(2015, 6, 1, hour=20, tzinfo=None)
default_args = {
    'owner': '@Ferg_In',
    'depends_on_past': False,
    'start_date': dag_start_date,
    'email': ['dferguson992@gmail.com'],
    'provide_context': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=None,
         max_active_runs=1, concurrency=4, catchup=False) as dag:

    sensor = KafkaSensor(
        task_id='trigger',
        topic='',
        host='',
        port=''
    )
