import logging

import DummyOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'ds',
    'poke_interval': 600
}

with DAG("ds_test",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ds']
         ) as dag:

dummy = DummyOperator(task_id="dummy")

echo_ds = BashOperator(
    task_id="echo_ds",
    bash_commands="echo {{ ds }}",
    dag=dag
)


def hello_world_func():
    logging.info("Hello World!")


hello_world = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world_func,
    dag=dag
)
}
dummy >> [echo_ds, hello_world]
