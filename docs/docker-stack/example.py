from datetime import timedelta
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dim_promotion',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
)

dop = DockerOperator(
    api_version='1.37',
    docker_url='TCP://docker-socket-proxy:2375',
    command='echo Hello World',
    image='ubuntu',
    network_mode='bridge',
    task_id='docker_op_tester',
    dag=dag,
)
