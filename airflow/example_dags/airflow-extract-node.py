from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG('extract_dag',
         schedule_interval=None,
         start_date=datetime(2022, 1, 1),
         catchup=False) as dag:

    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://datahub.io/core/top-level-domain-names/r/top-level-domain-names.csv.csv -O /root/airflow-extract-data.csv',
    )

    extract_task
