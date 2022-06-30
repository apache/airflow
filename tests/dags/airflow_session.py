
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators import dummy_operator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    'airflow_session',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 30),
    catchup=False,
) as dag:

    start = dummy_operator.DummyOperator(
    task_id='start',
    trigger_rule='all_success',
    )

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_hello_mobilityware',
        depends_on_past=False,
        bash_command='echo hello_mobilityware',
    )

    t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t3 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )


    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_success',
        dag=dag
    )

    start >> [t1, t2] >> t3 >> end
