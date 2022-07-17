

from airflow import models
from airflow.operators.empty import EmptyOperator
from tests.models import DEFAULT_DATE

def basic_dag() -> models.DAG:
    the_dag = models.DAG(
        'the_basic_dag',
        schedule_interval='@daily',
        start_date=DEFAULT_DATE,
    )

    with the_dag:
        op1 = EmptyOperator(task_id='task_1')
        op2 = EmptyOperator(task_id='task_2')
        op3 = EmptyOperator(task_id='task_3')

        op1 >> op2 >> op3

    return the_dag
