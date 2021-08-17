from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import dates


def make_dag(config):

    with DAG(
        dag_id=f"generated_dag_{config['id']}",
        start_date=dates.days_ago(1),
        schedule_interval=config['schedule']
    ) as dag:
        bash_task = BashOperator(
            task_id='bash_task',
            bash_command=config['command']
        )
    return dag
