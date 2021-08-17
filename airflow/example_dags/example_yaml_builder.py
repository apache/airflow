from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import dates


def make_dag(yaml_config):

    with DAG(
        dag_id=f"example_yaml_dag_{yaml_config['id']}",
        start_date=dates.days_ago(1),
        schedule_interval=yaml_config['schedule']
    ) as dag:
        bash_task = BashOperator(
            task_id='bash_task',
            bash_command=yaml_config['command']
        )
    return dag
