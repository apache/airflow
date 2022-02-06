"""
This is an example DAG.
"""
import pendulum

from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="example_wrong_skip",
    schedule_interval="@daily",
    catchup=False,
    start_date=pendulum.DateTime(2022, 1, 1),
) as dag:
    branch = BranchPythonOperator(task_id="branch", python_callable=lambda: "task_b")
    task_a = PythonOperator(task_id="task_a", python_callable=lambda: True)
    task_b = PythonOperator(task_id="task_b", python_callable=lambda: True)
    task_c = PythonSensor(task_id="task_c", python_callable=lambda: False)
    task_d = PythonOperator(task_id="task_d", python_callable=lambda: True, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    branch >> [task_a, task_b]
    [task_a, task_c] >> task_d
