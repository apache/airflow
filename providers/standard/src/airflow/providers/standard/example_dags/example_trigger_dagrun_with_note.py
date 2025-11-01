from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="example_trigger_dagrun_with_note",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    trigger = TriggerDagRunOperator(
        task_id="trigger_with_note",
        trigger_dag_id="example_target_dag",
        note="Triggered with a note!"
    )