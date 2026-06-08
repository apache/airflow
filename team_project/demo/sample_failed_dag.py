"""sample_failed_dag.py
Milestone Mavericks, CSS 566A, UW Bothell, Spring 2026.

Sample DAG that intentionally fails with a missing connection error
to demonstrate the AI-assisted triage plugin end-to-end.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def fail_with_missing_connection():
    raise Exception(
        "host unreachable: DNS resolution failed for external-db.example.com. "
        "Connection 'external_db' is not configured in Airflow connections."
    )


with DAG(
    dag_id="sample_failed_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "triage"],
) as dag:

    trigger_failure = PythonOperator(
        task_id="trigger_failure",
        python_callable=fail_with_missing_connection,
    )
