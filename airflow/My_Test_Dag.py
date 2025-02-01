from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator  # Use EmptyOperator

with DAG(
    dag_id="my_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # Use 'schedule' instead of 'schedule_interval'
    catchup=False,
    # Add a description (good practice)
    description="A simple test DAG",
    # Set a default for how tasks should retry
    default_args={
        'retries': 1,  # Retry once if a task fails
    },
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> end
