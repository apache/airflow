
# Define Dag to load
import datetime
import time

import airflow
from airflow.providers.standard.operators.python import PythonOperator

time.sleep(1)

with airflow.DAG(
    "import_timeout",
    start_date=datetime.datetime(2022, 1, 1),
    schedule=None) as dag:
    def f():
        print("Sleeping")
        time.sleep(1)


    for ind in range(10):
        PythonOperator(
            dag=dag,
            task_id=f"sleep_2_{ind}",
            python_callable=f,
        )
        