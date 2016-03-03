import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

num_of_tasks = 30

default_args = {
    'owner': 'lucafuji',
    'depends_on_past': True,
    'start_date': datetime(2016, 3, 1),
    'end_date': datetime(2016, 3, 3),
    'auto_backfill': True
}


def sleep_30(**kwargs):
    print "My task id is {}".format(kwargs['task'].task_id)
    time.sleep(30)

dag = DAG(dag_id="concurrency_test", default_args=default_args,concurrency=1)
for i in range(0, num_of_tasks):
    tid = "sleep_task_{}".format(i)
    task = PythonOperator(dag=dag, task_id=tid, python_callable=sleep_30, provide_context=True)