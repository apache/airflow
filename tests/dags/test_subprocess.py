import shlex
import subprocess
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator


def _sub_process(ds, **context):
    cmd = "python -c 'print(\"Test subprocess log.\")'"
    p = subprocess.run(shlex.split(cmd))
    ret = p.returncode
    if ret == 0:
        print("Task Completed With Success")
    else:
        print("Error")


dag = DAG(dag_id='test_subprocess', start_date=datetime(2020, 1, 1))
dag_task = PythonOperator(task_id='subprocess_task', python_callable=_sub_process)
