import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 1, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0
    }

dag = DAG("bashop",
          default_args=default_args,
          schedule_interval=timedelta(1))

# TODO: we'd like to have a separate file and/or written content per execution date
#tempDir = tempfile.mkdtemp()
tempDir = "/tmp"

BashOperator(
    task_id='echoechoecho',
    bash_command='echo success > %s/out.txt' % tempDir,
            dag=dag)
