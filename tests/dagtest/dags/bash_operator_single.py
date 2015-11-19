from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'unittest',
    'start_date': datetime(2015, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
    }

dag = DAG("tests_dags__bash_operator_single", default_args=default_args)

tempDir = Variable.get(key="unit_test_tmp_dir")

BashOperator(
    task_id='echo',
    bash_command='echo success > %s/out.{{ ds }}.txt' % tempDir,
    dag=dag)
