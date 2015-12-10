from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'unittest',
    'email_on_failure': False,
    'email_on_retry': False
    }

dag = DAG("tests_dags__bash_operator_single",
          start_date=datetime(2015, 1, 1),
          default_args=default_args)

# no default value for those: it is a bug to try to load this DAG without
# preparing a tmp folder for it
tempDir = Variable.get(key="unit_test_tmp_dir")

BashOperator(
    task_id='echo',
    bash_command='echo success > %s/out.{{ ds }}.txt' % tempDir,
    dag=dag)
