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

dag = DAG("bash_operator_ab", default_args=default_args)

tempDir = Variable.get("unit_test_tmp_dir", deserialize_json=True)

b = BashOperator(
    task_id='echo_b',
    bash_command='echo success_b > %s/out.b.{{ ds }}.txt' % tempDir,
    dag=dag)

a = BashOperator(
    task_id='echo_a',
    bash_command='echo success_a > %s/out.a.{{ ds }}.txt' % tempDir,
    dag=dag)

direction = Variable.get("dep_direction", deserialize_json=True)

if direction == "downstream":
    a.set_downstream(b)
elif direction == "upstream":
    b.set_upstream(a)
