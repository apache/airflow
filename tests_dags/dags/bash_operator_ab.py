from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'unittest',
    'email_on_failure': False,
    'email_on_retry': False
    }

dag = DAG("tests_dags__bash_operator_ab",
          start_date=datetime(2015, 1, 1),
          end_date=datetime(2015, 1, 10),
          default_args=default_args)

# no default value for those: it is a bug to try to load this DAG without
# preparing a tmp folder for it
tempDir = Variable.get("unit_test_tmp_dir")

b = BashOperator(
    task_id='echo_b',
    bash_command='echo success_b > %s/out.b.{{ ds }}.txt' % tempDir,
    dag=dag)

a = BashOperator(
    task_id='echo_a',
    bash_command='echo success_a > %s/out.a.{{ ds }}.txt' % tempDir,
    dag=dag)

direction = Variable.get(key="dependency_direction",
                         deserialize_json=True,
                         default_var="downstream")

if direction == "downstream":
    a.set_downstream(b)
elif direction == "upstream":
    b.set_upstream(a)
