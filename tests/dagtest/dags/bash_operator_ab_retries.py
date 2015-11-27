from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'unittest',
    'start_date': datetime(2015, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
    }

dag = DAG("bash_operator_ab_retries", default_args=default_args)

tempDir = Variable.get("unit_test_tmp_dir", deserialize_json=True)

# retry a number of ( day % 3 ) times
bash_command = """
try={{ ti.try_number }}
day=10#{{ macros.ds_format(ds, "%%Y-%%m-%%d", "%%d") }}
if [ "$try" -ge $(( (($day-1)%%3)+1 )) ]
  then
    echo success_a > %s/out.a.{{ ds }}.txt
    exit 0
  else
    exit 1
fi
""" % tempDir

depends_on_past = Variable.get("depends_on_past", deserialize_json=True)
wait_for_downstream = Variable.get("wait_for_downstream", deserialize_json=True)

a = BashOperator(
    task_id='echo_a',
    bash_command=bash_command,
    wait_for_downstream=wait_for_downstream,
    dag=dag)

b = BashOperator(
    task_id='echo_b',
    bash_command='echo success_b > %s/out.b.{{ ds }}.txt' % tempDir,
    depends_on_past=depends_on_past,
    dag=dag)

a.set_downstream(b)
