from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable, Trigger
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2015, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
    }

dag = DAG("example_range_operator", default_args=default_args)

tempDir = "/tmp/airflow"

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

a = BashOperator(
    task_id='runme_0',
    bash_command=bash_command,
    depends_on_past=True,
    dag=dag)

b = BashOperator(
    task_id='past_dependence',
    bash_command='echo success_b > %s/out.b.{{ ds }}.txt' % tempDir,
    dag=dag)

c = BashOperator(
    task_id='no_past_dependence',
    bash_command='echo success_c > %s/out.c.{{ ds }}.txt' % tempDir,
    dag=dag)

b.set_trigger(Trigger(a, past_executions=(-4, -2)))
c.set_trigger(Trigger(a, past_executions=0))
