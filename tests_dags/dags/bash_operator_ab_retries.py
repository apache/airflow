from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'unittest',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=1)
    }

dag = DAG("tests_dags__bash_operator_ab_retries",
          start_date=datetime(2015, 1, 1),
          end_date=datetime(2015, 1, 10),
          default_args=default_args)

# no default value for those: it is a bug to try to load this DAG without
# preparing a tmp folder for it
tempDir = Variable.get("unit_test_tmp_dir")

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

depends_on_past = Variable.get(key="depends_on_past",
                               deserialize_json=True,
                               default_var=False)

wait_for_downstream = Variable.get(key="wait_for_downstream",
                                   deserialize_json=True,
                                   default_var=False)

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
