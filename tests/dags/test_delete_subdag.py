"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow.operators.dummy_operator import DummyOperator

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

delta = timedelta(seconds=3)

def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
  dag = DAG(
    '%s.%s' % (parent_dag_name, child_dag_name),
    default_args=default_args,
    schedule_interval=schedule_interval,
    start_date=start_date,
  )

  dummy_operator = DummyOperator(
    task_id='dummy_task',
    dag=dag,
  )

  return dag

from airflow.operators.subdag_operator import SubDagOperator


PARENT_DAG_NAME = 'test_delete_subdag'
CHILD_DAG_NAME = 'test_delete_subdag_child'

main_dag = DAG(
  dag_id=PARENT_DAG_NAME,
  default_args=default_args,
  schedule_interval=timedelta(seconds=3),
  start_date=datetime(2016, 1, 1)
)

sub_dag = SubDagOperator(
  subdag=sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME, main_dag.start_date,
                 main_dag.schedule_interval),
  task_id=CHILD_DAG_NAME,
  dag=main_dag,
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=main_dag)

def py_callable(*args, **kwargs):
    print "args = "
    print args
    print "kwargs = "
    print kwargs

t3 = PythonOperator(
    task_id='py_callable',
    python_callable=py_callable,
    op_args=['dogs'],
    op_kwargs={'cats': 20},
    provide_context=True,
    dag=main_dag)

t3.set_upstream(t1)

