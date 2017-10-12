"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
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
dag = DAG('test_delete_dag', default_args=default_args, schedule_interval=delta)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

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
    dag=dag)

t3.set_upstream(t1)
