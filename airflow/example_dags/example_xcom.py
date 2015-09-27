from __future__ import print_function
import airflow
import datetime

dag = airflow.DAG(
    'example_xcom',
    start_date=datetime.datetime(2015, 1, 1),
    default_args={'owner': 'airflow', 'provide_context': True})

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}

def push(**kwargs):
    # pushes an XCom without a specific target
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)

def push_by_returning(**kwargs):
    # pushes an XCom without a specific target, just by returning it
    return value_2

def puller(**kwargs):
    ti = kwargs['ti']

    # get value_1
    v1 = ti.xcom_pull(key=None, task_ids='push')
    assert v1 == value_1

    # get value_2
    v2 = ti.xcom_pull(task_ids='push_by_returning')
    assert v2 == value_2

    # get both value_1 and value_2
    v1, v2 = ti.xcom_pull(key=None, task_ids=['push', 'push_by_returning'])
    assert (v1, v2) == (value_1, value_2)

push1 = airflow.operators.PythonOperator(
    task_id='push', dag=dag, python_callable=push)

push2 = airflow.operators.PythonOperator(
    task_id='push_by_returning', dag=dag, python_callable=push_by_returning)

pull = airflow.operators.PythonOperator(
    task_id='puller', dag=dag, python_callable=puller)

pull.set_upstream([push1, push2])