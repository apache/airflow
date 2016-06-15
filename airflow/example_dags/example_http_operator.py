"""
### Example HTTP operator and sensor
"""
from airflow import DAG
from airflow.operators import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
from datetime import datetime, timedelta
import json

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_http_operator', default_args=default_args)

dag.doc_md = __doc__

# t1, t2 and t3 are examples of tasks created by instatiating operators
t1 = SimpleHttpOperator(
    task_id='post_op',
    endpoint='api/v1.0/nodes',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if len(response.json()) == 0 else False,
    dag=dag)

t5 = SimpleHttpOperator(
    task_id='post_op_formenc',
    endpoint='nodes/url',
    data="name=Joe",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag)

t2 = SimpleHttpOperator(
    task_id='get_op',
    method='GET',
    endpoint='api/v1.0/nodes',
    data={"param1": "value1", "param2": "value2"},
    headers={},
    dag=dag)

t3 = SimpleHttpOperator(
    task_id='put_op',
    method='PUT',
    endpoint='api/v1.0/nodes',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    dag=dag)

t4 = SimpleHttpOperator(
    task_id='del_op',
    method='DELETE',
    endpoint='api/v1.0/nodes',
    data="some=data",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag)

sensor = HttpSensor(
    task_id='http_sensor_check',
    http_conn_id='http_default',
    endpoint='',
    params={},
    response_check=lambda response: True if "Google" in response.content else False,
    poke_interval=5,
    dag=dag)

t1.set_upstream(sensor)
t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
