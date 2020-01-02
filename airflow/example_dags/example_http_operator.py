# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example HTTP operator and sensor"""

import json
from datetime import timedelta

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_http_operator', default_args=default_args)

dag.doc_md = __doc__

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = SimpleHttpOperator(
    task_id='post_op',
    endpoint='post',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.json()['json']['priority'] == 5,
    dag=dag,
)

t5 = SimpleHttpOperator(
    task_id='post_op_formenc',
    endpoint='post',
    data="name=Joe",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag,
)

t2 = SimpleHttpOperator(
    task_id='get_op',
    method='GET',
    endpoint='get',
    data={"param1": "value1", "param2": "value2"},
    headers={},
    dag=dag,
)

t3 = SimpleHttpOperator(
    task_id='put_op',
    method='PUT',
    endpoint='put',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    dag=dag,
)

t4 = SimpleHttpOperator(
    task_id='del_op',
    method='DELETE',
    endpoint='delete',
    data="some=data",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag,
)

sensor = HttpSensor(
    task_id='http_sensor_check',
    http_conn_id='http_default',
    endpoint='',
    request_params={},
    response_check=lambda response: "httpbin" in response.text,
    poke_interval=5,
    dag=dag,
)

sensor >> t1 >> t2 >> t3 >> t4 >> t5
