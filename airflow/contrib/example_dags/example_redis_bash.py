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

from __future__ import print_function

import airflow
from airflow import DAG
from airflow.contrib.sensors.redis_pub_sub_sensor import RedisPubSubSensor
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True,
}

dag = DAG('redis_bash_v1', schedule_interval=None, default_args=args)

redis_sensor = RedisPubSubSensor(task_id='redis_sensor_id', channels='test',
                                 redis_conn_id='redis_default', dag=dag, )

bash_echo_task = BashOperator(
    task_id='bash_echo_id',
    bash_command='echo "{{ ti.xcom_pull(key="message") }}"',
    dag=dag,
)

redis_sensor >> bash_echo_task
