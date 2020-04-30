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

from datetime import timedelta

from airflow import DAG
from airflow.providers.apache.kafka.sensors.kafka_sensor import KafkaSensor
from airflow.utils.timezone import datetime

DAG_ID = "example_kafka_dag"
dag_start_date = datetime(2015, 6, 1, hour=20, tzinfo=None)
default_args = {
    'owner': '@Ferg_In',
    'depends_on_past': False,
    'start_date': dag_start_date,
    'email': ['dferguson992@gmail.com'],
    'provide_context': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=None,
         max_active_runs=1, concurrency=4, catchup=False) as dag:

    sensor = KafkaSensor(
        task_id='trigger',
        topic='',
        host='',
        port=''
    )
