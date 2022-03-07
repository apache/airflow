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
from datetime import datetime
from json import loads
from os import environ

from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.amazon.aws.sensors.batch import BatchSensor

# The inputs below are required for the submit batch example DAG.
JOB_NAME = environ.get('BATCH_JOB_NAME', 'example_job_name')
JOB_DEFINITION = environ.get('BATCH_JOB_DEFINITION', 'example_job_definition')
JOB_QUEUE = environ.get('BATCH_JOB_QUEUE', 'example_job_queue')
JOB_OVERRIDES = loads(environ.get('BATCH_JOB_OVERRIDES', '{}'))

# An existing (externally triggered) job id is required for the sensor example DAG.
JOB_ID = environ.get('BATCH_JOB_ID', '00000000-0000-0000-0000-000000000000')


with DAG(
    dag_id='example_batch_submit_job',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as submit_dag:

    # [START howto_operator_batch]
    submit_batch_job = BatchOperator(
        task_id='submit_batch_job',
        job_name=JOB_NAME,
        job_queue=JOB_QUEUE,
        job_definition=JOB_DEFINITION,
        overrides=JOB_OVERRIDES,
    )
    # [END howto_operator_batch]

with DAG(
    dag_id='example_batch_wait_for_job_sensor',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as sensor_dag:

    # [START howto_sensor_batch]
    wait_for_batch_job = BatchSensor(
        task_id='wait_for_batch_job',
        job_id=JOB_ID,
    )
    # [END howto_sensor_batch]
