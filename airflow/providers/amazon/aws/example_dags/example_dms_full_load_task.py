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
"""
This is an example dag for running full load DMS replication task.
"""
from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.dms import (
    DmsCreateTaskOperator,
    DmsDeleteTaskOperator,
    DmsStartTaskOperator,
)
from airflow.providers.amazon.aws.sensors.dms import DmsTaskCompletedSensor

REPLICATION_TASK_ID = getenv('REPLICATION_TASK_ID', 'full-load-test-export')
SOURCE_ENDPOINT_ARN = getenv('SOURCE_ENDPOINT_ARN', 'source_endpoint_arn')
TARGET_ENDPOINT_ARN = getenv('TARGET_ENDPOINT_ARN', 'target_endpoint_arn')
REPLICATION_INSTANCE_ARN = getenv('REPLICATION_INSTANCE_ARN', 'replication_instance_arn')
TABLE_MAPPINGS = {
    'rules': [
        {
            'rule-type': 'selection',
            'rule-id': '1',
            'rule-name': '1',
            'object-locator': {
                'schema-name': 'test',
                'table-name': '%',
            },
            'rule-action': 'include',
        }
    ]
}


with DAG(
    dag_id='dms_full_load_task_run_dag',
    dagrun_timeout=timedelta(hours=2),
    start_date=datetime(2021, 1, 1),
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['example'],
) as dag:

    # [START howto_dms_operators]
    create_task = DmsCreateTaskOperator(
        task_id='create_task',
        replication_task_id=REPLICATION_TASK_ID,
        source_endpoint_arn=SOURCE_ENDPOINT_ARN,
        target_endpoint_arn=TARGET_ENDPOINT_ARN,
        replication_instance_arn=REPLICATION_INSTANCE_ARN,
        table_mappings=TABLE_MAPPINGS,
    )

    start_task = DmsStartTaskOperator(
        task_id='start_task',
        replication_task_arn=create_task.output,
    )

    wait_for_completion = DmsTaskCompletedSensor(
        task_id='wait_for_completion',
        replication_task_arn=create_task.output,
    )

    delete_task = DmsDeleteTaskOperator(
        task_id='delete_task',
        replication_task_arn=create_task.output,
    )
    # [END howto_dms_operators]

    start_task >> wait_for_completion >> delete_task

    # Task dependencies created via `XComArgs`:
    #   create_task >> start_task
    #   create_task >> wait_for_completion
    #   create_task >> delete_task
