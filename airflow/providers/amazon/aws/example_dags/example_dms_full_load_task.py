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
from datetime import timedelta
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.dms_create_task import DmsCreateTaskOperator
from airflow.providers.amazon.aws.operators.dms_delete_task import DmsDeleteTaskOperator
from airflow.providers.amazon.aws.operators.dms_start_task import DmsStartTaskOperator
from airflow.providers.amazon.aws.sensors.dms_task import DmsTaskCompletedSensor
from airflow.utils.dates import days_ago

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

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='dms_full_load_task_run_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval='0 3 * * *',
    tags=['example'],
) as dag:

    # [START howto_dms_create_task_operator]
    create_task = DmsCreateTaskOperator(
        task_id='create_task',
        replication_task_id=REPLICATION_TASK_ID,
        source_endpoint_arn=SOURCE_ENDPOINT_ARN,
        target_endpoint_arn=TARGET_ENDPOINT_ARN,
        replication_instance_arn=REPLICATION_INSTANCE_ARN,
        table_mappings=TABLE_MAPPINGS,
    )
    # [END howto_dms_create_task_operator]

    # [START howto_dms_start_task_operator]
    start_task = DmsStartTaskOperator(
        task_id='start_task',
        replication_task_arn='{{ task_instance.xcom_pull(task_ids="create_task", key="return_value") }}',
    )
    # [END howto_dms_start_task_operator]

    # [START howto_dms_task_completed_sensor]
    wait_for_completion = DmsTaskCompletedSensor(
        task_id='wait_for_completion',
        replication_task_arn='{{ task_instance.xcom_pull(task_ids="create_task", key="return_value") }}',
    )
    # [END howto_dms_task_completed_sensor]

    # [START howto_dms_delete_task_operator]
    delete_task = DmsDeleteTaskOperator(
        task_id='delete_task',
        replication_task_arn='{{ task_instance.xcom_pull(task_ids="create_task", key="return_value") }}',
    )
    # [END howto_dms_delete_task_operator]

    create_task >> start_task >> wait_for_completion >> delete_task
