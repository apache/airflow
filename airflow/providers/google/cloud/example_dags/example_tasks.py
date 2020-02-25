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
Example Airflow DAG that creates, gets, lists, updates, purges, pauses, resumes
and deletes Queues and creates, gets, lists, runs and deletes Tasks in the Google
Cloud Tasks service in the Google Cloud Platform.
"""


from datetime import datetime, timedelta

from google.api_core.retry import Retry
from google.cloud.tasks_v2.types import Queue
from google.protobuf import timestamp_pb2

from airflow import DAG
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueCreateOperator, CloudTasksTaskCreateOperator, CloudTasksTaskRunOperator,
)
from airflow.utils.dates import days_ago

default_args = {"start_date": days_ago(1)}
timestamp = timestamp_pb2.Timestamp()
timestamp.FromDatetime(datetime.now() + timedelta(hours=12))  # pylint: disable=no-member

LOCATION = "asia-east2"
QUEUE_ID = "cloud-tasks-queue"
TASK_NAME = "task-to-run"


TASK = {
    "app_engine_http_request": {  # Specify the type of request.
        "http_method": "POST",
        "relative_uri": "/example_task_handler",
        "body": "Hello".encode(),
    },
    "schedule_time": timestamp,
}

with DAG("example_gcp_tasks", default_args=default_args, schedule_interval=None, tags=['example'],) as dag:

    create_queue = CloudTasksQueueCreateOperator(
        location=LOCATION,
        task_queue=Queue(),
        queue_name=QUEUE_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_queue",
    )

    create_task_to_run = CloudTasksTaskCreateOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task=TASK,
        task_name=TASK_NAME,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_task_to_run",
    )

    run_task = CloudTasksTaskRunOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_name=TASK_NAME,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="run_task",
    )

    create_queue >> create_task_to_run >> run_task
