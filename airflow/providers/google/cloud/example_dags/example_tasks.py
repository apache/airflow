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
Cloud Tasks service in the Google Cloud.
"""
import os
from datetime import datetime, timedelta

from google.api_core.retry import Retry
from google.cloud.tasks_v2.types import Queue
from google.protobuf import timestamp_pb2

from airflow import models
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueCreateOperator,
    CloudTasksQueueDeleteOperator,
    CloudTasksQueueGetOperator,
    CloudTasksQueuePauseOperator,
    CloudTasksQueuePurgeOperator,
    CloudTasksQueueResumeOperator,
    CloudTasksQueuesListOperator,
    CloudTasksQueueUpdateOperator,
    CloudTasksTaskCreateOperator,
    CloudTasksTaskDeleteOperator,
    CloudTasksTaskGetOperator,
    CloudTasksTaskRunOperator,
    CloudTasksTasksListOperator,
)
from airflow.utils.dates import days_ago

timestamp = timestamp_pb2.Timestamp()
timestamp.FromDatetime(datetime.now() + timedelta(hours=12))  # pylint: disable=no-member

LOCATION = "europe-west1"
QUEUE_ID = os.environ.get('GCP_TASKS_QUEUE_ID', "cloud-tasks-queue")
TASK_NAME = "task-to-run"


TASK = {
    "app_engine_http_request": {  # Specify the type of request.
        "http_method": "POST",
        "relative_uri": "/example_task_handler",
        "body": b"Hello",
    },
    "schedule_time": timestamp,
}

with models.DAG(
    "example_gcp_tasks",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    # Queue operations
    create_queue = CloudTasksQueueCreateOperator(
        location=LOCATION,
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=0.5)),
        queue_name=QUEUE_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_queue",
    )

    delete_queue = CloudTasksQueueDeleteOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="delete_queue",
    )

    resume_queue = CloudTasksQueueResumeOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="resume_queue",
    )

    pause_queue = CloudTasksQueuePauseOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="pause_queue",
    )

    purge_queue = CloudTasksQueuePurgeOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="purge_queue",
    )

    get_queue = CloudTasksQueueGetOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="get_queue",
    )

    get_queue_result = BashOperator(
        task_id="get_queue_result",
        bash_command="echo \"{{ task_instance.xcom_pull('get_queue') }}\"",
    )
    get_queue >> get_queue_result

    update_queue = CloudTasksQueueUpdateOperator(
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=1)),
        location=LOCATION,
        queue_name=QUEUE_ID,
        update_mask={"paths": ["stackdriver_logging_config.sampling_ratio"]},
        task_id="update_queue",
    )

    list_queue = CloudTasksQueuesListOperator(location=LOCATION, task_id="list_queue")

    chain(
        create_queue,
        update_queue,
        pause_queue,
        resume_queue,
        purge_queue,
        get_queue,
        list_queue,
        delete_queue,
    )

    # Tasks operations
    create_task = CloudTasksTaskCreateOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task=TASK,
        task_name=TASK_NAME,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_task_to_run",
    )

    tasks_get = CloudTasksTaskGetOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_name=TASK_NAME,
        task_id="tasks_get",
    )

    run_task = CloudTasksTaskRunOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_name=TASK_NAME,
        task_id="run_task",
    )

    list_tasks = CloudTasksTasksListOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="list_tasks")

    delete_task = CloudTasksTaskDeleteOperator(
        location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="delete_task"
    )

    chain(purge_queue, create_task, tasks_get, list_tasks, run_task, delete_task, delete_queue)
