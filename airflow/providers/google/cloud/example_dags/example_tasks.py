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
from google.protobuf.field_mask_pb2 import FieldMask

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

timestamp = timestamp_pb2.Timestamp()
timestamp.FromDatetime(datetime.now() + timedelta(hours=12))

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
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Queue operations
    # [START create_queue]
    create_queue = CloudTasksQueueCreateOperator(
        location=LOCATION,
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=0.5)),
        queue_name=QUEUE_ID,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_queue",
    )
    # [END create_queue]

    # [START delete_queue]
    delete_queue = CloudTasksQueueDeleteOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="delete_queue",
    )
    # [END delete_queue]

    # [START resume_queue]
    resume_queue = CloudTasksQueueResumeOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="resume_queue",
    )
    # [END resume_queue]

    # [START pause_queue]
    pause_queue = CloudTasksQueuePauseOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="pause_queue",
    )
    # [END pause_queue]

    # [START purge_queue]
    purge_queue = CloudTasksQueuePurgeOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="purge_queue",
    )
    # [END purge_queue]

    # [START get_queue]
    get_queue = CloudTasksQueueGetOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_id="get_queue",
    )

    get_queue_result = BashOperator(
        task_id="get_queue_result",
        bash_command=f"echo {get_queue.output}",
    )
    # [END get_queue]

    get_queue >> get_queue_result

    # [START update_queue]
    update_queue = CloudTasksQueueUpdateOperator(
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=1)),
        location=LOCATION,
        queue_name=QUEUE_ID,
        update_mask=FieldMask(paths=["stackdriver_logging_config.sampling_ratio"]),
        task_id="update_queue",
    )
    # [END update_queue]

    # [START list_queue]
    list_queue = CloudTasksQueuesListOperator(location=LOCATION, task_id="list_queue")
    # [END list_queue]

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
    # [START create_task]
    create_task = CloudTasksTaskCreateOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task=TASK,
        task_name=TASK_NAME,
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_task_to_run",
    )
    # [END create_task]

    # [START tasks_get]
    tasks_get = CloudTasksTaskGetOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_name=TASK_NAME,
        task_id="tasks_get",
    )
    # [END tasks_get]

    # [START run_task]
    run_task = CloudTasksTaskRunOperator(
        location=LOCATION,
        queue_name=QUEUE_ID,
        task_name=TASK_NAME,
        task_id="run_task",
    )
    # [END run_task]

    # [START list_tasks]
    list_tasks = CloudTasksTasksListOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="list_tasks")
    # [END list_tasks]

    # [START delete_task]
    delete_task = CloudTasksTaskDeleteOperator(
        location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="delete_task"
    )
    # [END delete_task]

    chain(purge_queue, create_task, tasks_get, list_tasks, run_task, delete_task, delete_queue)
