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
Example Airflow DAG that creates and deletes Queues and creates, gets, lists,
runs and deletes Tasks in the Google Cloud Tasks service in the Google Cloud.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from google.api_core.retry import Retry
from google.cloud.tasks_v2.types import Queue
from google.protobuf import timestamp_pb2

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueCreateOperator,
    CloudTasksQueueDeleteOperator,
    CloudTasksTaskCreateOperator,
    CloudTasksTaskDeleteOperator,
    CloudTasksTaskGetOperator,
    CloudTasksTaskRunOperator,
    CloudTasksTasksListOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "cloud_tasks_tasks"

timestamp = timestamp_pb2.Timestamp()
timestamp.FromDatetime(datetime.now() + timedelta(hours=12))

LOCATION = "us-central1"
# queue cannot use recent names even if queue was removed
QUEUE_ID = f"queue-{ENV_ID}-{DAG_ID.replace('_', '-')}"
TASK_NAME = "task-to-run"
TASK = {
    "http_request": {
        "http_method": "POST",
        "url": "http://www.example.com/example",
        "body": b"",
    },
    "schedule_time": timestamp,
}

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "tasks"],
) as dag:

    @task(task_id="random_string")
    def generate_random_string():
        """
        Generate random string for queue and task names.
        Queue name cannot be repeated in preceding 7 days and
        task name in the last 1 hour.
        """
        import random
        import string

        return "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

    random_string = generate_random_string()

    create_queue = CloudTasksQueueCreateOperator(
        location=LOCATION,
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=0.5)),
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_queue",
    )

    delete_queue = CloudTasksQueueDeleteOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="delete_queue",
    )
    delete_queue.trigger_rule = TriggerRule.ALL_DONE

    # [START create_task]
    create_task = CloudTasksTaskCreateOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task=TASK,
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_task_to_run",
    )
    # [END create_task]

    # [START tasks_get]
    tasks_get = CloudTasksTaskGetOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="tasks_get",
    )
    # [END tasks_get]

    # [START run_task]
    run_task = CloudTasksTaskRunOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        retry=Retry(maximum=10.0),
        task_id="run_task",
    )
    # [END run_task]

    # [START list_tasks]
    list_tasks = CloudTasksTasksListOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="list_tasks",
    )
    # [END list_tasks]

    # [START delete_task]
    delete_task = CloudTasksTaskDeleteOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_name=TASK_NAME + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="delete_task",
    )
    # [END delete_task]

    chain(
        random_string,
        create_queue,
        create_task,
        tasks_get,
        list_tasks,
        run_task,
        delete_task,
        delete_queue,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
