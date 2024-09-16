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
and deletes Queues in the Google Cloud Tasks service in the Google Cloud.

Required setup:
- GCP_APP_ENGINE_LOCATION: GCP Project's App Engine location `gcloud app describe | grep locationId`.
"""

from __future__ import annotations

import os
from datetime import datetime

from google.api_core.retry import Retry
from google.cloud.tasks_v2.types import Queue
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueCreateOperator,
    CloudTasksQueueDeleteOperator,
    CloudTasksQueueGetOperator,
    CloudTasksQueuePauseOperator,
    CloudTasksQueuePurgeOperator,
    CloudTasksQueueResumeOperator,
    CloudTasksQueuesListOperator,
    CloudTasksQueueUpdateOperator,
)
from airflow.providers.standard.core.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "cloud_tasks_queue"

LOCATION = os.environ.get("GCP_APP_ENGINE_LOCATION", "europe-west2")
QUEUE_ID = f"queue-{ENV_ID}-{DAG_ID.replace('_', '-')}"


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

    # [START create_queue]
    create_queue = CloudTasksQueueCreateOperator(
        location=LOCATION,
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=0.5)),
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        retry=Retry(maximum=10.0),
        timeout=5,
        task_id="create_queue",
    )
    # [END create_queue]

    # [START delete_queue]
    delete_queue = CloudTasksQueueDeleteOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="delete_queue",
    )
    # [END delete_queue]
    delete_queue.trigger_rule = TriggerRule.ALL_DONE

    # [START resume_queue]
    resume_queue = CloudTasksQueueResumeOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="resume_queue",
    )
    # [END resume_queue]

    # [START pause_queue]
    pause_queue = CloudTasksQueuePauseOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="pause_queue",
    )
    # [END pause_queue]

    # [START purge_queue]
    purge_queue = CloudTasksQueuePurgeOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="purge_queue",
    )
    # [END purge_queue]

    # [START get_queue]
    get_queue = CloudTasksQueueGetOperator(
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        task_id="get_queue",
    )

    get_queue_result = BashOperator(
        task_id="get_queue_result",
        bash_command=f"echo {get_queue.output}",
    )
    # [END get_queue]

    # [START update_queue]
    update_queue = CloudTasksQueueUpdateOperator(
        task_queue=Queue(stackdriver_logging_config=dict(sampling_ratio=1)),
        location=LOCATION,
        queue_name=QUEUE_ID + "{{ task_instance.xcom_pull(task_ids='random_string') }}",
        update_mask=FieldMask(paths=["stackdriver_logging_config.sampling_ratio"]),
        task_id="update_queue",
    )
    # [END update_queue]

    # [START list_queue]
    list_queue = CloudTasksQueuesListOperator(location=LOCATION, task_id="list_queue")
    # [END list_queue]

    chain(
        random_string,
        create_queue,
        update_queue,
        pause_queue,
        resume_queue,
        purge_queue,
        get_queue,
        get_queue_result,
        list_queue,
        delete_queue,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
