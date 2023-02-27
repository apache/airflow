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
from __future__ import annotations

from typing import Any
from unittest import mock

from google.cloud.tasks_v2.types import Task

from airflow.providers.google.cloud.sensors.tasks import TaskQueueEmptySensor

API_RESPONSE: dict[Any, Any] = {}
PROJECT_ID = "test-project"
LOCATION = "asia-east2"
FULL_LOCATION_PATH = "projects/test-project/locations/asia-east2"
QUEUE_ID = "test-queue"
FULL_QUEUE_PATH = "projects/test-project/locations/asia-east2/queues/test-queue"
TASK_NAME = "test-task"
FULL_TASK_PATH = "projects/test-project/locations/asia-east2/queues/test-queue/tasks/test-task"


class TestCloudTasksEmptySensor:
    @mock.patch("airflow.providers.google.cloud.sensors.tasks.CloudTasksHook")
    def test_queue_empty(self, mock_hook):

        operator = TaskQueueEmptySensor(
            task_id=TASK_NAME, location=LOCATION, project_id=PROJECT_ID, queue_name=QUEUE_ID, poke_interval=0
        )

        result = operator.poke(mock.MagicMock)

        assert result is True

    @mock.patch("airflow.providers.google.cloud.sensors.tasks.CloudTasksHook")
    def test_queue_not_empty(self, mock_hook):
        mock_hook.return_value.list_tasks.return_value = [Task(name=FULL_TASK_PATH)]

        operator = TaskQueueEmptySensor(
            task_id=TASK_NAME, location=LOCATION, project_id=PROJECT_ID, queue_name=QUEUE_ID, poke_interval=0
        )

        result = operator.poke(mock.MagicMock)

        assert result is False
