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

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.tasks_v2.types import Queue, Task

from airflow.providers.google.cloud.hooks.tasks import CloudTasksHook
from airflow.providers.google.common.consts import CLIENT_INFO

from providers.tests.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id

API_RESPONSE: dict[Any, Any] = {}
PROJECT_ID = "test-project"
LOCATION = "asia-east2"
FULL_LOCATION_PATH = "projects/test-project/locations/asia-east2"
QUEUE_ID = "test-queue"
FULL_QUEUE_PATH = "projects/test-project/locations/asia-east2/queues/test-queue"
TASK_NAME = "test-task"
FULL_TASK_PATH = "projects/test-project/locations/asia-east2/queues/test-queue/tasks/test-task"


def mock_patch_return_object(attribute: str, return_value: Any) -> object:
    class Obj:
        pass

    obj = Obj()
    obj.__setattr__(attribute, mock.MagicMock(return_value=return_value))
    return obj


class TestCloudTasksHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            CloudTasksHook(gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudTasksHook(gcp_conn_id="test")

    @mock.patch("airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.tasks.CloudTasksClient")
    def test_cloud_tasks_client_creation(self, mock_client, mock_get_creds):
        result = self.hook.get_conn()
        mock_client.assert_called_once_with(credentials=mock_get_creds.return_value, client_info=CLIENT_INFO)
        assert mock_client.return_value == result
        assert self.hook._client == result

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("create_queue", API_RESPONSE),
    )
    def test_create_queue(self, get_conn):
        result = self.hook.create_queue(
            location=LOCATION,
            task_queue=Queue(),
            queue_name=QUEUE_ID,
            project_id=PROJECT_ID,
        )

        assert result is API_RESPONSE

        get_conn.return_value.create_queue.assert_called_once_with(
            request=dict(parent=FULL_LOCATION_PATH, queue=Queue(name=FULL_QUEUE_PATH)),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("update_queue", API_RESPONSE),
    )
    def test_update_queue(self, get_conn):
        result = self.hook.update_queue(
            task_queue=Queue(state=3),
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=PROJECT_ID,
        )

        assert result is API_RESPONSE

        get_conn.return_value.update_queue.assert_called_once_with(
            request=dict(queue=Queue(name=FULL_QUEUE_PATH, state=3), update_mask=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("get_queue", API_RESPONSE),
    )
    def test_get_queue(self, get_conn):
        result = self.hook.get_queue(location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID)

        assert result is API_RESPONSE

        get_conn.return_value.get_queue.assert_called_once_with(
            request=dict(name=FULL_QUEUE_PATH), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("list_queues", [Queue(name=FULL_QUEUE_PATH)]),
    )
    def test_list_queues(self, get_conn):
        result = self.hook.list_queues(location=LOCATION, project_id=PROJECT_ID)

        assert result == [Queue(name=FULL_QUEUE_PATH)]

        get_conn.return_value.list_queues.assert_called_once_with(
            request=dict(parent=FULL_LOCATION_PATH, filter=None, page_size=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("delete_queue", None),
    )
    def test_delete_queue(self, get_conn):
        result = self.hook.delete_queue(location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID)

        assert result is None

        get_conn.return_value.delete_queue.assert_called_once_with(
            request=dict(name=FULL_QUEUE_PATH), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("purge_queue", Queue(name=FULL_QUEUE_PATH)),
    )
    def test_purge_queue(self, get_conn):
        result = self.hook.purge_queue(location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID)

        assert result == Queue(name=FULL_QUEUE_PATH)

        get_conn.return_value.purge_queue.assert_called_once_with(
            request=dict(name=FULL_QUEUE_PATH), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("pause_queue", Queue(name=FULL_QUEUE_PATH)),
    )
    def test_pause_queue(self, get_conn):
        result = self.hook.pause_queue(location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID)

        assert result == Queue(name=FULL_QUEUE_PATH)

        get_conn.return_value.pause_queue.assert_called_once_with(
            request=dict(name=FULL_QUEUE_PATH), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("resume_queue", Queue(name=FULL_QUEUE_PATH)),
    )
    def test_resume_queue(self, get_conn):
        result = self.hook.resume_queue(location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID)

        assert result == Queue(name=FULL_QUEUE_PATH)

        get_conn.return_value.resume_queue.assert_called_once_with(
            request=dict(name=FULL_QUEUE_PATH), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("create_task", Task(name=FULL_TASK_PATH)),
    )
    def test_create_task(self, get_conn):
        result = self.hook.create_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task=Task(),
            project_id=PROJECT_ID,
            task_name=TASK_NAME,
        )

        assert result == Task(name=FULL_TASK_PATH)

        get_conn.return_value.create_task.assert_called_once_with(
            request=dict(parent=FULL_QUEUE_PATH, task=Task(name=FULL_TASK_PATH), response_view=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("get_task", Task(name=FULL_TASK_PATH)),
    )
    def test_get_task(self, get_conn):
        result = self.hook.get_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=PROJECT_ID,
        )

        assert result == Task(name=FULL_TASK_PATH)

        get_conn.return_value.get_task.assert_called_once_with(
            request=dict(name=FULL_TASK_PATH, response_view=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("list_tasks", [Task(name=FULL_TASK_PATH)]),
    )
    def test_list_tasks(self, get_conn):
        result = self.hook.list_tasks(location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID)

        assert result == [Task(name=FULL_TASK_PATH)]

        get_conn.return_value.list_tasks.assert_called_once_with(
            request=dict(parent=FULL_QUEUE_PATH, response_view=None, page_size=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("delete_task", None),
    )
    def test_delete_task(self, get_conn):
        result = self.hook.delete_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=PROJECT_ID,
        )

        assert result is None

        get_conn.return_value.delete_task.assert_called_once_with(
            request=dict(name=FULL_TASK_PATH), retry=DEFAULT, timeout=None, metadata=()
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.tasks.CloudTasksHook.get_conn",
        return_value=mock_patch_return_object("run_task", Task(name=FULL_TASK_PATH)),
    )
    def test_run_task(self, get_conn):
        result = self.hook.run_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=PROJECT_ID,
        )

        assert result == Task(name=FULL_TASK_PATH)

        get_conn.return_value.run_task.assert_called_once_with(
            request=dict(name=FULL_TASK_PATH, response_view=None),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
