# -*- coding: utf-8 -*-
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
#
import unittest
from typing import Dict, Any

from google.cloud.tasks_v2.types import Queue, Task

from airflow.gcp.hooks.tasks import CloudTasksHook
from tests.compat import mock
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id


API_RESPONSE = {}  # type: Dict[Any, Any]
PROJECT_ID = "test-project"
LOCATION = "asia-east2"
FULL_LOCATION_PATH = "projects/test-project/locations/asia-east2"
QUEUE_ID = "test-queue"
FULL_QUEUE_PATH = "projects/test-project/locations/asia-east2/queues/test-queue"
TASK_NAME = "test-task"
FULL_TASK_PATH = (
    "projects/test-project/locations/asia-east2/queues/test-queue/tasks/test-task"
)


class TestCloudTasksHook(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            "airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = CloudTasksHook(gcp_conn_id="test")

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.create_queue.return_value": API_RESPONSE},  # type: ignore
    )
    def test_create_queue(self, get_conn):
        result = self.hook.create_queue(
            location=LOCATION,
            task_queue=Queue(),
            queue_name=QUEUE_ID,
            project_id=PROJECT_ID,
        )

        self.assertIs(result, API_RESPONSE)

        get_conn.return_value.create_queue.assert_called_once_with(
            parent=FULL_LOCATION_PATH,
            queue=Queue(name=FULL_QUEUE_PATH),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.update_queue.return_value": API_RESPONSE},  # type: ignore
    )
    def test_update_queue(self, get_conn):
        result = self.hook.update_queue(
            task_queue=Queue(state=3),
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=PROJECT_ID,
        )

        self.assertIs(result, API_RESPONSE)

        get_conn.return_value.update_queue.assert_called_once_with(
            queue=Queue(name=FULL_QUEUE_PATH, state=3),
            update_mask=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.get_queue.return_value": API_RESPONSE},  # type: ignore
    )
    def test_get_queue(self, get_conn):
        result = self.hook.get_queue(
            location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID
        )

        self.assertIs(result, API_RESPONSE)

        get_conn.return_value.get_queue.assert_called_once_with(
            name=FULL_QUEUE_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.list_queues.return_value": API_RESPONSE},  # type: ignore
    )
    def test_list_queues(self, get_conn):
        result = self.hook.list_queues(location=LOCATION, project_id=PROJECT_ID)

        self.assertEqual(result, list(API_RESPONSE))

        get_conn.return_value.list_queues.assert_called_once_with(
            parent=FULL_LOCATION_PATH,
            filter_=None,
            page_size=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.delete_queue.return_value": API_RESPONSE},  # type: ignore
    )
    def test_delete_queue(self, get_conn):
        result = self.hook.delete_queue(
            location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID
        )

        self.assertEqual(result, None)

        get_conn.return_value.delete_queue.assert_called_once_with(
            name=FULL_QUEUE_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.purge_queue.return_value": API_RESPONSE},  # type: ignore
    )
    def test_purge_queue(self, get_conn):
        result = self.hook.purge_queue(
            location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID
        )

        self.assertEqual(result, API_RESPONSE)

        get_conn.return_value.purge_queue.assert_called_once_with(
            name=FULL_QUEUE_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.pause_queue.return_value": API_RESPONSE},  # type: ignore
    )
    def test_pause_queue(self, get_conn):
        result = self.hook.pause_queue(
            location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID
        )

        self.assertEqual(result, API_RESPONSE)

        get_conn.return_value.pause_queue.assert_called_once_with(
            name=FULL_QUEUE_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.resume_queue.return_value": API_RESPONSE},  # type: ignore
    )
    def test_resume_queue(self, get_conn):
        result = self.hook.resume_queue(
            location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID
        )

        self.assertEqual(result, API_RESPONSE)

        get_conn.return_value.resume_queue.assert_called_once_with(
            name=FULL_QUEUE_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.create_task.return_value": API_RESPONSE},  # type: ignore
    )
    def test_create_task(self, get_conn):
        result = self.hook.create_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task=Task(),
            project_id=PROJECT_ID,
            task_name=TASK_NAME,
        )

        self.assertEqual(result, API_RESPONSE)

        get_conn.return_value.create_task.assert_called_once_with(
            parent=FULL_QUEUE_PATH,
            task=Task(name=FULL_TASK_PATH),
            response_view=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.get_task.return_value": API_RESPONSE},  # type: ignore
    )
    def test_get_task(self, get_conn):
        result = self.hook.get_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=PROJECT_ID,
        )

        self.assertEqual(result, API_RESPONSE)

        get_conn.return_value.get_task.assert_called_once_with(
            name=FULL_TASK_PATH,
            response_view=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.list_tasks.return_value": API_RESPONSE},  # type: ignore
    )
    def test_list_tasks(self, get_conn):
        result = self.hook.list_tasks(
            location=LOCATION, queue_name=QUEUE_ID, project_id=PROJECT_ID
        )

        self.assertEqual(result, list(API_RESPONSE))

        get_conn.return_value.list_tasks.assert_called_once_with(
            parent=FULL_QUEUE_PATH,
            response_view=None,
            page_size=None,
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.delete_task.return_value": API_RESPONSE},  # type: ignore
    )
    def test_delete_task(self, get_conn):
        result = self.hook.delete_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=PROJECT_ID,
        )

        self.assertEqual(result, None)

        get_conn.return_value.delete_task.assert_called_once_with(
            name=FULL_TASK_PATH, retry=None, timeout=None, metadata=None
        )

    @mock.patch(  # type: ignore
        "airflow.gcp.hooks.tasks.CloudTasksHook.get_conn",
        **{"return_value.run_task.return_value": API_RESPONSE},  # type: ignore
    )
    def test_run_task(self, get_conn):
        result = self.hook.run_task(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=PROJECT_ID,
        )

        self.assertEqual(result, API_RESPONSE)

        get_conn.return_value.run_task.assert_called_once_with(
            name=FULL_TASK_PATH,
            response_view=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
