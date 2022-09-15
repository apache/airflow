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

import unittest
from unittest import mock

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.tasks_v2.types import Queue, Task

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

GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "test-project"
LOCATION = "asia-east2"
FULL_LOCATION_PATH = "projects/test-project/locations/asia-east2"
QUEUE_ID = "test-queue"
FULL_QUEUE_PATH = "projects/test-project/locations/asia-east2/queues/test-queue"
TASK_NAME = "test-task"
FULL_TASK_PATH = "projects/test-project/locations/asia-east2/queues/test-queue/tasks/test-task"
TEST_QUEUE = Queue(name=FULL_QUEUE_PATH)
TEST_TASK = Task(app_engine_http_request={})


class TestCloudTasksQueueCreate(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_create_queue(self, mock_hook):
        mock_hook.return_value.create_queue.return_value = TEST_QUEUE
        operator = CloudTasksQueueCreateOperator(location=LOCATION, task_queue=TEST_QUEUE, task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert {'name': FULL_QUEUE_PATH, 'state': 0} == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_queue.assert_called_once_with(
            location=LOCATION,
            task_queue=TEST_QUEUE,
            project_id=None,
            queue_name=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksQueueUpdate(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_update_queue(self, mock_hook):
        mock_hook.return_value.update_queue.return_value = TEST_QUEUE
        operator = CloudTasksQueueUpdateOperator(task_queue=Queue(name=FULL_QUEUE_PATH), task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert {'name': FULL_QUEUE_PATH, 'state': 0} == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.update_queue.assert_called_once_with(
            task_queue=Queue(name=FULL_QUEUE_PATH),
            project_id=None,
            location=None,
            queue_name=None,
            update_mask=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksQueueGet(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_get_queue(self, mock_hook):
        mock_hook.return_value.get_queue.return_value = TEST_QUEUE
        operator = CloudTasksQueueGetOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert {'name': FULL_QUEUE_PATH, 'state': 0} == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksQueuesList(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_list_queues(self, mock_hook):
        mock_hook.return_value.list_queues.return_value = [TEST_QUEUE]
        operator = CloudTasksQueuesListOperator(location=LOCATION, task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert [{'name': FULL_QUEUE_PATH, 'state': 0}] == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_queues.assert_called_once_with(
            location=LOCATION,
            project_id=None,
            results_filter=None,
            page_size=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksQueueDelete(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_delete_queue(self, mock_hook):
        mock_hook.return_value.delete_queue.return_value = None
        operator = CloudTasksQueueDeleteOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="id")

        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksQueuePurge(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_delete_queue(self, mock_hook):
        mock_hook.return_value.purge_queue.return_value = TEST_QUEUE
        operator = CloudTasksQueuePurgeOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert {'name': FULL_QUEUE_PATH, 'state': 0} == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.purge_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksQueuePause(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_pause_queue(self, mock_hook):
        mock_hook.return_value.pause_queue.return_value = TEST_QUEUE
        operator = CloudTasksQueuePauseOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert {'name': FULL_QUEUE_PATH, 'state': 0} == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.pause_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksQueueResume(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_resume_queue(self, mock_hook):
        mock_hook.return_value.resume_queue.return_value = TEST_QUEUE
        operator = CloudTasksQueueResumeOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert {'name': FULL_QUEUE_PATH, 'state': 0} == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.resume_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksTaskCreate(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_create_task(self, mock_hook):
        mock_hook.return_value.create_task.return_value = TEST_TASK
        operator = CloudTasksTaskCreateOperator(
            location=LOCATION, queue_name=QUEUE_ID, task=Task(), task_id="id"
        )

        result = operator.execute(context=mock.MagicMock())

        assert {
            'app_engine_http_request': {'body': '', 'headers': {}, 'http_method': 0, 'relative_uri': ''},
            'dispatch_count': 0,
            'name': '',
            'response_count': 0,
            'view': 0,
        } == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.create_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task=Task(),
            project_id=None,
            task_name=None,
            response_view=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksTaskGet(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_get_task(self, mock_hook):
        mock_hook.return_value.get_task.return_value = TEST_TASK
        operator = CloudTasksTaskGetOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="id"
        )

        result = operator.execute(context=mock.MagicMock())

        assert {
            'app_engine_http_request': {'body': '', 'headers': {}, 'http_method': 0, 'relative_uri': ''},
            'dispatch_count': 0,
            'name': '',
            'response_count': 0,
            'view': 0,
        } == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.get_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=None,
            response_view=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksTasksList(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_list_tasks(self, mock_hook):
        mock_hook.return_value.list_tasks.return_value = [TEST_TASK]
        operator = CloudTasksTasksListOperator(location=LOCATION, queue_name=QUEUE_ID, task_id="id")

        result = operator.execute(context=mock.MagicMock())

        assert [
            {
                'app_engine_http_request': {
                    'body': '',
                    'headers': {},
                    'http_method': 0,
                    'relative_uri': '',
                },
                'dispatch_count': 0,
                'name': '',
                'response_count': 0,
                'view': 0,
            }
        ] == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.list_tasks.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            response_view=None,
            page_size=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksTaskDelete(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_delete_task(self, mock_hook):
        mock_hook.return_value.delete_task.return_value = None
        operator = CloudTasksTaskDeleteOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="id"
        )

        operator.execute(context=mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.delete_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )


class TestCloudTasksTaskRun(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.tasks.CloudTasksHook")
    def test_run_task(self, mock_hook):
        mock_hook.return_value.run_task.return_value = TEST_TASK
        operator = CloudTasksTaskRunOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="id"
        )

        result = operator.execute(context=mock.MagicMock())

        assert {
            'app_engine_http_request': {'body': '', 'headers': {}, 'http_method': 0, 'relative_uri': ''},
            'dispatch_count': 0,
            'name': '',
            'response_count': 0,
            'view': 0,
        } == result
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=None,
        )
        mock_hook.return_value.run_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=None,
            response_view=None,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
