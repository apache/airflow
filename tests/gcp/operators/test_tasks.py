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

import unittest

from google.cloud.tasks_v2.types import Queue, Task

from airflow.gcp.operators.tasks import (
    CloudTasksQueueCreateOperator,
    CloudTasksTaskCreateOperator,
    CloudTasksQueueDeleteOperator,
    CloudTasksTaskDeleteOperator,
    CloudTasksQueueGetOperator,
    CloudTasksTaskGetOperator,
    CloudTasksQueuesListOperator,
    CloudTasksTasksListOperator,
    CloudTasksQueuePauseOperator,
    CloudTasksQueuePurgeOperator,
    CloudTasksQueueResumeOperator,
    CloudTasksTaskRunOperator,
    CloudTasksQueueUpdateOperator,
)
from tests.compat import mock

GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "test-project"
LOCATION = "asia-east2"
FULL_LOCATION_PATH = "projects/test-project/locations/asia-east2"
QUEUE_ID = "test-queue"
FULL_QUEUE_PATH = "projects/test-project/locations/asia-east2/queues/test-queue"
TASK_NAME = "test-task"
FULL_TASK_PATH = (
    "projects/test-project/locations/asia-east2/queues/test-queue/tasks/test-task"
)


class TestCloudTasksQueueCreate(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_create_queue(self, mock_hook):
        mock_hook.return_value.create_queue.return_value = {}
        operator = CloudTasksQueueCreateOperator(
            location=LOCATION, task_queue=Queue(), task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_queue.assert_called_once_with(
            location=LOCATION,
            task_queue=Queue(),
            project_id=None,
            queue_name=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksQueueUpdate(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_update_queue(self, mock_hook):
        mock_hook.return_value.update_queue.return_value = {}
        operator = CloudTasksQueueUpdateOperator(
            task_queue=Queue(name=FULL_QUEUE_PATH), task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_queue.assert_called_once_with(
            task_queue=Queue(name=FULL_QUEUE_PATH),
            project_id=None,
            location=None,
            queue_name=None,
            update_mask=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksQueueGet(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_get_queue(self, mock_hook):
        mock_hook.return_value.get_queue.return_value = {}
        operator = CloudTasksQueueGetOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.get_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksQueuesList(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_list_queues(self, mock_hook):
        mock_hook.return_value.list_queues.return_value = {}
        operator = CloudTasksQueuesListOperator(location=LOCATION, task_id="id")
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.list_queues.assert_called_once_with(
            location=LOCATION,
            project_id=None,
            results_filter=None,
            page_size=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksQueueDelete(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_delete_queue(self, mock_hook):
        mock_hook.return_value.delete_queue.return_value = {}
        operator = CloudTasksQueueDeleteOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksQueuePurge(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_delete_queue(self, mock_hook):
        mock_hook.return_value.purge_queue.return_value = {}
        operator = CloudTasksQueuePurgeOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.purge_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksQueuePause(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_pause_queue(self, mock_hook):
        mock_hook.return_value.pause_queue.return_value = {}
        operator = CloudTasksQueuePauseOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.pause_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksQueueResume(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_resume_queue(self, mock_hook):
        mock_hook.return_value.resume_queue.return_value = {}
        operator = CloudTasksQueueResumeOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.resume_queue.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksTaskCreate(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_create_task(self, mock_hook):
        mock_hook.return_value.create_task.return_value = {}
        operator = CloudTasksTaskCreateOperator(
            location=LOCATION, queue_name=QUEUE_ID, task=Task(), task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task=Task(),
            project_id=None,
            task_name=None,
            response_view=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksTaskGet(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_get_task(self, mock_hook):
        mock_hook.return_value.get_task.return_value = {}
        operator = CloudTasksTaskGetOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.get_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=None,
            response_view=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksTasksList(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_list_tasks(self, mock_hook):
        mock_hook.return_value.list_tasks.return_value = {}
        operator = CloudTasksTasksListOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.list_tasks.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            project_id=None,
            response_view=None,
            page_size=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksTaskDelete(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_delete_task(self, mock_hook):
        mock_hook.return_value.delete_task.return_value = {}
        operator = CloudTasksTaskDeleteOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=None,
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestCloudTasksTaskRun(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.tasks.CloudTasksHook")
    def test_run_task(self, mock_hook):
        mock_hook.return_value.run_task.return_value = {}
        operator = CloudTasksTaskRunOperator(
            location=LOCATION, queue_name=QUEUE_ID, task_name=TASK_NAME, task_id="id"
        )
        operator.execute(context=None)
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.run_task.assert_called_once_with(
            location=LOCATION,
            queue_name=QUEUE_ID,
            task_name=TASK_NAME,
            project_id=None,
            response_view=None,
            retry=None,
            timeout=None,
            metadata=None,
        )
