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

from unittest import mock

import pytest
from google.cloud.tasks_v2.types import Queue

from airflow.providers.google.cloud.links.cloud_tasks import (
    CLOUD_TASKS_LINK,
    CLOUD_TASKS_QUEUE_LINK,
    CloudTasksLink,
    CloudTasksQueueLink,
)
from airflow.providers.google.cloud.operators.tasks import (
    CloudTasksQueueCreateOperator,
    CloudTasksQueuesListOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

PROJECT_ID = "test-project"
LOCATION = "asia-east2"
QUEUE_ID = "test-queue"
FULL_QUEUE_PATH = f"projects/{PROJECT_ID}/locations/{LOCATION}/queues/{QUEUE_ID}"
TEST_QUEUE = Queue(name=FULL_QUEUE_PATH)

EXPECTED_CLOUD_TASKS_QUEUE_URL = (
    f"https://console.cloud.google.com/cloudtasks/queue/{LOCATION}/{QUEUE_ID}/tasks?project={PROJECT_ID}"
)
EXPECTED_CLOUD_TASKS_URL = f"https://console.cloud.google.com/cloudtasks?project={PROJECT_ID}"


def mock_context():
    """Build a context whose ``task`` exposes no extra link params, as ``GoogleCloudBaseOperator`` does."""
    return {"ti": mock.MagicMock(), "task": mock.MagicMock(extra_links_params={})}


class TestCloudTasksQueueLink:
    def test_class_attributes(self):
        assert CloudTasksQueueLink.key == "cloud_task_queue"
        assert CloudTasksQueueLink.name == "Cloud Tasks Queue"
        assert CloudTasksQueueLink.format_str == CLOUD_TASKS_QUEUE_LINK

    @pytest.mark.parametrize(
        ("queue_name", "expected_parts"),
        [
            pytest.param(FULL_QUEUE_PATH, (PROJECT_ID, LOCATION, QUEUE_ID), id="full-resource-name"),
            pytest.param(None, ("", "", ""), id="none"),
            pytest.param("", ("", "", ""), id="empty-string"),
        ],
    )
    def test_extract_parts(self, queue_name, expected_parts):
        assert CloudTasksQueueLink.extract_parts(queue_name) == expected_parts

    def test_persist(self):
        context = mock_context()

        CloudTasksQueueLink.persist(context=context, queue_name=FULL_QUEUE_PATH)

        context["ti"].xcom_push.assert_called_once_with(
            key=CloudTasksQueueLink.key,
            value={"project_id": PROJECT_ID, "location": LOCATION, "queue_id": QUEUE_ID},
        )

    def test_persist_without_queue_name(self):
        context = mock_context()

        CloudTasksQueueLink.persist(context=context)

        context["ti"].xcom_push.assert_called_once_with(
            key=CloudTasksQueueLink.key,
            value={"project_id": "", "location": "", "queue_id": ""},
        )

    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        link = CloudTasksQueueLink()
        ti = create_task_instance_of_operator(
            CloudTasksQueueCreateOperator,
            dag_id="test_cloud_tasks_queue_link_dag",
            task_id="test_cloud_tasks_queue_link_task",
            location=LOCATION,
            project_id=PROJECT_ID,
            task_queue=TEST_QUEUE,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        link.persist(context={"ti": ti, "task": task}, queue_name=FULL_QUEUE_PATH)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={"project_id": PROJECT_ID, "location": LOCATION, "queue_id": QUEUE_ID},
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == EXPECTED_CLOUD_TASKS_QUEUE_URL


class TestCloudTasksLink:
    def test_class_attributes(self):
        assert CloudTasksLink.key == "cloud_task"
        assert CloudTasksLink.name == "Cloud Tasks"
        assert CloudTasksLink.format_str == CLOUD_TASKS_LINK

    def test_persist(self):
        context = mock_context()

        CloudTasksLink.persist(context=context, project_id=PROJECT_ID)

        context["ti"].xcom_push.assert_called_once_with(
            key=CloudTasksLink.key,
            value={"project_id": PROJECT_ID},
        )

    @pytest.mark.db_test
    def test_get_link(self, dag_maker, create_task_instance_of_operator, session, mock_supervisor_comms):
        link = CloudTasksLink()
        ti = create_task_instance_of_operator(
            CloudTasksQueuesListOperator,
            dag_id="test_cloud_tasks_link_dag",
            task_id="test_cloud_tasks_link_task",
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        task = dag_maker.dag.get_task(ti.task_id)

        link.persist(context={"ti": ti, "task": task}, project_id=PROJECT_ID)

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={"project_id": PROJECT_ID},
            )
        actual_url = link.get_link(operator=task, ti_key=ti.key)
        assert actual_url == EXPECTED_CLOUD_TASKS_URL
