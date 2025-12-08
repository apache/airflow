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
from google.cloud.dataform_v1beta1.types import Target, WorkflowInvocationAction

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationActionStateSensor

TEST_TASK_ID = "task_id"
TEST_PROJECT_ID = "test_project"
TEST_REGION = "us-central1"
TEST_REPOSITORY_ID = "test_repository_id"
TEST_WORKFLOW_INVOCATION_ID = "test_workflow_invocation_id"
TEST_TARGET_NAME = "test_target_name"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestDataformWorkflowInvocationActionStateSensor:
    @pytest.mark.parametrize(
        ("expected_status", "current_status", "sensor_return"),
        [
            (WorkflowInvocationAction.State.SUCCEEDED, WorkflowInvocationAction.State.SUCCEEDED, True),
            (WorkflowInvocationAction.State.SUCCEEDED, WorkflowInvocationAction.State.RUNNING, False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataform.DataformHook")
    def test_poke(
        self,
        mock_hook: mock.MagicMock,
        expected_status: WorkflowInvocationAction.State,
        current_status: WorkflowInvocationAction.State,
        sensor_return: bool,
    ):
        target = Target(database="", schema="", name=TEST_TARGET_NAME)
        workflow_invocation_action = WorkflowInvocationAction(target=target, state=current_status)
        mock_query_workflow_invocation_actions = mock_hook.return_value.query_workflow_invocation_actions
        mock_query_workflow_invocation_actions.return_value = [workflow_invocation_action]

        task = DataformWorkflowInvocationActionStateSensor(
            task_id=TEST_TASK_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
            target_name=TEST_TARGET_NAME,
            expected_statuses=[expected_status],
            failure_statuses=[],
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        results = task.poke(mock.MagicMock())

        assert sensor_return == results

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID, impersonation_chain=TEST_IMPERSONATION_CHAIN
        )
        mock_query_workflow_invocation_actions.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
        )

    @mock.patch("airflow.providers.google.cloud.sensors.dataform.DataformHook")
    def test_target_state_failure_raises_exception(self, mock_hook: mock.MagicMock):
        target = Target(database="", schema="", name=TEST_TARGET_NAME)
        workflow_invocation_action = WorkflowInvocationAction(
            target=target, state=WorkflowInvocationAction.State.FAILED
        )
        mock_query_workflow_invocation_actions = mock_hook.return_value.query_workflow_invocation_actions
        mock_query_workflow_invocation_actions.return_value = [workflow_invocation_action]

        task = DataformWorkflowInvocationActionStateSensor(
            task_id=TEST_TASK_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
            target_name=TEST_TARGET_NAME,
            expected_statuses=[WorkflowInvocationAction.State.SUCCEEDED],
            failure_statuses=[WorkflowInvocationAction.State.FAILED],
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

        with pytest.raises(AirflowException):
            task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID, impersonation_chain=TEST_IMPERSONATION_CHAIN
        )
        mock_query_workflow_invocation_actions.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
        )

    @mock.patch("airflow.providers.google.cloud.sensors.dataform.DataformHook")
    def test_target_not_found_raises_exception(self, mock_hook: mock.MagicMock):
        mock_query_workflow_invocation_actions = mock_hook.return_value.query_workflow_invocation_actions
        mock_query_workflow_invocation_actions.return_value = []

        task = DataformWorkflowInvocationActionStateSensor(
            task_id=TEST_TASK_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
            target_name=TEST_TARGET_NAME,
            expected_statuses=[WorkflowInvocationAction.State.SUCCEEDED],
            failure_statuses=[WorkflowInvocationAction.State.FAILED],
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

        with pytest.raises(AirflowException):
            task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID, impersonation_chain=TEST_IMPERSONATION_CHAIN
        )
        mock_query_workflow_invocation_actions.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            repository_id=TEST_REPOSITORY_ID,
            workflow_invocation_id=TEST_WORKFLOW_INVOCATION_ID,
        )
