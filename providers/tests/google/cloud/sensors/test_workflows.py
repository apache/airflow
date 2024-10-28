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
from google.cloud.workflows.executions_v1beta import Execution

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.sensors.workflows import WorkflowExecutionSensor

BASE_PATH = "airflow.providers.google.cloud.sensors.workflows.{}"
LOCATION = "europe-west1"
WORKFLOW_ID = "workflow_id"
EXECUTION_ID = "execution_id"
PROJECT_ID = "airflow-testing"
METADATA = None
TIMEOUT = None
RETRY = None
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = None


class TestWorkflowExecutionSensor:
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_poke_success(self, mock_hook):
        mock_hook.return_value.get_execution.return_value = mock.MagicMock(
            state=Execution.State.SUCCEEDED
        )
        op = WorkflowExecutionSensor(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            request_timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.poke({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_execution.assert_called_once_with(
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert result is True

    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_poke_wait(self, mock_hook):
        mock_hook.return_value.get_execution.return_value = mock.MagicMock(
            state=Execution.State.ACTIVE
        )
        op = WorkflowExecutionSensor(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            request_timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.poke({})

        assert result is False

    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_poke_failure(self, mock_hook):
        mock_hook.return_value.get_execution.return_value = mock.MagicMock(
            state=Execution.State.FAILED
        )
        op = WorkflowExecutionSensor(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            request_timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        with pytest.raises(AirflowException):
            op.poke({})
