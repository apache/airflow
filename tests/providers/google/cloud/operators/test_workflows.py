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
import datetime
from unittest import mock

import pytz

from airflow.providers.google.cloud.operators.workflows import (
    WorkflowsCancelExecutionOperator,
    WorkflowsCreateExecutionOperator,
    WorkflowsCreateWorkflowOperator,
    WorkflowsDeleteWorkflowOperator,
    WorkflowsGetExecutionOperator,
    WorkflowsGetWorkflowOperator,
    WorkflowsListExecutionsOperator,
    WorkflowsListWorkflowsOperator,
    WorkflowsUpdateWorkflowOperator,
)

BASE_PATH = "airflow.providers.google.cloud.operators.workflows.{}"
LOCATION = "europe-west1"
WORKFLOW_ID = "workflow_id"
EXECUTION_ID = "execution_id"
WORKFLOW = {"aa": "bb"}
EXECUTION = {"ccc": "ddd"}
PROJECT_ID = "airflow-testing"
METADATA = None
TIMEOUT = None
RETRY = None
FILTER_ = "aaaa"
ORDER_BY = "bbb"
UPDATE_MASK = "aaa,bbb"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = None


class TestWorkflowsCreateWorkflowOperator:
    @mock.patch(BASE_PATH.format("Workflow"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(self, mock_hook, mock_object):
        op = WorkflowsCreateWorkflowOperator(
            task_id="test_task",
            workflow=WORKFLOW,
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.create_workflow.assert_called_once_with(
            workflow=WORKFLOW,
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert result == mock_object.to_dict.return_value


class TestWorkflowsUpdateWorkflowOperator:
    @mock.patch(BASE_PATH.format("Workflow"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(self, mock_hook, mock_object):
        op = WorkflowsUpdateWorkflowOperator(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            update_mask=UPDATE_MASK,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_workflow.assert_called_once_with(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        mock_hook.return_value.update_workflow.assert_called_once_with(
            workflow=mock_hook.return_value.get_workflow.return_value,
            update_mask=UPDATE_MASK,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert result == mock_object.to_dict.return_value


class TestWorkflowsDeleteWorkflowOperator:
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(
        self,
        mock_hook,
    ):
        op = WorkflowsDeleteWorkflowOperator(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.delete_workflow.assert_called_once_with(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestWorkflowsListWorkflowsOperator:
    @mock.patch(BASE_PATH.format("Workflow"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(self, mock_hook, mock_object):
        workflow_mock = mock.MagicMock()
        workflow_mock.start_time = datetime.datetime.now(tz=pytz.UTC) + datetime.timedelta(minutes=5)
        mock_hook.return_value.list_workflows.return_value = [workflow_mock]

        op = WorkflowsListWorkflowsOperator(
            task_id="test_task",
            location=LOCATION,
            project_id=PROJECT_ID,
            filter_=FILTER_,
            order_by=ORDER_BY,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.list_workflows.assert_called_once_with(
            location=LOCATION,
            project_id=PROJECT_ID,
            filter_=FILTER_,
            order_by=ORDER_BY,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert result == [mock_object.to_dict.return_value]


class TestWorkflowsGetWorkflowOperator:
    @mock.patch(BASE_PATH.format("Workflow"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(self, mock_hook, mock_object):
        op = WorkflowsGetWorkflowOperator(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.get_workflow.assert_called_once_with(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert result == mock_object.to_dict.return_value


class TestWorkflowExecutionsCreateExecutionOperator:
    @mock.patch(BASE_PATH.format("Execution"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    @mock.patch(BASE_PATH.format("WorkflowsCreateExecutionOperator.xcom_push"))
    def test_execute(self, mock_xcom, mock_hook, mock_object):
        mock_hook.return_value.create_execution.return_value.name = "name/execution_id"
        op = WorkflowsCreateExecutionOperator(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            execution=EXECUTION,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.create_execution.assert_called_once_with(
            workflow_id=WORKFLOW_ID,
            execution=EXECUTION,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        mock_xcom.assert_called_once_with({}, key="execution_id", value="execution_id")
        assert result == mock_object.to_dict.return_value


class TestWorkflowExecutionsCancelExecutionOperator:
    @mock.patch(BASE_PATH.format("Execution"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(self, mock_hook, mock_object):
        op = WorkflowsCancelExecutionOperator(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.cancel_execution.assert_called_once_with(
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert result == mock_object.to_dict.return_value


class TestWorkflowExecutionsListExecutionsOperator:
    @mock.patch(BASE_PATH.format("Execution"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(self, mock_hook, mock_object):
        execution_mock = mock.MagicMock()
        execution_mock.start_time = datetime.datetime.now(tz=pytz.UTC) + datetime.timedelta(minutes=5)
        mock_hook.return_value.list_executions.return_value = [execution_mock]

        op = WorkflowsListExecutionsOperator(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.list_executions.assert_called_once_with(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert result == [mock_object.to_dict.return_value]


class TestWorkflowExecutionsGetExecutionOperator:
    @mock.patch(BASE_PATH.format("Execution"))
    @mock.patch(BASE_PATH.format("WorkflowsHook"))
    def test_execute(self, mock_hook, mock_object):
        op = WorkflowsGetExecutionOperator(
            task_id="test_task",
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = op.execute({})

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

        assert result == mock_object.to_dict.return_value
