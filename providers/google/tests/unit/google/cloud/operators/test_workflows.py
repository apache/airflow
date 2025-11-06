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

import datetime
import json
import re
from unittest import mock

import pendulum
from google.protobuf.timestamp_pb2 import Timestamp

from airflow.models.dagrun import DagRun
from airflow.providers.common.compat.sdk import Context
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
from airflow.utils.hashlib_wrapper import md5

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

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
        context = mock.MagicMock()
        result = op.execute(context=context)

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

    def test_execute_without_workflow_id(self):
        op = WorkflowsCreateWorkflowOperator(
            task_id="test_task",
            workflow=WORKFLOW,
            workflow_id="",
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        hash_base = json.dumps(WORKFLOW, sort_keys=True)
        date = pendulum.datetime(2025, 1, 1)
        ctx = Context(logical_date=date)
        if AIRFLOW_V_3_0_PLUS:
            ctx["dag_run"] = DagRun(run_after=date)
        expected = md5(f"airflow_{op.dag_id}_test_task_{date.isoformat()}_{hash_base}".encode()).hexdigest()
        assert op._workflow_id(ctx) == re.sub(r"[:\-+.]", "_", expected)

        if AIRFLOW_V_3_0_PLUS:
            ctx = Context(dag_run=DagRun(run_after=date))
            assert op._workflow_id(ctx) == re.sub(r"[:\-+.]", "_", expected)


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
        context = mock.MagicMock()
        result = op.execute(context=context)

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
        timestamp = Timestamp()
        timestamp.FromDatetime(
            datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(minutes=5)
        )
        workflow_mock = mock.MagicMock()
        workflow_mock.start_time = timestamp
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
        context = mock.MagicMock()
        result = op.execute(context=context)

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
        context = mock.MagicMock()
        result = op.execute(context=context)

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
    @mock.patch(BASE_PATH.format("WorkflowsExecutionLink.persist"))
    def test_execute(self, mock_link_persist, mock_hook, mock_object):
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
        context = mock.MagicMock()
        result = op.execute(context=context)

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
        mock_link_persist.assert_called_with(
            context=context,
            location_id=LOCATION,
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            project_id=PROJECT_ID,
        )
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
        context = mock.MagicMock()
        result = op.execute(context=context)

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
        start_date_filter = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(minutes=5)
        execution_mock = mock.MagicMock()
        execution_mock.start_time = start_date_filter
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
        context = mock.MagicMock()
        result = op.execute(context=context)

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
        context = mock.MagicMock()
        result = op.execute(context=context)

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
