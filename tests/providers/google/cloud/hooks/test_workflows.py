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

from unittest import mock

from airflow.providers.google.cloud.hooks.workflows import WorkflowsHook
from airflow.providers.google.common.consts import CLIENT_INFO

BASE_PATH = "airflow.providers.google.cloud.hooks.workflows.{}"
LOCATION = "europe-west1"
WORKFLOW_ID = "workflow_id"
EXECUTION_ID = "execution_id"
WORKFLOW = {"aa": "bb"}
EXECUTION = {"ccc": "ddd"}
PROJECT_ID = "airflow-testing"
METADATA = ()
TIMEOUT = None
RETRY = None
FILTER_ = "aaaa"
ORDER_BY = "bbb"
UPDATE_MASK = "aaa,bbb"

WORKFLOW_PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}"
WORKFLOW_NAME = f"projects/{PROJECT_ID}/locations/{LOCATION}/workflows/{WORKFLOW_ID}"
EXECUTION_PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}/workflows/{WORKFLOW_ID}"
EXECUTION_NAME = (
    f"projects/{PROJECT_ID}/locations/{LOCATION}/workflows/{WORKFLOW_ID}/executions/{EXECUTION_ID}"
)


def mock_init(*args, **kwargs):
    pass


class TestWorkflowsHook:
    def setup_method(self, _):
        with mock.patch(BASE_PATH.format("GoogleBaseHook.__init__"), new=mock_init):
            self.hook = WorkflowsHook(gcp_conn_id="test")

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_credentials"))
    @mock.patch(BASE_PATH.format("WorkflowsClient"))
    def test_get_workflows_client(self, mock_client, mock_get_credentials):
        self.hook.get_workflows_client()
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_credentials"))
    @mock.patch(BASE_PATH.format("ExecutionsClient"))
    def test_get_executions_client(self, mock_client, mock_get_credentials):
        self.hook.get_executions_client()
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=CLIENT_INFO,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_workflows_client"))
    def test_create_workflow(self, mock_client):
        result = self.hook.create_workflow(
            workflow=WORKFLOW,
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.create_workflow.return_value == result
        mock_client.return_value.create_workflow.assert_called_once_with(
            request=dict(workflow=WORKFLOW, workflow_id=WORKFLOW_ID, parent=WORKFLOW_PARENT),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_workflows_client"))
    def test_get_workflow(self, mock_client):
        result = self.hook.get_workflow(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.get_workflow.return_value == result
        mock_client.return_value.get_workflow.assert_called_once_with(
            request=dict(name=WORKFLOW_NAME),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_workflows_client"))
    def test_update_workflow(self, mock_client):
        result = self.hook.update_workflow(
            workflow=WORKFLOW,
            update_mask=UPDATE_MASK,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.update_workflow.return_value == result
        mock_client.return_value.update_workflow.assert_called_once_with(
            request=dict(
                workflow=WORKFLOW,
                update_mask=UPDATE_MASK,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_workflows_client"))
    def test_delete_workflow(self, mock_client):
        result = self.hook.delete_workflow(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.delete_workflow.return_value == result
        mock_client.return_value.delete_workflow.assert_called_once_with(
            request=dict(name=WORKFLOW_NAME),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_workflows_client"))
    def test_list_workflows(self, mock_client):
        result = self.hook.list_workflows(
            location=LOCATION,
            project_id=PROJECT_ID,
            filter_=FILTER_,
            order_by=ORDER_BY,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.list_workflows.return_value == result
        mock_client.return_value.list_workflows.assert_called_once_with(
            request=dict(
                parent=WORKFLOW_PARENT,
                filter=FILTER_,
                order_by=ORDER_BY,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_executions_client"))
    def test_create_execution(self, mock_client):
        result = self.hook.create_execution(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            execution=EXECUTION,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.create_execution.return_value == result
        mock_client.return_value.create_execution.assert_called_once_with(
            request=dict(
                parent=EXECUTION_PARENT,
                execution=EXECUTION,
            ),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_executions_client"))
    def test_get_execution(self, mock_client):
        result = self.hook.get_execution(
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.get_execution.return_value == result
        mock_client.return_value.get_execution.assert_called_once_with(
            request=dict(name=EXECUTION_NAME),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_executions_client"))
    def test_cancel_execution(self, mock_client):
        result = self.hook.cancel_execution(
            workflow_id=WORKFLOW_ID,
            execution_id=EXECUTION_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.cancel_execution.return_value == result
        mock_client.return_value.cancel_execution.assert_called_once_with(
            request=dict(name=EXECUTION_NAME),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

    @mock.patch(BASE_PATH.format("WorkflowsHook.get_executions_client"))
    def test_list_execution(self, mock_client):
        result = self.hook.list_executions(
            workflow_id=WORKFLOW_ID,
            location=LOCATION,
            project_id=PROJECT_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

        assert mock_client.return_value.list_executions.return_value == result
        mock_client.return_value.list_executions.assert_called_once_with(
            request=dict(parent=EXECUTION_PARENT),
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
