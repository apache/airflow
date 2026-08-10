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

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.google.cloud.operators.vertex_ai.agent_engine import (
    CreateAgentEngineOperator,
    DeleteAgentEngineOperator,
    GetAgentEngineOperator,
    RunQueryJobOperator,
    UpdateAgentEngineOperator,
)

AGENT_ENGINE_PATH = "airflow.providers.google.cloud.operators.vertex_ai.agent_engine.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
AGENT_ENGINE_ID = "123"
AGENT_ENGINE_NAME = "projects/test-project/locations/us-central1/reasoningEngines/123"
CONFIG = {"display_name": "test-agent-engine"}
QUERY_CONFIG = {"query": "hello", "output_gcs_uri": "gs://test-bucket/query-output/"}
CHECK_QUERY_CONFIG = {"retrieve_result": True}
OPERATION = {"name": "operations/delete-123", "done": False}
QUERY_OPERATION_NAME = "operations/query-123"
OPERATION_ID = "delete-123"
QUERY_OPERATION_ID = "query-123"


class FakeModel:
    def __init__(self, payload):
        self.payload = payload
        for key, value in payload.items():
            setattr(self, key, value)

    def model_dump(self, mode="json"):
        return self.payload


class FakeAgentEngine:
    def __init__(self, payload):
        self.api_resource = FakeModel(payload)


@pytest.fixture
def context():
    return {"ti": mock.Mock(spec_set=["xcom_push"])}


def assert_hook_created(mock_hook):
    mock_hook.assert_called_once_with(
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )


class TestCreateAgentEngineOperator:
    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute(self, mock_hook, context):
        mock_hook.return_value.create_agent_engine.return_value = FakeAgentEngine(
            {"name": AGENT_ENGINE_NAME, "display_name": "test-agent-engine"}
        )
        op = CreateAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            config=CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        assert_hook_created(mock_hook)
        mock_hook.return_value.create_agent_engine.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent=None,
            config=CONFIG,
        )
        assert result == {"name": AGENT_ENGINE_NAME, "display_name": "test-agent-engine"}


class TestGetAgentEngineOperator:
    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute(self, mock_hook, context):
        mock_hook.return_value.get_agent_engine.return_value = FakeAgentEngine({"name": AGENT_ENGINE_NAME})
        op = GetAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        mock_hook.return_value.get_agent_engine.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=None,
        )
        assert result == {"name": AGENT_ENGINE_NAME}

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_with_config(self, mock_hook, context):
        mock_hook.return_value.get_agent_engine.return_value = FakeAgentEngine({"name": AGENT_ENGINE_NAME})
        op = GetAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(context=context)

        mock_hook.return_value.get_agent_engine.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=CONFIG,
        )


class TestRunQueryJobOperator:
    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute(self, mock_hook, context):
        run_result_payload = {
            "job_name": "operations/query-123",
            "input_gcs_uri": "gs://test-bucket/query-output/input.json",
            "output_gcs_uri": "gs://test-bucket/query-output/output.json",
        }
        query_result_payload = {
            "operation_name": QUERY_OPERATION_NAME,
            "output_gcs_uri": "gs://test-bucket/query-output/output.json",
            "status": "SUCCESS",
            "result": "done",
        }
        mock_hook.return_value.run_query_job.return_value = FakeModel(run_result_payload)
        mock_hook.return_value.wait_for_query_agent_engine_job.return_value = FakeModel(query_result_payload)
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=QUERY_CONFIG,
            check_config=CHECK_QUERY_CONFIG,
            poll_interval=1,
            timeout=60,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        mock_hook.return_value.run_query_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=QUERY_CONFIG,
        )
        mock_hook.return_value.wait_for_query_agent_engine_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
            poll_interval=1,
            timeout=60,
        )
        assert result == query_result_payload

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_without_wait(self, mock_hook, context):
        result_payload = {
            "job_name": "operations/query-123",
            "input_gcs_uri": "gs://test-bucket/query-output/input.json",
            "output_gcs_uri": "gs://test-bucket/query-output/output.json",
        }
        mock_hook.return_value.run_query_job.return_value = FakeModel(result_payload)
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            config=CHECK_QUERY_CONFIG,
            agent_engine_id=AGENT_ENGINE_ID,
            wait_for_completion=False,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        mock_hook.return_value.wait_for_query_agent_engine_job.assert_not_called()
        assert result == result_payload

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineQueryJobTrigger"), autospec=True)
    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_deferrable(self, mock_hook, mock_trigger, context):
        mock_hook.return_value.run_query_job.return_value = FakeModel({"job_name": QUERY_OPERATION_NAME})
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=CHECK_QUERY_CONFIG,
            check_config=CHECK_QUERY_CONFIG,
            poll_interval=1,
            timeout=60,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred):
            op.execute(context=context)

        mock_hook.return_value.wait_for_query_agent_engine_job.assert_not_called()
        mock_trigger.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            poll_interval=1,
            timeout=60,
        )

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_raises_when_query_job_has_no_name(self, mock_hook, context):
        mock_hook.return_value.run_query_job.return_value = FakeModel({})
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
        )

        with pytest.raises(RuntimeError, match="Agent Engine query job did not include an operation name."):
            op.execute(context=context)

        mock_hook.return_value.wait_for_query_agent_engine_job.assert_not_called()

    def test_execute_complete_success(self, context):
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
        )
        query_job = {"operation_name": QUERY_OPERATION_NAME, "status": "SUCCESS"}

        result = op.execute_complete(
            context=context,
            event={"status": "success", "message": "done", "query_job": query_job},
        )

        assert result == query_job

    def test_execute_complete_error(self, context):
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
        )

        with pytest.raises(RuntimeError, match="boom"):
            op.execute_complete(context=context, event={"status": "error", "message": "boom"})

    def test_execute_complete_timeout(self, context):
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
        )

        with pytest.raises(TimeoutError, match="timed out"):
            op.execute_complete(context=context, event={"status": "timeout", "message": "timed out"})

    def test_execute_complete_without_event(self, context):
        op = RunQueryJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
        )

        with pytest.raises(RuntimeError, match="No event received in trigger callback"):
            op.execute_complete(context=context)


class TestUpdateAgentEngineOperator:
    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute(self, mock_hook, context):
        mock_hook.return_value.update_agent_engine.return_value = FakeAgentEngine(
            {"name": AGENT_ENGINE_NAME, "display_name": "updated-agent-engine"}
        )
        op = UpdateAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        mock_hook.return_value.update_agent_engine.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            agent=None,
            config=CONFIG,
        )
        assert result == {"name": AGENT_ENGINE_NAME, "display_name": "updated-agent-engine"}


class TestDeleteAgentEngineOperator:
    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_without_wait(self, mock_hook, context):
        mock_hook.return_value.delete_agent_engine.return_value = FakeModel(OPERATION)
        op = DeleteAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            force=True,
            config=CONFIG,
            wait_for_completion=False,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        mock_hook.return_value.delete_agent_engine.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            force=True,
            config=CONFIG,
        )
        mock_hook.return_value.wait_for_agent_engine_operation.assert_not_called()
        assert result == OPERATION

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_waits_until_deleted(self, mock_hook, context):
        mock_hook.return_value.delete_agent_engine.return_value = FakeModel(OPERATION)
        op = DeleteAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            wait_for_completion=True,
            poll_interval=1,
            timeout=60,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        mock_hook.return_value.wait_for_agent_engine_operation.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=OPERATION_ID,
            poll_interval=1,
            timeout=60,
        )
        assert result == OPERATION

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_does_not_wait_when_delete_operation_is_done(self, mock_hook, context):
        operation = {"name": "operations/delete-123", "done": True}
        mock_hook.return_value.delete_agent_engine.return_value = FakeModel(operation)
        op = DeleteAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            wait_for_completion=True,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        result = op.execute(context=context)

        mock_hook.return_value.wait_for_agent_engine_operation.assert_not_called()
        assert result == operation

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_raises_when_completed_delete_operation_has_error(self, mock_hook, context):
        operation = {
            "name": "operations/delete-123",
            "done": True,
            "error": {"message": "Permission denied"},
        }
        mock_hook.return_value.delete_agent_engine.return_value = FakeModel(operation)
        op = DeleteAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            wait_for_completion=True,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        with pytest.raises(
            RuntimeError,
            match=r"Agent Engine operation operations/delete-123 failed: \{'message': 'Permission denied'\}",
        ):
            op.execute(context=context)

        mock_hook.return_value.wait_for_agent_engine_operation.assert_not_called()

    @mock.patch(AGENT_ENGINE_PATH.format("AgentEngineHook"), autospec=True)
    def test_execute_raises_when_delete_operation_has_no_name(self, mock_hook, context):
        mock_hook.return_value.delete_agent_engine.return_value = FakeModel({"done": False})
        op = DeleteAgentEngineOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            wait_for_completion=True,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        with pytest.raises(
            RuntimeError, match=r"Delete Agent Engine operation did not include an operation name\."
        ):
            op.execute(context=context)

        mock_hook.return_value.wait_for_agent_engine_operation.assert_not_called()
