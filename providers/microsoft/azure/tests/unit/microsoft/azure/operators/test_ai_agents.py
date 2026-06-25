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
from airflow.providers.microsoft.azure.hooks.ai_agents import AzureAIAgentsHook
from airflow.providers.microsoft.azure.operators.ai_agents import (
    CreateAzureAIAgentOperator,
    DeleteAzureAIAgentOperator,
    RunAzureAIAgentOperator,
    UpdateAzureAIAgentOperator,
    validate_execute_complete_event,
)
from airflow.providers.microsoft.azure.triggers.ai_agents import (
    AzureAIAgentDeleteTrigger,
    AzureAIAgentVersionTrigger,
)

MODULE = "airflow.providers.microsoft.azure.operators.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_NAME = "agent-123"
DEFINITION = {
    "kind": "hosted",
    "container_configuration": {"image": "registry.azurecr.io/agent:v1"},
    "cpu": "1",
    "memory": "2Gi",
    "protocol_versions": [{"protocol": "responses", "version": "1.0.0"}],
}


class TestCreateAzureAIAgentOperator:
    def test_template_fields(self):
        assert CreateAzureAIAgentOperator.template_fields == (
            "agent_name",
            "definition",
            "azure_ai_agents_conn_id",
            "endpoint",
            "api_version",
        )

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute_waits_until_version_is_active(self, mock_create_agent, mock_get_version):
        mock_create_agent.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "active"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            poll_interval=0,
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "version": "1", "status": "active"}
        mock_create_agent.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, definition=DEFINITION)
        mock_get_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="1")
        assert operator.hook.conn_id == CONN_ID
        assert operator.hook.endpoint == ENDPOINT

    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute_without_waiting(self, mock_create_agent):
        mock_create_agent.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "version": "1", "status": "creating"}

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute_raises_when_version_fails(self, mock_create_agent, mock_get_version):
        mock_create_agent.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "failed"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent", agent_name=AGENT_NAME, definition=DEFINITION
        )

        with pytest.raises(RuntimeError, match="version 1 failed"):
            operator.execute(context={})

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute_raises_when_version_times_out(
        self, mock_create_agent, mock_get_version, mock_monotonic, mock_sleep
    ):
        mock_create_agent.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            timeout=0,
            poll_interval=0,
        )

        with pytest.raises(TimeoutError, match="Timeout waiting for Azure AI Hosted agent"):
            operator.execute(context={})

        mock_sleep.assert_not_called()

    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute_defers(self, mock_create_agent):
        mock_create_agent.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            deferrable=True,
            timeout=10,
            poll_interval=2,
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v2",
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context={})

        trigger = exc.value.trigger
        assert isinstance(trigger, AzureAIAgentVersionTrigger)
        assert trigger.azure_ai_agents_conn_id == CONN_ID
        assert trigger.endpoint == ENDPOINT
        assert trigger.api_version == "v2"
        assert trigger.agent_name == AGENT_NAME
        assert trigger.agent_version == "1"
        assert trigger.timeout == 10
        assert trigger.poll_interval == 2

    def test_execute_complete_success(self):
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
        )

        result = operator.execute_complete(
            context={},
            event={
                "status": "success",
                "message": "active",
                "version": {"name": AGENT_NAME, "version": "1", "status": "active"},
            },
        )

        assert result == {"name": AGENT_NAME, "version": "1", "status": "active"}

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute_waits_with_agent_create_response(self, mock_create_agent, mock_get_version):
        """CreateAzureAIAgentOperator must handle the POST /agents agent response (versions.latest)."""
        mock_create_agent.return_value = {
            "object": "agent",
            "id": AGENT_NAME,
            "versions": {"latest": {"object": "agent.version", "version": "1", "status": "creating"}},
        }
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "active"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            poll_interval=0,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "version": "1", "status": "active"}
        mock_get_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="1")

    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute_without_waiting_with_agent_create_response(self, mock_create_agent):
        """CreateAzureAIAgentOperator must handle the POST /agents agent response with wait_for_completion=False."""
        agent_response = {
            "object": "agent",
            "id": AGENT_NAME,
            "versions": {"latest": {"object": "agent.version", "version": "1", "status": "creating"}},
        }
        mock_create_agent.return_value = agent_response
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result == agent_response


class TestUpdateAzureAIAgentOperator:
    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_creates_new_version(self, mock_create_version, mock_get_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "2", "status": "creating"}
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "2", "status": "active"}
        operator = UpdateAzureAIAgentOperator(
            task_id="update_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            poll_interval=0,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "version": "2", "status": "active"}
        mock_create_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, definition=DEFINITION)
        mock_get_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="2")

    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_without_waiting(self, mock_create_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "2", "status": "creating"}
        operator = UpdateAzureAIAgentOperator(
            task_id="update_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "version": "2", "status": "creating"}
        mock_create_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, definition=DEFINITION)

    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_defers(self, mock_create_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "2", "status": "creating"}
        operator = UpdateAzureAIAgentOperator(
            task_id="update_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            deferrable=True,
            timeout=10,
            poll_interval=2,
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v2",
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context={})

        trigger = exc.value.trigger
        assert isinstance(trigger, AzureAIAgentVersionTrigger)
        assert trigger.azure_ai_agents_conn_id == CONN_ID
        assert trigger.endpoint == ENDPOINT
        assert trigger.api_version == "v2"
        assert trigger.agent_name == AGENT_NAME
        assert trigger.agent_version == "2"
        assert trigger.timeout == 10
        assert trigger.poll_interval == 2


class TestRunAzureAIAgentOperator:
    def test_template_fields(self):
        assert RunAzureAIAgentOperator.template_fields == (
            "agent_name",
            "input_data",
            "protocol",
            "azure_ai_agents_conn_id",
            "endpoint",
            "api_version",
        )

    @mock.patch.object(AzureAIAgentsHook, "invoke_agent_responses", autospec=True)
    def test_execute_responses_protocol(self, mock_invoke):
        mock_invoke.return_value = {"output_text": "hello"}
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_name=AGENT_NAME,
            protocol="responses",
            input_data={"input": "hello"},
        )

        result = operator.execute(context={})

        assert result == {"output_text": "hello"}
        mock_invoke.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, input_data={"input": "hello"})

    @mock.patch.object(AzureAIAgentsHook, "invoke_agent_invocations", autospec=True)
    def test_execute_invocations_protocol(self, mock_invoke):
        mock_invoke.return_value = {"result": "done"}
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_name=AGENT_NAME,
            protocol="invocations",
            input_data={"message": "hello"},
        )

        result = operator.execute(context={})

        assert result == {"result": "done"}
        mock_invoke.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, input_data={"message": "hello"})

    def test_execute_raises_for_unknown_protocol(self):
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_name=AGENT_NAME,
            protocol="unknown",
            input_data={"message": "hello"},
        )

        with pytest.raises(ValueError, match="protocol must be either"):
            operator.execute(context={})


class TestDeleteAzureAIAgentOperator:
    def test_template_fields(self):
        assert DeleteAzureAIAgentOperator.template_fields == (
            "agent_name",
            "agent_version",
            "azure_ai_agents_conn_id",
            "endpoint",
            "api_version",
        )

    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_deletes_agent(self, mock_delete_agent, mock_is_deleted):
        mock_is_deleted.return_value = True
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            poll_interval=0,
        )

        result = operator.execute(context={})

        assert result is None
        mock_delete_agent.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, force=False)
        mock_is_deleted.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME)

    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_deletes_agent_with_force(self, mock_delete_agent):
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            force=True,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result is None
        mock_delete_agent.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, force=True)

    @mock.patch.object(AzureAIAgentsHook, "is_agent_version_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent_version", autospec=True)
    def test_execute_deletes_agent_version(self, mock_delete_version, mock_is_deleted):
        mock_is_deleted.return_value = True
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            agent_version="2",
            poll_interval=0,
        )

        result = operator.execute(context={})

        assert result is None
        mock_delete_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="2")
        mock_is_deleted.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="2")

    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_without_waiting(self, mock_delete_agent):
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result is None
        mock_delete_agent.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, force=False)

    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_defers(self, mock_delete_agent):
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            deferrable=True,
            timeout=10,
            poll_interval=2,
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v2",
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context={})

        mock_delete_agent.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, force=False)
        trigger = exc.value.trigger
        assert isinstance(trigger, AzureAIAgentDeleteTrigger)
        assert trigger.azure_ai_agents_conn_id == CONN_ID
        assert trigger.endpoint == ENDPOINT
        assert trigger.api_version == "v2"
        assert trigger.agent_name == AGENT_NAME
        assert trigger.agent_version is None
        assert trigger.timeout == 10
        assert trigger.poll_interval == 2

    def test_execute_complete_success(self):
        operator = DeleteAzureAIAgentOperator(task_id="delete_agent", agent_name=AGENT_NAME)

        assert (
            operator.execute_complete(
                context={},
                event={"status": "success", "message": "deleted", "agent_name": AGENT_NAME},
            )
            is None
        )


@pytest.mark.parametrize(
    ("event", "exception", "message"),
    [
        (None, RuntimeError, "Trigger returned no event."),
        ({"status": "timeout", "message": "timed out"}, TimeoutError, "timed out"),
        ({"status": "error", "message": "failed"}, RuntimeError, "failed"),
        ({"status": "unknown"}, ValueError, "Unexpected trigger event status"),
    ],
)
def test_validate_execute_complete_event_errors(event, exception, message):
    with pytest.raises(exception, match=message):
        validate_execute_complete_event(event)
