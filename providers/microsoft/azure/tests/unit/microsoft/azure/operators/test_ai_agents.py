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
)
from airflow.providers.microsoft.azure.triggers.ai_agents import AzureAIAgentVersionTrigger

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
METADATA = {"team": "airflow"}
DESCRIPTION = "Airflow hosted agent"
BLUEPRINT_REFERENCE = {"type": "ManagedAgentIdentityBlueprint", "blueprint_id": "blueprint-1"}


class TestCreateAzureAIAgentOperator:
    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_waits_until_version_is_active(self, mock_create_version, mock_get_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "active"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            metadata=METADATA,
            description=DESCRIPTION,
            blueprint_reference=BLUEPRINT_REFERENCE,
            poll_interval=0,
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "version": "1", "status": "active"}
        mock_create_version.assert_called_once_with(
            mock.ANY,
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            metadata=METADATA,
            description=DESCRIPTION,
            blueprint_reference=BLUEPRINT_REFERENCE,
        )
        mock_get_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="1")
        assert operator.hook.conn_id == CONN_ID
        assert operator.hook.endpoint == ENDPOINT

    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_without_waiting(self, mock_create_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "version": "1", "status": "creating"}

    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_without_waiting_does_not_require_version(self, mock_create_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "status": "creating"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result == {"name": AGENT_NAME, "status": "creating"}

    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_raises_when_wait_response_has_no_version(self, mock_create_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "status": "creating"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
        )

        with pytest.raises(ValueError, match="did not include a version"):
            operator.execute(context={})

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_raises_when_version_fails(self, mock_create_version, mock_get_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "failed"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent", agent_name=AGENT_NAME, definition=DEFINITION
        )

        with pytest.raises(
            RuntimeError,
            match=f"Azure AI Hosted agent {AGENT_NAME} version 1 failed: No error details were returned.",
        ):
            operator.execute(context={})

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_raises_when_version_reaches_unknown_status(self, mock_create_version, mock_get_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "paused"}
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent", agent_name=AGENT_NAME, definition=DEFINITION
        )

        with pytest.raises(RuntimeError, match="unknown status paused"):
            operator.execute(context={})

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_raises_when_version_times_out(
        self, mock_create_version, mock_get_version, mock_monotonic, mock_sleep
    ):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
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

    @mock.patch.object(AzureAIAgentsHook, "create_agent_version", autospec=True)
    def test_execute_defers(self, mock_create_version):
        mock_create_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
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

    def test_execute_complete_requires_version_payload(self):
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            agent_name=AGENT_NAME,
            definition=DEFINITION,
        )

        with pytest.raises(RuntimeError, match="did not include version payload"):
            operator.execute_complete(context={}, event={"status": "success", "message": "active"})


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
        mock_create_version.assert_called_once_with(
            mock.ANY,
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            metadata=None,
            description=None,
            blueprint_reference=None,
        )
        mock_get_version.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="2")

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
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context={})

        trigger = exc.value.trigger
        assert isinstance(trigger, AzureAIAgentVersionTrigger)
        assert trigger.agent_version == "2"


class TestRunAzureAIAgentOperator:
    @mock.patch.object(AzureAIAgentsHook, "invoke_agent_responses", autospec=True)
    def test_execute_responses_protocol(self, mock_invoke):
        mock_invoke.return_value = {"output_text": "hello"}
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_name=AGENT_NAME,
            protocol="responses",
            input_data={"input": "hello"},
            user_isolation_key="user-key",
        )

        result = operator.execute(context={})

        assert result == {"output_text": "hello"}
        mock_invoke.assert_called_once_with(
            mock.ANY,
            agent_name=AGENT_NAME,
            input_data={"input": "hello"},
            user_isolation_key="user-key",
        )

    @mock.patch.object(AzureAIAgentsHook, "invoke_agent_invocations", autospec=True)
    def test_execute_invocations_protocol(self, mock_invoke):
        mock_invoke.return_value = {"result": "done"}
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_name=AGENT_NAME,
            protocol="invocations",
            input_data={"message": "hello"},
            agent_session_id="session-1",
            user_isolation_key="user-key",
        )

        result = operator.execute(context={})

        assert result == {"result": "done"}
        mock_invoke.assert_called_once_with(
            mock.ANY,
            agent_name=AGENT_NAME,
            input_data={"message": "hello"},
            agent_session_id="session-1",
            user_isolation_key="user-key",
        )

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
    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_deletes_agent(self, mock_delete_agent, mock_is_deleted):
        mock_delete_agent.return_value = {"object": "agent.deleted", "name": AGENT_NAME, "deleted": True}
        mock_is_deleted.return_value = True
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            poll_interval=0,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            result = operator.execute(context={})

        assert result == {"object": "agent.deleted", "name": AGENT_NAME, "deleted": True}
        mock_delete_agent.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, force=False)
        mock_is_deleted.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME)
        mock_log_info.assert_any_call("Azure AI Hosted %s was deleted.", f"agent {AGENT_NAME}")

    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_deletes_agent_with_force(self, mock_delete_agent):
        mock_delete_agent.return_value = {"object": "agent.deleted", "name": AGENT_NAME, "deleted": True}
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            force=True,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result == {"object": "agent.deleted", "name": AGENT_NAME, "deleted": True}
        mock_delete_agent.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, force=True)

    @mock.patch.object(AzureAIAgentsHook, "is_agent_version_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent_version", autospec=True)
    def test_execute_deletes_agent_version(self, mock_delete_version, mock_is_deleted):
        mock_delete_version.return_value = {"object": "agent.version.deleted", "deleted": True}
        mock_is_deleted.return_value = True
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            agent_version="2",
            poll_interval=0,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            result = operator.execute(context={})

        assert result == {"object": "agent.version.deleted", "deleted": True}
        mock_delete_version.assert_called_once_with(
            mock.ANY, agent_name=AGENT_NAME, agent_version="2", force=False
        )
        mock_is_deleted.assert_called_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="2")
        mock_log_info.assert_any_call("Azure AI Hosted %s was deleted.", f"agent {AGENT_NAME} version 2")

    @mock.patch.object(AzureAIAgentsHook, "delete_agent_version", autospec=True)
    def test_execute_deletes_agent_version_with_force(self, mock_delete_version):
        mock_delete_version.return_value = {"object": "agent.version.deleted", "deleted": True}
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent_version",
            agent_name=AGENT_NAME,
            agent_version="2",
            force=True,
            wait_for_completion=False,
        )

        result = operator.execute(context={})

        assert result == {"object": "agent.version.deleted", "deleted": True}
        mock_delete_version.assert_called_once_with(
            mock.ANY, agent_name=AGENT_NAME, agent_version="2", force=True
        )

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_polls_until_agent_is_deleted(self, mock_delete_agent, mock_is_deleted, mock_sleep):
        mock_delete_agent.return_value = {"object": "agent.deleted", "name": AGENT_NAME, "deleted": True}
        mock_is_deleted.side_effect = [False, False, True]
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            poll_interval=1,
        )

        operator.execute(context={})

        assert mock_is_deleted.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1)

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_raises_when_deletion_times_out(
        self, mock_delete_agent, mock_is_deleted, mock_monotonic, mock_sleep
    ):
        mock_is_deleted.return_value = False
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_name=AGENT_NAME,
            timeout=0,
            poll_interval=0,
        )

        with pytest.raises(TimeoutError, match="Timeout waiting for Azure AI Hosted agent"):
            operator.execute(context={})

        mock_sleep.assert_not_called()


@pytest.mark.parametrize(
    ("event", "exception", "message"),
    [
        (None, RuntimeError, "Trigger returned no event."),
        ({"status": "timeout", "message": "timed out"}, TimeoutError, "timed out"),
        ({"status": "error", "message": "failed"}, RuntimeError, "failed"),
        ({"status": "unknown"}, ValueError, "Unexpected trigger event status"),
    ],
)
def test_execute_complete_event_errors(event, exception, message):
    operator = CreateAzureAIAgentOperator(
        task_id="create_agent",
        agent_name=AGENT_NAME,
        definition=DEFINITION,
    )

    with pytest.raises(exception, match=message):
        operator.execute_complete(context={}, event=event)
