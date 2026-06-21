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

from types import SimpleNamespace
from unittest import mock

import pytest

pytest.importorskip("azure.ai.agents")

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
    AzureAIAgentRunTrigger,
)

MODULE = "airflow.providers.microsoft.azure.operators.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_ID = "agent_123"


def create_run(status: str = "completed", **kwargs) -> SimpleNamespace:
    return SimpleNamespace(id="run_123", thread_id="thread_123", status=status, **kwargs)


class TestCreateAzureAIAgentOperator:
    def test_template_fields(self):
        assert CreateAzureAIAgentOperator.template_fields == (
            "model",
            "config",
            "azure_ai_agents_conn_id",
            "endpoint",
        )

    @mock.patch.object(AzureAIAgentsHook, "create_agent", autospec=True)
    def test_execute(self, mock_create_agent):
        mock_create_agent.return_value = SimpleNamespace(id=AGENT_ID, name="test-agent")
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            model="gpt-4o",
            config={"name": "test-agent"},
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
        )

        result = operator.execute(context={})

        assert result == {"id": AGENT_ID, "name": "test-agent"}
        mock_create_agent.assert_called_once_with(mock.ANY, model="gpt-4o", name="test-agent")
        assert operator.hook.conn_id == CONN_ID
        assert operator.hook.endpoint == ENDPOINT

    def test_execute_raises_when_model_is_passed_in_config(self):
        operator = CreateAzureAIAgentOperator(
            task_id="create_agent",
            model="gpt-4o",
            config={"model": "gpt-4o-mini"},
        )

        with pytest.raises(ValueError, match="Pass 'model' as the operator parameter"):
            operator.execute(context={})


class TestUpdateAzureAIAgentOperator:
    def test_template_fields(self):
        assert UpdateAzureAIAgentOperator.template_fields == (
            "agent_id",
            "config",
            "azure_ai_agents_conn_id",
            "endpoint",
        )

    @mock.patch.object(AzureAIAgentsHook, "update_agent", autospec=True)
    def test_execute(self, mock_update_agent):
        mock_update_agent.return_value = SimpleNamespace(id=AGENT_ID, instructions="updated")
        operator = UpdateAzureAIAgentOperator(
            task_id="update_agent",
            agent_id=AGENT_ID,
            config={"instructions": "updated"},
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
        )

        result = operator.execute(context={})

        assert result == {"id": AGENT_ID, "instructions": "updated"}
        mock_update_agent.assert_called_once_with(mock.ANY, agent_id=AGENT_ID, instructions="updated")


class TestRunAzureAIAgentOperator:
    def test_template_fields(self):
        assert RunAzureAIAgentOperator.template_fields == (
            "agent_id",
            "config",
            "azure_ai_agents_conn_id",
            "endpoint",
        )

    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_without_waiting(self, mock_run_agent):
        mock_run_agent.return_value = create_run(status="queued")
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_id=AGENT_ID,
            config={"thread": {"messages": []}},
            wait_for_completion=False,
            azure_ai_agents_conn_id=CONN_ID,
        )

        result = operator.execute(context={})

        assert result == {"id": "run_123", "thread_id": "thread_123", "status": "queued"}
        mock_run_agent.assert_called_once_with(mock.ANY, agent_id=AGENT_ID, thread={"messages": []})

    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_waits_until_run_completes(self, mock_run_agent, mock_get_run):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.return_value = create_run(status="completed")
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_id=AGENT_ID,
            poll_interval=0,
            azure_ai_agents_conn_id=CONN_ID,
        )

        result = operator.execute(context={})

        assert result == {"id": "run_123", "thread_id": "thread_123", "status": "completed"}
        mock_get_run.assert_called_once_with(mock.ANY, thread_id="thread_123", run_id="run_123")

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_raises_when_run_fails(self, mock_run_agent, mock_get_run, mock_sleep):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.return_value = create_run(status="failed")
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID, poll_interval=0)

        with pytest.raises(RuntimeError, match="finished with status failed"):
            operator.execute(context={})

        mock_sleep.assert_not_called()

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_raises_when_run_times_out(
        self, mock_run_agent, mock_get_run, mock_monotonic, mock_sleep
    ):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.return_value = create_run(status="in_progress")
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_id=AGENT_ID,
            timeout=0,
            poll_interval=0,
        )

        with pytest.raises(TimeoutError, match="Timeout waiting for Azure AI Agent run run_123"):
            operator.execute(context={})

        mock_get_run.assert_called_once_with(mock.ANY, thread_id="thread_123", run_id="run_123")
        mock_sleep.assert_not_called()

    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_raises_when_run_is_cancelled(self, mock_run_agent, mock_get_run):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.return_value = create_run(status="cancelled")
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID)

        with pytest.raises(RuntimeError, match="finished with status cancelled"):
            operator.execute(context={})

    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_raises_when_run_requires_action(self, mock_run_agent, mock_get_run):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.return_value = create_run(
            status="requires_action",
            required_action={"type": "submit_tool_outputs"},
        )
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID)

        with pytest.raises(RuntimeError, match="requires tool outputs"):
            operator.execute(context={})

    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_raises_when_run_completes_with_incomplete_details(self, mock_run_agent, mock_get_run):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.return_value = create_run(
            status="completed",
            incomplete_details={"reason": "max_completion_tokens"},
        )
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID)

        with pytest.raises(RuntimeError, match="completed with incomplete output"):
            operator.execute(context={})

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_keeps_polling_when_run_is_cancelling(self, mock_run_agent, mock_get_run, mock_sleep):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.side_effect = [create_run(status="cancelling"), create_run(status="cancelled")]
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID, poll_interval=10)

        with pytest.raises(RuntimeError, match="finished with status cancelled"):
            operator.execute(context={})

        assert mock_get_run.call_count == 2
        mock_sleep.assert_called_once_with(10)

    @mock.patch.object(AzureAIAgentsHook, "get_run", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_raises_when_run_has_unknown_status(self, mock_run_agent, mock_get_run):
        mock_run_agent.return_value = create_run(status="queued")
        mock_get_run.return_value = create_run(status="validating")
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID)

        with pytest.raises(RuntimeError, match="reached unknown status validating"):
            operator.execute(context={})

    @mock.patch.object(AzureAIAgentsHook, "run_agent", autospec=True)
    def test_execute_defers(self, mock_run_agent):
        mock_run_agent.return_value = create_run(status="queued")
        operator = RunAzureAIAgentOperator(
            task_id="run_agent",
            agent_id=AGENT_ID,
            deferrable=True,
            timeout=10,
            poll_interval=2,
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context={})

        trigger = exc.value.trigger
        assert isinstance(trigger, AzureAIAgentRunTrigger)
        assert trigger.azure_ai_agents_conn_id == CONN_ID
        assert trigger.endpoint == ENDPOINT
        assert trigger.thread_id == "thread_123"
        assert trigger.run_id == "run_123"
        assert trigger.timeout == 10
        assert trigger.poll_interval == 2
        assert exc.value.method_name == "execute_complete"

    def test_execute_raises_when_run_response_missing_identifiers(self):
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID)
        hook = mock.Mock(spec_set=["run_agent"])
        hook.run_agent.return_value = SimpleNamespace(status="queued")
        operator.__dict__["hook"] = hook

        with pytest.raises(ValueError, match="must include both id and thread_id"):
            operator.execute(context={})

    def test_execute_complete_success(self):
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID)

        result = operator.execute_complete(
            context={},
            event={
                "status": "success",
                "message": "done",
                "run": {"id": "run_123", "status": "completed"},
            },
        )

        assert result == {"id": "run_123", "status": "completed"}

    def test_execute_complete_raises_when_success_event_omits_run(self):
        operator = RunAzureAIAgentOperator(task_id="run_agent", agent_id=AGENT_ID)

        with pytest.raises(RuntimeError, match="did not include run payload"):
            operator.execute_complete(context={}, event={"status": "success", "message": "done"})


class TestDeleteAzureAIAgentOperator:
    def test_template_fields(self):
        assert DeleteAzureAIAgentOperator.template_fields == (
            "agent_id",
            "azure_ai_agents_conn_id",
            "endpoint",
        )

    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_waits_until_deleted(self, mock_delete_agent, mock_is_agent_deleted):
        mock_is_agent_deleted.return_value = True
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_id=AGENT_ID,
            poll_interval=0,
            azure_ai_agents_conn_id=CONN_ID,
        )

        result = operator.execute(context={})

        assert result is None
        mock_delete_agent.assert_called_once_with(mock.ANY, agent_id=AGENT_ID)
        mock_is_agent_deleted.assert_called_once_with(mock.ANY, agent_id=AGENT_ID)

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_polls_until_deleted_after_delete_returns(
        self, mock_delete_agent, mock_is_agent_deleted, mock_sleep
    ):
        mock_is_agent_deleted.side_effect = [False, True]
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_id=AGENT_ID,
            poll_interval=10,
            azure_ai_agents_conn_id=CONN_ID,
        )

        result = operator.execute(context={})

        assert result is None
        assert mock_is_agent_deleted.call_count == 2
        mock_sleep.assert_called_once_with(10)

    @mock.patch(f"{MODULE}.time.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_raises_when_delete_times_out(
        self, mock_delete_agent, mock_is_agent_deleted, mock_monotonic, mock_sleep
    ):
        mock_is_agent_deleted.return_value = False
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_id=AGENT_ID,
            timeout=0,
            poll_interval=0,
            azure_ai_agents_conn_id=CONN_ID,
        )

        with pytest.raises(TimeoutError, match=f"Timeout waiting for Azure AI Agent {AGENT_ID} deletion"):
            operator.execute(context={})

        mock_delete_agent.assert_called_once_with(mock.ANY, agent_id=AGENT_ID)
        mock_is_agent_deleted.assert_called_once_with(mock.ANY, agent_id=AGENT_ID)
        mock_sleep.assert_not_called()

    @mock.patch.object(AzureAIAgentsHook, "delete_agent", autospec=True)
    def test_execute_defers(self, mock_delete_agent):
        operator = DeleteAzureAIAgentOperator(
            task_id="delete_agent",
            agent_id=AGENT_ID,
            deferrable=True,
            timeout=10,
            poll_interval=2,
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context={})

        mock_delete_agent.assert_called_once_with(mock.ANY, agent_id=AGENT_ID)
        trigger = exc.value.trigger
        assert isinstance(trigger, AzureAIAgentDeleteTrigger)
        assert trigger.azure_ai_agents_conn_id == CONN_ID
        assert trigger.endpoint == ENDPOINT
        assert trigger.agent_id == AGENT_ID
        assert trigger.timeout == 10
        assert trigger.poll_interval == 2
        assert exc.value.method_name == "execute_complete"

    def test_execute_complete_success(self):
        operator = DeleteAzureAIAgentOperator(task_id="delete_agent", agent_id=AGENT_ID)

        assert (
            operator.execute_complete(
                context={},
                event={"status": "success", "message": "deleted", "agent_id": AGENT_ID},
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
