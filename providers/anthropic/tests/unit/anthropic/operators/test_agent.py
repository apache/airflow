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

from airflow.exceptions import TaskDeferred
from airflow.providers.anthropic.exceptions import AnthropicAgentSessionError, AnthropicAgentSessionTimeout
from airflow.providers.anthropic.hooks.anthropic import AnthropicHook
from airflow.providers.anthropic.operators.agent import AnthropicAgentSessionOperator
from airflow.providers.anthropic.triggers.agent import AnthropicAgentSessionTrigger

pytest.importorskip("anthropic")


def _context():
    return {"ti": mock.MagicMock()}


def test_requires_exactly_one_of_message_or_outcome():
    with pytest.raises(ValueError, match="exactly one"):
        AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env")
    with pytest.raises(ValueError, match="exactly one"):
        AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", message="hi", outcome={"description": "x"}
        )


def test_outcome_requires_rubric():
    with pytest.raises(ValueError, match="rubric"):
        AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", outcome={"description": "x"}
        )


class TestExecute:
    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_message_sends_user_message_and_waits(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_session.return_value.id = "sess_1"
        mock_hook_prop.return_value = hook

        op = AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", message="summarize", deferrable=False
        )
        context = _context()
        assert op.execute(context) == "sess_1"
        hook.create_session.assert_called_once_with(agent="ag", environment_id="env")
        hook.send_event.assert_called_once_with(
            "sess_1", {"type": "user.message", "content": [{"type": "text", "text": "summarize"}]}
        )
        hook.wait_for_session.assert_called_once()
        context["ti"].xcom_push.assert_called_once_with(key="session_id", value="sess_1")

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_outcome_sends_define_outcome(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_session.return_value.id = "sess_1"
        mock_hook_prop.return_value = hook

        outcome = {"description": "build a CSV", "rubric": {"type": "text", "content": "has a price column"}}
        op = AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", outcome=outcome, deferrable=False
        )
        op.execute(_context())
        hook.send_event.assert_called_once_with("sess_1", {"type": "user.define_outcome", **outcome})

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_passes_vault_ids_and_resources(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_session.return_value.id = "sess_1"
        mock_hook_prop.return_value = hook

        op = AnthropicAgentSessionOperator(
            task_id="a",
            agent_id="ag",
            environment_id="env",
            message="hi",
            deferrable=False,
            vault_ids=["vlt_1"],
            session_resources=[{"type": "file", "file_id": "f1", "mount_path": "/workspace/f"}],
        )
        op.execute(_context())
        hook.create_session.assert_called_once_with(
            agent="ag",
            environment_id="env",
            vault_ids=["vlt_1"],
            resources=[{"type": "file", "file_id": "f1", "mount_path": "/workspace/f"}],
        )

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_timeout_archives_and_raises(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_session.return_value.id = "sess_1"
        hook.wait_for_session.side_effect = AnthropicAgentSessionTimeout("too slow")
        mock_hook_prop.return_value = hook

        op = AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", message="hi", deferrable=False
        )
        with pytest.raises(AnthropicAgentSessionTimeout, match="too slow"):
            op.execute(_context())
        hook.archive_session.assert_called_once_with("sess_1")

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_deferrable_defers_with_trigger(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_session.return_value.id = "sess_1"
        mock_hook_prop.return_value = hook

        op = AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", message="hi", deferrable=True
        )
        with pytest.raises(TaskDeferred) as exc:
            op.execute(_context())
        assert isinstance(exc.value.trigger, AnthropicAgentSessionTrigger)
        assert exc.value.trigger.session_id == "sess_1"
        assert exc.value.method_name == "execute_complete"
        hook.wait_for_session.assert_not_called()


class TestExecuteComplete:
    def test_success_returns_session_id(self):
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        assert op.execute_complete({}, {"status": "success", "session_id": "sess_1"}) == "sess_1"

    def test_error_raises(self):
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicAgentSessionError, match="boom"):
            op.execute_complete({}, {"status": "error", "session_id": "s", "message": "boom"})

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_timeout_archives_and_raises(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicAgentSessionTimeout):
            op.execute_complete({}, {"status": "timeout", "session_id": "s", "message": "slow"})
        hook.archive_session.assert_called_once_with("s")

    def test_none_event_raises(self):
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicAgentSessionError, match="without an event"):
            op.execute_complete({}, None)


class TestOnKill:
    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_on_kill_archives_session(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        op.session_id = "sess_1"
        op.on_kill()
        hook.archive_session.assert_called_once_with("sess_1")

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_on_kill_noop_without_session(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        op.on_kill()
        hook.archive_session.assert_not_called()
