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
from airflow.providers.anthropic.exceptions import (
    AnthropicAgentSessionError,
    AnthropicAgentSessionTimeout,
    AnthropicSessionBudgetExceeded,
    AnthropicTriggerEventError,
)
from airflow.providers.anthropic.hooks.anthropic import AnthropicHook
from airflow.providers.anthropic.operators.agent import AnthropicAgentSessionOperator
from airflow.providers.anthropic.triggers.agent import AnthropicAgentSessionTrigger

pytest.importorskip("anthropic")


def _context():
    return {"ti": mock.MagicMock()}


def _create_op(**kwargs) -> AnthropicAgentSessionOperator:
    # deferrable=False explicitly: the default reads operators.default_deferrable, which
    # would send these through defer() instead of the synchronous path.
    return AnthropicAgentSessionOperator(
        task_id="a",
        agent_id="ag",
        environment_id="env",
        message="hi",
        deferrable=False,
        **kwargs,
    )


def test_requires_exactly_one_of_message_or_outcome():
    op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env")
    with pytest.raises(ValueError, match="exactly one"):
        op.execute(_context())

    op = AnthropicAgentSessionOperator(
        task_id="a", agent_id="ag", environment_id="env", message="hi", outcome={"description": "x"}
    )
    with pytest.raises(ValueError, match="exactly one"):
        op.execute(_context())


def test_outcome_requires_description_and_rubric():
    # missing rubric
    op = AnthropicAgentSessionOperator(
        task_id="a", agent_id="ag", environment_id="env", outcome={"description": "x"}
    )
    with pytest.raises(ValueError, match="description.*rubric"):
        op.execute(_context())

    # missing description
    op = AnthropicAgentSessionOperator(
        task_id="a",
        agent_id="ag",
        environment_id="env",
        outcome={"rubric": {"type": "text", "content": "c"}},
    )
    with pytest.raises(ValueError, match="description.*rubric"):
        op.execute(_context())


def test_init_does_not_validate_message_or_outcome():
    """Regression test: __init__ must not read template-field values (see #70296)."""
    op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env")
    assert op.message is None
    assert op.outcome is None

    op = AnthropicAgentSessionOperator(
        task_id="a", agent_id="ag", environment_id="env", outcome={"description": "x"}
    )
    assert op.outcome == {"description": "x"}


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
    def test_sync_non_timeout_error_archives_and_raises(self, mock_hook_prop):
        # A non-timeout failure while waiting (SDK 5xx, auth expiry) also leaves the session
        # container running, so the broadened except archives it best-effort before re-raising.
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_session.return_value.id = "sess_1"
        hook.wait_for_session.side_effect = RuntimeError("api 5xx")
        mock_hook_prop.return_value = hook

        op = AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", message="hi", deferrable=False
        )
        with pytest.raises(RuntimeError, match="api 5xx"):
            op.execute(_context())
        hook.archive_session.assert_called_once_with("sess_1")

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_send_event_failure_archives_session(self, mock_hook_prop):
        # send_event fails after create_session allocated the container; it must be archived.
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_session.return_value.id = "sess_1"
        hook.send_event.side_effect = RuntimeError("send boom")
        mock_hook_prop.return_value = hook

        op = AnthropicAgentSessionOperator(
            task_id="a", agent_id="ag", environment_id="env", message="hi", deferrable=False
        )
        with pytest.raises(RuntimeError, match="send boom"):
            op.execute(_context())
        hook.archive_session.assert_called_once_with("sess_1")
        hook.wait_for_session.assert_not_called()

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


class TestBudgetParam:
    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_dollar_amount_is_converted_to_minor_units(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        _create_op(budget=25).execute(_context())
        assert hook.create_session.call_args.kwargs["budget"] == {
            "type": "limit",
            "max_list_cost": {"amount": "2500", "currency": "USD"},
        }

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_mapping_passes_through(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        raw = {"type": "limit", "max_list_cost": {"amount": "750", "currency": "USD"}}
        _create_op(budget=raw).execute(_context())
        assert hook.create_session.call_args.kwargs["budget"] == raw

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_no_budget_key_when_unset(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        _create_op().execute(_context())
        assert "budget" not in hook.create_session.call_args.kwargs

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_conflicting_budget_sources_rejected(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = _create_op(budget=25, session_kwargs={"budget": {"type": "limit"}})
        with pytest.raises(ValueError, match="not both"):
            op.execute(_context())
        hook.create_session.assert_not_called()

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_invalid_amount_fails_before_allocating_a_session(self, mock_hook_prop):
        # A bad amount must not leave a server-side container running.
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        with pytest.raises(ValueError, match="positive"):
            _create_op(budget=-1).execute(_context())
        hook.create_session.assert_not_called()

    def test_budget_is_templated(self):
        assert "budget" in AnthropicAgentSessionOperator.template_fields


class TestExecuteComplete:
    def test_success_returns_session_id(self):
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        assert op.execute_complete({}, {"status": "success", "session_id": "sess_1"}) == "sess_1"

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_error_archives_and_raises(self, mock_hook_prop):
        # The trigger's "error" event means polling gave up while the session may still be
        # running, so the operator archives it best-effort before failing.
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicAgentSessionError, match="boom"):
            op.execute_complete({}, {"status": "error", "session_id": "s", "message": "boom"})
        hook.archive_session.assert_called_once_with("s")

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_budget_stop_raises_budget_exception(self, mock_hook_prop):
        # The deferrable path must raise the same class the synchronous path does; the
        # trigger event's stop_reason is the only classification channel across the boundary.
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicSessionBudgetExceeded, match="over budget"):
            op.execute_complete(
                {},
                {
                    "status": "error",
                    "session_id": "s",
                    "message": "over budget",
                    "stop_reason": "budget_reached",
                },
            )
        hook.archive_session.assert_called_once_with("s")

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_error_event_without_stop_reason_raises_generic(self, mock_hook_prop):
        # Version skew: a trigger serialized before stop_reason existed omits the key.
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicAgentSessionError) as exc:
            op.execute_complete({}, {"status": "error", "session_id": "s", "message": "boom"})
        assert not isinstance(exc.value, AnthropicSessionBudgetExceeded)

    @mock.patch.object(AnthropicAgentSessionOperator, "hook", new_callable=mock.PropertyMock)
    def test_timeout_archives_and_raises(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicAgentSessionTimeout):
            op.execute_complete({}, {"status": "timeout", "session_id": "s", "message": "slow"})
        hook.archive_session.assert_called_once_with("s")

    @pytest.mark.parametrize(
        ("event", "match"),
        [
            pytest.param(None, "event is None", id="none"),
            pytest.param(
                {"status": "rescheduling", "session_id": "s"},
                "Unexpected trigger event status",
                id="unknown-status",
            ),
        ],
    )
    def test_invalid_event_raises(self, event, match):
        op = AnthropicAgentSessionOperator(task_id="a", agent_id="ag", environment_id="env", message="hi")
        with pytest.raises(AnthropicTriggerEventError, match=match):
            op.execute_complete({}, event)


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
