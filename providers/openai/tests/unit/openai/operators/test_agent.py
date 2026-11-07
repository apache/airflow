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

import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from openai import OpenAI

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.providers.openai.operators.agent import OpenAIAgentSessionOperator
from airflow.providers.openai.triggers.agent import OpenAIAgentSessionTrigger

httpx2 = pytest.importorskip("httpx2")
AgentSession = pytest.importorskip("openai.types.beta.agent_session").AgentSession
Turn = pytest.importorskip("openai.types.beta.agents.sessions.turn").Turn
SessionTurnError = pytest.importorskip("openai.types.beta.session_turn_error").SessionTurnError


@pytest.fixture
def hook():
    hook = OpenAIHook()
    with OpenAI(api_key="test") as client:
        hook.conn = MagicMock(spec=client)
        yield hook


@pytest.mark.parametrize("status", ["queued", "in_progress", "waiting", "completed", "failed", "cancelled"])
def test_poll_turn_status(hook, status):
    hook.conn.beta.agents.sessions.retrieve.return_value = AgentSession.model_construct(
        status="idle", required_actions=[], error=None
    )
    hook.conn.beta.agents.sessions.turns.list.return_value.data = [
        Turn.model_construct(id="turn", status=status, error=None, usage=None)
    ]
    result = hook.poll_agent_session("session")
    if status in {"queued", "in_progress", "waiting"}:
        assert result is None
    else:
        assert result["status"] == ("success" if status == "completed" else "error")
        assert result["turn_id"] == "turn"
    hook.conn.beta.agents.sessions.turns.list.assert_called_once_with("session", order="asc", limit=1)


def test_idle_without_visible_turn_keeps_waiting(hook):
    hook.conn.beta.agents.sessions.retrieve.return_value = AgentSession.model_construct(
        status="idle", required_actions=[], error=None
    )
    hook.conn.beta.agents.sessions.turns.list.return_value.data = []
    assert hook.poll_agent_session("session") is None


@pytest.mark.parametrize(
    ("status", "actions", "expected"),
    [
        ("failed", [], "error"),
        ("requires_action", [SimpleNamespace(type="function_call")], "error"),
        ("requires_action", [SimpleNamespace(type="environment_connection")], None),
    ],
)
def test_poll_session_failure_and_required_actions(hook, status, actions, expected):
    hook.conn.beta.agents.sessions.retrieve.return_value = AgentSession.model_construct(
        status=status, required_actions=actions, error="Environment failed"
    )
    hook.conn.beta.agents.sessions.turns.list.return_value.data = []
    result = hook.poll_agent_session("session")
    assert (result["status"] if result else None) == expected


def test_create_and_cancel_sdk_payload(hook):
    hook.create_agent_session(input="Hello", environment={"type": "none"}, agent_id="agent")
    hook.conn.beta.agents.sessions.create.assert_called_once_with(
        input="Hello", environment={"type": "none"}, agent_id="agent", stream=False
    )
    hook.cancel_agent_session("session")
    hook.conn.beta.agents.sessions.events.create.assert_called_once_with(
        "session", events=[{"type": "agent.session.input.cancel"}]
    )


@pytest.fixture
def operator():
    operator = OpenAIAgentSessionOperator(
        task_id="agent",
        input="Hello",
        environment={"type": "none"},
        agent_id="agent",
        do_xcom_push=False,
    )
    operator.hook = MagicMock(spec=OpenAIHook)
    operator.hook.create_agent_session.return_value = AgentSession.model_construct(id="session")
    return operator


def test_sync_completion(operator):
    operator.hook.poll_agent_session.return_value = {"status": "success", "session_id": "session"}
    assert operator.execute({}) == "session"
    operator.hook.cancel_agent_session.assert_not_called()


def test_deferral_roundtrip(operator):
    operator.deferrable = True
    with pytest.raises(TaskDeferred) as exc:
        operator.execute({})
    assert exc.value.kwargs == {"session_id": "session"}
    path, kwargs = exc.value.trigger.serialize()
    assert path.endswith(".OpenAIAgentSessionTrigger")
    assert OpenAIAgentSessionTrigger(**kwargs).serialize() == (path, kwargs)
    operator.hook.cancel_agent_session.assert_not_called()
    assert (
        operator.execute_complete({}, {"status": "success", "session_id": "session"}, session_id="session")
        == "session"
    )


@pytest.mark.parametrize("event", [None, {}, {"status": "success", "session_id": "other"}])
def test_invalid_event_cancels_owned_session(operator, event):
    with pytest.raises(AirflowException, match="Invalid"):
        operator.execute_complete({}, event, session_id="session")
    operator.hook.cancel_agent_session.assert_called_once_with("session")


@pytest.mark.parametrize("status", ["error", "timeout"])
def test_deferred_failure_cancels(operator, status):
    with pytest.raises(AirflowException, match="Failed"):
        operator.execute_complete(
            {}, {"status": status, "session_id": "session", "message": "Failed"}, session_id="session"
        )
    operator.hook.cancel_agent_session.assert_called_once_with("session")


def test_sync_poll_exception_cancels_and_preserves_error(operator):
    operator.hook.poll_agent_session.side_effect = RuntimeError("Read failed")
    operator.hook.cancel_agent_session.side_effect = RuntimeError("Cancel failed")
    with patch("airflow.providers.openai.operators.agent.time.sleep", autospec=True) as sleep:
        with pytest.raises(RuntimeError, match="Read failed"):
            operator.execute({})
    assert operator.hook.poll_agent_session.call_count == OpenAIHook.MAX_CONSECUTIVE_POLL_FAILURES
    assert sleep.call_count == OpenAIHook.MAX_CONSECUTIVE_POLL_FAILURES - 1


def test_sync_transient_poll_failure_recovers_without_cancelling(operator):
    operator.hook.poll_agent_session.side_effect = [
        RuntimeError("Read failed"),
        None,
        RuntimeError("Read failed again"),
        {"status": "success", "session_id": "session"},
    ]
    with patch("airflow.providers.openai.operators.agent.time.sleep", autospec=True):
        assert operator.execute({}) == "session"
    operator.hook.cancel_agent_session.assert_not_called()


def test_deferral_timeout_is_capped_by_execution_timeout(operator):
    operator.deferrable = True
    operator.execution_timeout = timedelta(seconds=60)
    with pytest.raises(TaskDeferred) as exc:
        operator.execute({})
    assert exc.value.timeout == timedelta(seconds=60)


def test_deferral_timeout_defaults_to_operator_timeout(operator):
    operator.deferrable = True
    with pytest.raises(TaskDeferred) as exc:
        operator.execute({})
    assert exc.value.timeout == timedelta(seconds=operator.timeout + operator.poll_interval + 60)


def test_sync_timeout_cancels(operator):
    with patch("airflow.providers.openai.operators.agent.time.monotonic", side_effect=[0, 3601]):
        with pytest.raises(AirflowException, match="timed out"):
            operator.execute({})
    operator.hook.cancel_agent_session.assert_called_once_with("session")


@pytest.mark.parametrize(
    ("name", "value"), [("timeout", 0), ("poll_interval", -1), ("timeout", float("inf"))]
)
def test_invalid_wait_configuration(name, value):
    with pytest.raises(ValueError, match=name):
        OpenAIAgentSessionOperator(task_id="agent", input="Hi", environment={}, **{name: value})


@pytest.mark.parametrize("reserved", ["input", "environment", "agent_id", "stream"])
def test_reserved_kwargs_rejected_before_creation(operator, reserved):
    operator.session_kwargs = {reserved: "bad"}
    with pytest.raises(ValueError, match="Reserved"):
        operator.execute({})
    operator.hook.create_agent_session.assert_not_called()


@pytest.mark.parametrize("agent", [None, "gpt-6-astra", ["gpt-6-astra"]])
def test_non_dict_agent_rejected_with_value_error(operator, agent):
    operator.agent_id = None
    operator.session_kwargs = {"agent": agent}
    with pytest.raises(ValueError, match="agent"):
        operator.execute({})
    operator.hook.create_agent_session.assert_not_called()


@pytest.mark.parametrize(
    ("status", "error", "expected"),
    [
        ("cancelled", None, "Agent turn turn cancelled"),
        (
            "failed",
            SessionTurnError.model_construct(code="usage_limit_exceeded", message="Add credits."),
            "Agent turn turn failed (usage_limit_exceeded): Add credits.",
        ),
    ],
)
def test_failed_turn_message_formats_error(hook, status, error, expected):
    hook.conn.beta.agents.sessions.retrieve.return_value = AgentSession.model_construct(
        status="idle", required_actions=[], error=None
    )
    hook.conn.beta.agents.sessions.turns.list.return_value.data = [
        Turn.model_construct(id="turn", status=status, error=error, usage=None)
    ]
    assert hook.poll_agent_session("session")["message"] == expected


def test_real_sdk_request_and_response_contract():
    requests = []
    turn_reads = 0

    def respond(request):
        nonlocal turn_reads
        requests.append(request)
        assert request.headers["OpenAI-Beta"] == "agents=v1"
        if request.url.path.endswith("/events"):
            return httpx2.Response(204)
        if request.url.path.endswith("/turns"):
            turn_reads += 1
            return httpx2.Response(
                200,
                json={
                    "object": "list",
                    "has_more": False,
                    "data": []
                    if turn_reads == 1
                    else [
                        {
                            "id": "turn",
                            "session_id": "session",
                            "agent_id": "agent",
                            "object": "agent.session.turn",
                            "created_at": 0,
                            "status": "completed",
                            "usage": {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
                        }
                    ],
                },
            )
        return httpx2.Response(
            200,
            json={"id": "session", "status": "idle", "required_actions": [], "error": None},
        )

    with OpenAI(api_key="test", http_client=httpx2.Client(transport=httpx2.MockTransport(respond))) as client:
        hook = OpenAIHook()
        hook.conn = client
        session = hook.create_agent_session(input="Hello", environment={"type": "none"}, agent_id="agent")
        assert session.id == "session"
        assert hook.poll_agent_session(session.id) is None
        result = hook.poll_agent_session(session.id)
        assert result["status"] == "success"
        assert result["usage"]["total_tokens"] == 5
        hook.cancel_agent_session(session.id)
    assert json.loads(requests[0].content) == {
        "input": "Hello",
        "environment": {"type": "none"},
        "agent_id": "agent",
        "stream": False,
    }
    assert json.loads(requests[-1].content) == {"events": [{"type": "agent.session.input.cancel"}]}
    assert dict(requests[2].url.params) == {"order": "asc", "limit": "1"}


def test_usage_is_recorded_with_attempt(operator):
    operator.do_xcom_push = True
    push = MagicMock(spec=lambda **kwargs: None)
    context = {"ti": SimpleNamespace(xcom_push=push, try_number=2)}
    event = {"status": "success", "session_id": "session", "turn_id": "turn", "usage": {"total_tokens": 5}}
    assert operator.execute_complete(context, event, session_id="session") == "session"
    push.assert_any_call(key="turn_id", value="turn")
    push.assert_any_call(key="usage", value={"total_tokens": 5, "try_number": 2})
    assert event["usage"] == {"total_tokens": 5}


def test_usage_failure_does_not_mask_agent_failure(operator):
    operator.do_xcom_push = True
    push = MagicMock(spec=lambda **kwargs: None, side_effect=RuntimeError("XCom unavailable"))
    context = {"ti": SimpleNamespace(xcom_push=push, try_number=1)}
    with pytest.raises(AirflowException, match="Agent failed"):
        operator.execute_complete(
            context,
            {
                "status": "error",
                "session_id": "session",
                "turn_id": "turn",
                "message": "Agent failed",
            },
            session_id="session",
        )
    operator.hook.cancel_agent_session.assert_called_once_with("session")


def test_sync_failed_turn_cancels_once(operator):
    operator.hook.poll_agent_session.return_value = {
        "status": "error",
        "session_id": "session",
        "message": "Agent failed",
    }
    with pytest.raises(AirflowException, match="Agent failed"):
        operator.execute({})
    operator.hook.cancel_agent_session.assert_called_once_with("session")
