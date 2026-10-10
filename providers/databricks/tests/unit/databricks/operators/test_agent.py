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
from uuid import UUID

import pytest

from airflow.providers.common.compat.sdk import AirflowFailException, TaskDeferred
from airflow.providers.databricks.exceptions import (
    DatabricksAgentInvocationError,
    DatabricksAgentInvocationTimeout,
)
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook
from airflow.providers.databricks.operators.agent import DatabricksAgentInvokeOperator
from airflow.providers.databricks.triggers.agent import DatabricksAgentInvocationTrigger

APP_URL = "https://agent.databricksapps.com"
INVOCATION_ID = "550e8400-e29b-41d4-a716-446655440000"


@pytest.fixture
def operator():
    op = DatabricksAgentInvokeOperator(
        task_id="invoke",
        app_url=APP_URL,
        input={"messages": []},
        session_id="conversation",
        invocation_id=INVOCATION_ID,
        databricks_conn_id="agent_oauth",
        polling_period_seconds=3,
    )
    op.hook = mock.create_autospec(DatabricksAgentHook, instance=True)
    op.hook.create_invocation.return_value = {"id": INVOCATION_ID, "status_url": "ignored"}
    return op


@pytest.mark.parametrize("field", ["polling_period_seconds", "timeout"])
@pytest.mark.parametrize("value", [0, -1])
def test_invalid_wait_options(field, value):
    with pytest.raises(ValueError, match="must be positive"):
        DatabricksAgentInvokeOperator(task_id="invoke", app_url=APP_URL, input={}, **{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("map_index", 0),
        ("run_id", "next"),
        ("task_id", "other"),
        ("dag_id", "other"),
    ],
)
def test_idempotency_identity(operator, field, value):
    operator.invocation_id = None
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "map_index"])
    ti.dag_id, ti.task_id, ti.run_id, ti.map_index = "dag", "task", "run", -1
    first = operator._get_invocation_id({"ti": ti})
    setattr(ti, field, value)
    assert operator._get_invocation_id({"ti": ti}) != first


def test_identity_is_stable_across_retries(operator):
    operator.invocation_id = None
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "map_index", "try_number"])
    ti.dag_id, ti.task_id, ti.run_id, ti.map_index, ti.try_number = "dag", "task", "run", -1, 1
    first = operator._get_invocation_id({"ti": ti})
    assert str(UUID(first)) == first
    ti.try_number = 2
    assert operator._get_invocation_id({"ti": ti}) == first


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("app_url", "https://other.databricksapps.com"),
        ("input", {"messages": [{"role": "user", "content": "new question"}]}),
        ("session_id", "new-session"),
    ],
)
def test_changed_request_gets_new_identity(operator, field, value):
    operator.invocation_id = None
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "map_index"])
    ti.dag_id, ti.task_id, ti.run_id, ti.map_index = "dag", "task", "run", -1
    original = operator._get_invocation_id({"ti": ti})
    setattr(operator, field, value)
    assert operator._get_invocation_id({"ti": ti}) != original


def test_identity_normalizes_trailing_slash(operator):
    operator.invocation_id = None
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "map_index"])
    ti.dag_id, ti.task_id, ti.run_id, ti.map_index = "dag", "task", "run", -1
    original = operator._get_invocation_id({"ti": ti})
    operator.app_url += "/"
    assert operator._get_invocation_id({"ti": ti}) == original


def test_identity_ignores_mapping_key_order(operator):
    operator.invocation_id = None
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "map_index"])
    ti.dag_id, ti.task_id, ti.run_id, ti.map_index = "dag", "task", "run", -1
    operator.input = {"messages": [], "options": {"a": 1, "b": 2}}
    original = operator._get_invocation_id({"ti": ti})
    operator.input = {"options": {"b": 2, "a": 1}, "messages": []}
    assert operator._get_invocation_id({"ti": ti}) == original


def test_template_fields():
    operator = DatabricksAgentInvokeOperator(
        task_id="invoke",
        app_url="https://{{ app }}.databricksapps.com",
        input={"messages": [{"role": "user", "content": "{{ prompt }}"}]},
        databricks_conn_id="{{ connection }}",
        session_id="{{ run_id }}",
        invocation_id="{{ invocation_id }}",
    )
    operator.render_template_fields(
        {
            "app": "agent",
            "prompt": "hello",
            "connection": "oauth",
            "run_id": "conversation",
            "invocation_id": INVOCATION_ID,
        }
    )
    assert operator.app_url == APP_URL
    assert operator.input == {"messages": [{"role": "user", "content": "hello"}]}
    assert operator.databricks_conn_id == "oauth"
    assert operator.session_id == "conversation"
    assert operator.invocation_id == INVOCATION_ID


def test_submit_without_waiting(operator):
    operator.wait_for_termination = False
    operator.deferrable = True
    assert operator.execute({}) == operator.hook.create_invocation.return_value
    operator.hook.create_invocation.assert_called_once_with(INVOCATION_ID, operator.input, "conversation")
    operator.hook.get_invocation.assert_not_called()


@pytest.mark.parametrize("status", ["completed", "interrupted"])
def test_immediate_result(operator, status):
    result = {"status": "completed", "output": {"status": status, "output": []}}
    operator.hook.create_invocation.return_value = result
    assert operator.execute({}) == result
    operator.hook.get_invocation.assert_not_called()


@mock.patch("airflow.providers.databricks.operators.agent.time.sleep", autospec=True)
@mock.patch("airflow.providers.databricks.operators.agent.time.monotonic", autospec=True, return_value=0)
def test_waits_for_result(monotonic, sleep, operator):
    result = {"status": "completed", "output": "answer"}
    operator.hook.get_invocation.side_effect = [{"status": "active"}, result]
    assert operator.execute({}) == result
    assert (
        operator.hook.get_invocation.call_args_list
        == [mock.call(INVOCATION_ID, "conversation", timeout_seconds=3600)] * 2
    )
    sleep.assert_called_once_with(3)


def test_polling_missing_status(operator):
    operator.hook.get_invocation.return_value = {}
    with pytest.raises(DatabricksAgentInvocationError, match="missing"):
        operator.execute({})


def test_stored_failure_stops_task_retries(operator):
    operator.hook.get_invocation.return_value = {"id": INVOCATION_ID, "status": "failed"}
    with pytest.raises(AirflowFailException, match="new.*invocation_id") as exc:
        operator.execute({})
    assert INVOCATION_ID in str(exc.value)
    operator.hook.create_invocation.assert_called_once_with(INVOCATION_ID, operator.input, "conversation")


def test_resumed_stored_failure_stops_task_retries(operator):
    operator.hook.get_invocation.return_value = {"id": INVOCATION_ID, "status": "failed"}
    with pytest.raises(AirflowFailException, match="new.*invocation_id") as exc:
        operator.execute_complete(
            {}, {"status": "success", "invocation_id": INVOCATION_ID}, invocation_id=INVOCATION_ID
        )
    assert INVOCATION_ID in str(exc.value)
    operator.hook.get_invocation.assert_called_once_with(INVOCATION_ID, "conversation")


@mock.patch("airflow.providers.databricks.operators.agent.time.monotonic", autospec=True)
@pytest.mark.parametrize("elapsed", [3600, 3601])
def test_timeout_before_poll(monotonic, operator, elapsed):
    monotonic.side_effect = [0, elapsed]
    operator.hook.get_invocation.return_value = {"status": "active"}
    with pytest.raises(DatabricksAgentInvocationTimeout, match=INVOCATION_ID):
        operator.execute({})
    operator.hook.get_invocation.assert_not_called()


@mock.patch("airflow.providers.databricks.operators.agent.time.monotonic", autospec=True)
@pytest.mark.parametrize("elapsed", [3600, 3601])
def test_rejects_late_terminal_result(monotonic, operator, elapsed):
    monotonic.side_effect = [0, 0, elapsed]
    operator.hook.get_invocation.return_value = {"status": "completed", "output": "late"}
    with pytest.raises(DatabricksAgentInvocationTimeout, match=INVOCATION_ID):
        operator.execute({})
    operator.hook.get_invocation.assert_called_once_with(INVOCATION_ID, "conversation", timeout_seconds=3600)


@mock.patch("airflow.providers.databricks.operators.agent.time.sleep", autospec=True)
@mock.patch(
    "airflow.providers.databricks.operators.agent.time.monotonic",
    autospec=True,
    side_effect=[0, 3599, 3599, 3600],
)
def test_timeout_after_sleep(monotonic, sleep, operator):
    operator.hook.get_invocation.return_value = {"status": "active"}
    with pytest.raises(DatabricksAgentInvocationTimeout, match=INVOCATION_ID):
        operator.execute({})
    operator.hook.get_invocation.assert_called_once_with(INVOCATION_ID, "conversation", timeout_seconds=1)
    sleep.assert_called_once_with(1)


def test_defers(operator):
    operator.deferrable = True
    with pytest.raises(TaskDeferred) as exc:
        operator.execute({})
    trigger = exc.value.trigger
    assert isinstance(trigger, DatabricksAgentInvocationTrigger)
    assert trigger.app_url == APP_URL
    assert trigger.invocation_id == INVOCATION_ID
    assert trigger.session_id == "conversation"
    assert trigger.databricks_conn_id == "agent_oauth"
    assert trigger.polling_period_seconds == 3
    assert exc.value.method_name == "execute_complete"
    assert exc.value.kwargs == {"invocation_id": INVOCATION_ID}
    assert exc.value.timeout.total_seconds() == 3600


@pytest.mark.parametrize("status", ["completed", "interrupted"])
def test_execute_complete(operator, status):
    result = {"status": "completed", "output": {"status": status, "output": []}}
    operator.hook.get_invocation.return_value = result
    assert (
        operator.execute_complete(
            {}, {"status": "success", "invocation_id": INVOCATION_ID}, invocation_id=INVOCATION_ID
        )
        == result
    )
    operator.hook.get_invocation.assert_called_once_with(INVOCATION_ID, "conversation")


@pytest.mark.parametrize(
    "event",
    [
        None,
        {},
        {"status": "error"},
        {"status": "success", "invocation_id": "other"},
        {"status": "unknown", "invocation_id": INVOCATION_ID},
    ],
)
def test_invalid_trigger_event(operator, event):
    with pytest.raises(
        DatabricksAgentInvocationError, match=f"{INVOCATION_ID} trigger returned an invalid event"
    ):
        operator.execute_complete({}, event, invocation_id=INVOCATION_ID)
    operator.hook.get_invocation.assert_not_called()


@pytest.mark.parametrize(
    ("error_type", "expected"),
    [
        ("api_error", "api_error"),
        ("invalid_response", "invalid_response"),
        ("unexpected_error", "unexpected_error"),
        (None, "unexpected_error"),
        ("private error text", "unexpected_error"),
    ],
)
def test_trigger_polling_error(operator, error_type, expected):
    with pytest.raises(DatabricksAgentInvocationError, match=rf"{INVOCATION_ID} failed \({expected}\)"):
        operator.execute_complete(
            {},
            {"status": "error", "invocation_id": INVOCATION_ID, "error_type": error_type},
            invocation_id=INVOCATION_ID,
        )
    operator.hook.get_invocation.assert_not_called()


@pytest.mark.parametrize(("status", "message"), [("active", "terminal"), (None, "missing")])
def test_trigger_terminal_failure(operator, status, message):
    operator.hook.get_invocation.return_value = {"status": status}
    with pytest.raises(DatabricksAgentInvocationError, match=message) as exc:
        operator.execute_complete(
            {}, {"status": "success", "invocation_id": INVOCATION_ID}, invocation_id=INVOCATION_ID
        )
    assert INVOCATION_ID in str(exc.value)
    operator.hook.get_invocation.assert_called_once_with(INVOCATION_ID, "conversation")


@mock.patch("airflow.providers.databricks.operators.agent.DatabricksAgentHook", autospec=True)
def test_hook_configuration(hook):
    operator = DatabricksAgentInvokeOperator(
        task_id="invoke", app_url=APP_URL, input={}, databricks_conn_id="oauth"
    )
    assert operator.hook is hook.return_value
    hook.assert_called_once_with(APP_URL, "oauth")
