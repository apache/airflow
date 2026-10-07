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

from airflow.providers.common.compat.sdk import TaskDeferred
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
    )
    op.hook = mock.create_autospec(DatabricksAgentHook, instance=True)
    op.hook.invoke_agent.return_value = {"id": INVOCATION_ID, "status_url": "ignored"}
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
        ("app_url", "https://other.databricksapps.com"),
    ],
)
def test_idempotency_identity(operator, field, value):
    operator.invocation_id = None
    ti = mock.Mock(spec=["dag_id", "task_id", "run_id", "map_index", "try_number"])
    ti.dag_id, ti.task_id, ti.run_id, ti.map_index, ti.try_number = "dag", "task", "run", -1, 1
    first = operator._get_invocation_id({"ti": ti})
    assert str(UUID(first)) == first
    ti.try_number = 2
    assert operator._get_invocation_id({"ti": ti}) == first
    if field == "app_url":
        operator.app_url = value
    else:
        setattr(ti, field, value)
    assert operator._get_invocation_id({"ti": ti}) != first


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
    assert operator.execute({}) == operator.hook.invoke_agent.return_value
    operator.hook.invoke_agent.assert_called_once_with(INVOCATION_ID, operator.input, "conversation")
    operator.hook.get_invocation.assert_not_called()


@pytest.mark.parametrize("status", ["completed", "interrupted"])
def test_immediate_result(operator, status):
    result = {"status": status, "output": {"answer": "hello"}}
    operator.hook.invoke_agent.return_value = result
    assert operator.execute({}) == result
    operator.hook.get_invocation.assert_not_called()


@mock.patch("airflow.providers.databricks.operators.agent.time.sleep", autospec=True)
@mock.patch("airflow.providers.databricks.operators.agent.time.monotonic", autospec=True, return_value=0)
def test_waits_for_result(monotonic, sleep, operator):
    result = {"status": "completed", "output": "answer"}
    operator.hook.get_invocation.side_effect = [{"status": "running"}, result]
    assert operator.execute({}) == result
    assert (
        operator.hook.get_invocation.call_args_list
        == [mock.call(INVOCATION_ID, "conversation", timeout_seconds=3600)] * 2
    )
    sleep.assert_called_once_with(10)


@pytest.mark.parametrize("result", [{"status": "failed"}, {}])
def test_polling_failure(operator, result):
    operator.hook.get_invocation.return_value = result
    with pytest.raises(DatabricksAgentInvocationError, match="failed|missing"):
        operator.execute({})


@mock.patch("airflow.providers.databricks.operators.agent.time.monotonic", autospec=True)
@pytest.mark.parametrize("elapsed", [3600, 3601])
def test_timeout_before_poll(monotonic, operator, elapsed):
    monotonic.side_effect = [0, elapsed]
    operator.hook.get_invocation.return_value = {"status": "running"}
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
    operator.hook.get_invocation.return_value = {"status": "running"}
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
    assert exc.value.method_name == "execute_complete"
    assert exc.value.kwargs == {"invocation_id": INVOCATION_ID}
    assert exc.value.timeout.total_seconds() == 3600


@pytest.mark.parametrize("status", ["completed", "interrupted"])
def test_execute_complete(operator, status):
    result = {"status": status, "output": "answer"}
    operator.hook.get_invocation.return_value = result
    assert (
        operator.execute_complete(
            {}, {"status": "success", "invocation_id": INVOCATION_ID}, invocation_id=INVOCATION_ID
        )
        == result
    )


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
    "error_type", ["api_error", "invalid_response", "unexpected_error", None, "private error text"]
)
def test_trigger_polling_error(operator, error_type):
    expected = error_type if error_type in ("api_error", "invalid_response") else "unexpected_error"
    with pytest.raises(DatabricksAgentInvocationError, match=rf"{INVOCATION_ID} failed \({expected}\)"):
        operator.execute_complete(
            {},
            {"status": "error", "invocation_id": INVOCATION_ID, "error_type": error_type},
            invocation_id=INVOCATION_ID,
        )
    operator.hook.get_invocation.assert_not_called()


@pytest.mark.parametrize(
    ("status", "message"), [("running", "terminal"), ("failed", "failed"), (None, "missing")]
)
def test_trigger_terminal_failure(operator, status, message):
    operator.hook.get_invocation.return_value = {"status": status}
    with pytest.raises(DatabricksAgentInvocationError, match=message) as exc:
        operator.execute_complete(
            {}, {"status": "success", "invocation_id": INVOCATION_ID}, invocation_id=INVOCATION_ID
        )
    assert INVOCATION_ID in str(exc.value)


@mock.patch("airflow.providers.databricks.operators.agent.DatabricksAgentHook", autospec=True)
def test_hook_configuration(hook):
    operator = DatabricksAgentInvokeOperator(
        task_id="invoke", app_url=APP_URL, input={}, databricks_conn_id="oauth"
    )
    assert operator.hook is hook.return_value
    hook.assert_called_once_with(APP_URL, "oauth")
