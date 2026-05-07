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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.informatica.hooks.idmc import (
    IDMCRunStatus,
    IDMCTimeoutException,
    InformaticaIDMCError,
)
from airflow.providers.informatica.operators.idmc import (
    InformaticaIDMCRunTaskflowOperator,
    InformaticaIDMCRunTaskOperator,
)
from airflow.providers.informatica.triggers.idmc import (
    InformaticaIDMCTaskflowRunTrigger,
    InformaticaIDMCTaskRunTrigger,
)


def _context() -> dict:
    return {"ti": MagicMock()}


def _make_task_op(**overrides) -> InformaticaIDMCRunTaskOperator:
    """Build an ``InformaticaIDMCRunTaskOperator`` with sane defaults for tests."""
    kwargs = dict(
        task_id="run_idmc_task",  # Airflow DAG-level task id
        idmc_task_id="cdi-task-1",
        task_federated_id=None,
        idmc_task_type="MTT",
        wait_for_completion=False,
        deferrable=False,
        check_interval=0,
    )
    kwargs.update(overrides)
    return InformaticaIDMCRunTaskOperator(**kwargs)


def _make_taskflow_op(**overrides) -> InformaticaIDMCRunTaskflowOperator:
    kwargs = dict(
        task_id="run_idmc_taskflow",
        taskflow_api_name="MyFlow",
        wait_for_completion=False,
        deferrable=False,
        check_interval=0,
    )
    kwargs.update(overrides)
    return InformaticaIDMCRunTaskflowOperator(**kwargs)


def test_run_task_operator_requires_idmc_id_or_federated_id():
    with pytest.raises(ValueError, match="idmc_task_id or task_federated_id"):
        InformaticaIDMCRunTaskOperator(
            task_id="missing_both",
            idmc_task_id=None,
            task_federated_id=None,
            idmc_task_type="MTT",
        )


def test_run_task_operator_no_wait_returns_run_id_immediately():
    op = _make_task_op()
    fake_hook = MagicMock()
    fake_hook.start_task.return_value = {"run_id": "777", "task_type": "MTT", "raw": {}}
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        ctx = _context()
        result = op.execute(ctx)
    assert result == "777"
    fake_hook.start_task.assert_called_once_with(
        task_id="cdi-task-1", task_federated_id=None, task_type="MTT", callback_url=None
    )
    ctx["ti"].xcom_push.assert_called_with(key="idmc_run_id", value="777")


def test_run_task_operator_uses_federated_id_when_only_one_provided():
    op = _make_task_op(idmc_task_id=None, task_federated_id="fed-99")
    fake_hook = MagicMock()
    fake_hook.start_task.return_value = {"run_id": "1"}
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        op.execute(_context())
    fake_hook.start_task.assert_called_once_with(
        task_id=None, task_federated_id="fed-99", task_type="MTT", callback_url=None
    )


def test_run_task_operator_sync_wait_succeeds_then_returns_run_id():
    op = _make_task_op(wait_for_completion=True)
    fake_hook = MagicMock()
    fake_hook.start_task.return_value = {"run_id": "55"}
    fake_hook.get_task_run_status.side_effect = [
        {"status": IDMCRunStatus.RUNNING.value},
        {"status": IDMCRunStatus.SUCCESS.value},
    ]
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        result = op.execute(_context())
    assert result == "55"
    assert fake_hook.get_task_run_status.call_count == 2


def test_run_task_operator_sync_wait_raises_on_failure():
    op = _make_task_op(wait_for_completion=True)
    fake_hook = MagicMock()
    fake_hook.start_task.return_value = {"run_id": "9"}
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.FAILED.value}
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        with pytest.raises(InformaticaIDMCError, match="failed"):
            op.execute(_context())


def test_run_task_operator_sync_wait_raises_on_cancellation():
    op = _make_task_op(wait_for_completion=True)
    fake_hook = MagicMock()
    fake_hook.start_task.return_value = {"run_id": "9"}
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.CANCELLED.value}
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        with pytest.raises(InformaticaIDMCError, match="cancelled"):
            op.execute(_context())


def test_run_task_operator_deferrable_returns_immediately_when_already_terminal():
    op = _make_task_op(wait_for_completion=True, deferrable=True)
    fake_hook = MagicMock()
    fake_hook.start_task.return_value = {"run_id": "11"}
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.SUCCESS.value}
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        result = op.execute(_context())
    assert result == "11"


def test_run_task_operator_deferrable_defers_with_trigger():
    op = _make_task_op(wait_for_completion=True, deferrable=True, check_interval=5)
    fake_hook = MagicMock()
    fake_hook.start_task.return_value = {"run_id": "21"}
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.RUNNING.value}
    with (
        patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook),
        patch.object(InformaticaIDMCRunTaskOperator, "defer") as defer,
    ):
        op.execute(_context())
    defer.assert_called_once()
    trigger = defer.call_args.kwargs["trigger"]
    assert isinstance(trigger, InformaticaIDMCTaskRunTrigger)
    assert trigger.run_id == "21"
    assert defer.call_args.kwargs["method_name"] == "execute_complete"


def test_execute_complete_translates_terminal_event_to_status():
    op = _make_task_op(wait_for_completion=True, deferrable=True)
    op.run_id = "33"
    result = op.execute_complete(_context(), {"status": IDMCRunStatus.SUCCESS.value, "run_id": "33"})
    assert result == IDMCRunStatus.SUCCESS.value


def test_execute_complete_raises_on_timeout_event():
    op = _make_task_op(wait_for_completion=True, deferrable=True)
    op.run_id = "44"
    with pytest.raises(IDMCTimeoutException, match="timed out"):
        op.execute_complete(_context(), {"status": "timeout", "run_id": "44", "message": "timed out"})


def test_execute_complete_raises_on_error_event():
    op = _make_task_op(wait_for_completion=True, deferrable=True)
    op.run_id = "55"
    with pytest.raises(InformaticaIDMCError):
        op.execute_complete(_context(), {"status": "error", "run_id": "55", "message": "boom"})


def test_execute_complete_raises_on_failed_terminal_event():
    op = _make_task_op(wait_for_completion=True, deferrable=True)
    op.run_id = "66"
    with pytest.raises(InformaticaIDMCError, match="failed"):
        op.execute_complete(_context(), {"status": IDMCRunStatus.FAILED.value, "run_id": "66"})


def test_run_taskflow_operator_passes_inputs_through():
    op = _make_taskflow_op(input_parameters={"k": "v"})
    fake_hook = MagicMock()
    fake_hook.start_taskflow.return_value = {"run_id": "tf-1", "raw": {}}
    with patch.object(InformaticaIDMCRunTaskflowOperator, "hook", new=fake_hook):
        result = op.execute(_context())
    assert result == "tf-1"
    fake_hook.start_taskflow.assert_called_once_with("MyFlow", input_parameters={"k": "v"}, callback_url=None)


def test_run_taskflow_operator_uses_taskflow_trigger_when_deferring():
    op = _make_taskflow_op(wait_for_completion=True, deferrable=True)
    fake_hook = MagicMock()
    fake_hook.start_taskflow.return_value = {"run_id": "tf-9"}
    fake_hook.get_taskflow_run_status.return_value = {"status": IDMCRunStatus.RUNNING.value}
    with (
        patch.object(InformaticaIDMCRunTaskflowOperator, "hook", new=fake_hook),
        patch.object(InformaticaIDMCRunTaskflowOperator, "defer") as defer,
    ):
        op.execute(_context())
    trigger = defer.call_args.kwargs["trigger"]
    assert isinstance(trigger, InformaticaIDMCTaskflowRunTrigger)
    assert trigger.run_id == "tf-9"


def test_on_kill_calls_cancel_task_when_run_id_known():
    op = _make_task_op()
    op.run_id = "111"
    fake_hook = MagicMock()
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        op.on_kill()
    fake_hook.cancel_task.assert_called_once_with("111")


def test_on_kill_is_noop_without_run_id():
    op = _make_task_op()
    fake_hook = MagicMock()
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        op.on_kill()
    fake_hook.cancel_task.assert_not_called()


def test_on_kill_swallows_idmc_errors():
    op = _make_task_op()
    op.run_id = "222"
    fake_hook = MagicMock()
    fake_hook.cancel_task.side_effect = InformaticaIDMCError("nope")
    with patch.object(InformaticaIDMCRunTaskOperator, "hook", new=fake_hook):
        op.on_kill()  # must not raise
    fake_hook.cancel_task.assert_called_once_with("222")
