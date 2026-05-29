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

from unittest.mock import MagicMock, create_autospec, patch

import pytest

from airflow.providers.informatica.hooks.idmc import (
    IDMCRunStatus,
    IDMCTimeoutException,
    InformaticaIDMCError,
    InformaticaIDMCHook,
)
from airflow.providers.informatica.sensors.idmc import (
    InformaticaIDMCTaskflowRunSensor,
    InformaticaIDMCTaskRunSensor,
)
from airflow.providers.informatica.triggers.idmc import (
    InformaticaIDMCTaskflowRunTrigger,
    InformaticaIDMCTaskRunTrigger,
)


def _make_task_sensor(**overrides) -> InformaticaIDMCTaskRunSensor:
    kwargs = dict(task_id="wait_for_idmc_task", run_id="42", idmc_task_id="cdi-task-1", deferrable=False)
    kwargs.update(overrides)
    return InformaticaIDMCTaskRunSensor(**kwargs)


def _make_taskflow_sensor(**overrides) -> InformaticaIDMCTaskflowRunSensor:
    kwargs = dict(task_id="wait_for_idmc_taskflow", run_id="tf-1", deferrable=False)
    kwargs.update(overrides)
    return InformaticaIDMCTaskflowRunSensor(**kwargs)


def _hook_mock() -> MagicMock:
    return create_autospec(InformaticaIDMCHook, instance=True)


def test_poke_returns_true_on_success():
    sensor = _make_task_sensor()
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.SUCCESS.value}
    with patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook):
        assert sensor.poke({}) is True
    fake_hook.get_task_run_status.assert_called_once_with("42", task_id="cdi-task-1")


def test_poke_returns_true_on_warning():
    sensor = _make_task_sensor()
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.WARNING.value}
    with patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook):
        assert sensor.poke({}) is True


def test_poke_returns_false_when_running():
    sensor = _make_task_sensor()
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.RUNNING.value}
    with patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook):
        assert sensor.poke({}) is False


def test_poke_raises_on_failure():
    sensor = _make_task_sensor()
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.FAILED.value}
    with patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook):
        with pytest.raises(InformaticaIDMCError, match="failed"):
            sensor.poke({})


def test_poke_raises_on_cancellation():
    sensor = _make_task_sensor()
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.CANCELLED.value}
    with patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook):
        with pytest.raises(InformaticaIDMCError, match="cancelled"):
            sensor.poke({})


def test_taskflow_sensor_uses_taskflow_status_method():
    sensor = _make_taskflow_sensor()
    fake_hook = _hook_mock()
    fake_hook.get_taskflow_run_status.return_value = {"status": IDMCRunStatus.SUCCESS.value}
    with patch.object(InformaticaIDMCTaskflowRunSensor, "hook", new=fake_hook):
        assert sensor.poke({}) is True
    fake_hook.get_taskflow_run_status.assert_called_once_with("tf-1")


def test_non_deferrable_sensor_returns_run_id_when_successful():
    sensor = _make_task_sensor()
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.SUCCESS.value}
    with patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook):
        assert sensor.execute({}) == "42"


def test_deferrable_sensor_returns_when_already_terminal():
    sensor = _make_task_sensor(deferrable=True)
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.SUCCESS.value}
    with patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook):
        # If deferrable is True and poke returns True, ``execute`` returns
        # without deferring.
        assert sensor.execute({}) == "42"


def test_deferrable_sensor_defers_with_task_trigger():
    sensor = _make_task_sensor(deferrable=True)
    fake_hook = _hook_mock()
    fake_hook.get_task_run_status.return_value = {"status": IDMCRunStatus.RUNNING.value}
    with (
        patch.object(InformaticaIDMCTaskRunSensor, "hook", new=fake_hook),
        patch.object(InformaticaIDMCTaskRunSensor, "defer", autospec=True) as defer,
    ):
        sensor.execute({})
    trigger = defer.call_args.kwargs["trigger"]
    assert isinstance(trigger, InformaticaIDMCTaskRunTrigger)
    assert trigger.task_id == "cdi-task-1"


def test_deferrable_taskflow_sensor_defers_with_taskflow_trigger():
    sensor = _make_taskflow_sensor(deferrable=True)
    fake_hook = _hook_mock()
    fake_hook.get_taskflow_run_status.return_value = {"status": IDMCRunStatus.RUNNING.value}
    with (
        patch.object(InformaticaIDMCTaskflowRunSensor, "hook", new=fake_hook),
        patch.object(InformaticaIDMCTaskflowRunSensor, "defer", autospec=True) as defer,
    ):
        sensor.execute({})
    trigger = defer.call_args.kwargs["trigger"]
    assert isinstance(trigger, InformaticaIDMCTaskflowRunTrigger)


def test_execute_complete_returns_run_id_on_success():
    sensor = _make_task_sensor(deferrable=True)
    result = sensor.execute_complete({}, {"status": IDMCRunStatus.SUCCESS.value, "run_id": "42"})
    assert result == "42"


def test_execute_complete_raises_on_failure_event():
    sensor = _make_task_sensor(deferrable=True)
    with pytest.raises(InformaticaIDMCError):
        sensor.execute_complete({}, {"status": IDMCRunStatus.FAILED.value, "run_id": "42"})


def test_execute_complete_raises_on_timeout_event():
    sensor = _make_task_sensor(deferrable=True)
    with pytest.raises(IDMCTimeoutException, match="timed out"):
        sensor.execute_complete({}, {"status": "timeout", "run_id": "42", "message": "timed out"})


def test_execute_complete_raises_on_error_event():
    sensor = _make_task_sensor(deferrable=True)
    with pytest.raises(InformaticaIDMCError):
        sensor.execute_complete({}, {"status": "error", "run_id": "42"})
