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

from datetime import UTC, datetime
from unittest.mock import create_autospec, patch

import pytest
import time_machine
from azure.core.exceptions import AzureError, ServiceRequestTimeoutError, ServiceResponseTimeoutError
from azure.mgmt.appcontainers.models import JobExecution

from airflow.exceptions import TaskDeferred
from airflow.providers.microsoft.azure.exceptions import AzureContainerAppsJobExecutionError
from airflow.providers.microsoft.azure.hooks.container_apps import AzureContainerAppsJobHook
from airflow.providers.microsoft.azure.operators.container_apps import AzureContainerAppsJobOperator
from airflow.providers.microsoft.azure.triggers.container_apps import AzureContainerAppsJobTrigger

MODULE = "airflow.providers.microsoft.azure.operators.container_apps"


@pytest.fixture
def operator():
    operator = AzureContainerAppsJobOperator(task_id="job", resource_group="group", job_name="job", retries=0)
    operator.hook = create_autospec(AzureContainerAppsJobHook, instance=True)
    operator.hook.start_job.return_value = "execution"
    return operator


@patch(f"{MODULE}.time.sleep", autospec=True)
def test_worker_polls_once_per_interval_and_returns_execution(sleep, operator):
    operator.hook.get_execution.side_effect = [
        JobExecution(status="Processing"),
        JobExecution(status="Succeeded"),
    ]
    assert operator.execute({}) == "execution"
    operator.hook.start_job.assert_called_once()
    operator.hook.get_execution.assert_called_with("group", "job", "execution", timeout_seconds=None)
    sleep.assert_called_once_with(60)
    operator.on_kill()
    operator.hook.stop_execution.assert_not_called()


@pytest.mark.parametrize("execution", [None, JobExecution(status="Failed", message="Azure diagnostic")])
def test_worker_failure(operator, execution):
    operator.hook.get_execution.return_value = execution
    with pytest.raises(AzureContainerAppsJobExecutionError, match="history|Azure diagnostic"):
        operator.execute({})


@time_machine.travel(datetime(2026, 1, 1, tzinfo=UTC), tick=False)
@pytest.mark.parametrize("timeout", [None, 90])
def test_deferral_submits_once_and_preserves_identity(operator, timeout):
    operator.deferrable = True
    operator.timeout_seconds = timeout
    with pytest.raises(TaskDeferred) as caught:
        operator.execute({})
    trigger = caught.value.trigger
    assert isinstance(trigger, AzureContainerAppsJobTrigger)
    assert trigger.execution_name == "execution"
    assert trigger.deadline == ("2026-01-01T00:01:30+00:00" if timeout else None)
    assert caught.value.method_name == "execute_complete"
    operator.hook.start_job.assert_called_once()
    operator.hook.get_execution.assert_not_called()
    assert operator.execute_complete({}, {"status": "success", "execution_name": "execution"}) == "execution"
    operator.execute_complete({}, {"status": "success", "execution_name": "execution"})
    operator.hook.start_job.assert_called_once()


@pytest.mark.parametrize(
    "event", [None, {}, {"status": "success"}, {"status": "error", "message": "timeout"}]
)
def test_invalid_completion_event(operator, event):
    with pytest.raises(AzureContainerAppsJobExecutionError, match="execution|timeout"):
        operator.execute_complete({}, event)


@pytest.mark.parametrize("stop_error", [None, AzureError("stop failed"), TimeoutError("stop timed out")])
@patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 1, 3, 4])
@patch(f"{MODULE}.time.sleep", autospec=True)
def test_worker_timeout_cancels_even_when_termination_cancellation_disabled(
    sleep, monotonic, operator, stop_error
):
    operator.timeout_seconds = 4
    operator.cancel_on_termination = False
    operator.hook.get_execution.return_value = JobExecution(status="Running")
    operator.hook.stop_execution.side_effect = stop_error
    with pytest.raises(AzureContainerAppsJobExecutionError, match="timed out"):
        operator.execute({})
    sleep.assert_called_once_with(1)
    operator.hook.stop_execution.assert_called_once_with("group", "job", "execution")


@pytest.mark.parametrize("finished_at", [3, 4, 5])
@patch(f"{MODULE}.time", autospec=True)
def test_worker_checks_deadline_after_terminal_lookup(clock, operator, finished_at):
    clock.monotonic.side_effect = [0, 1, finished_at]
    operator.timeout_seconds = 4
    operator.cancel_on_termination = False
    operator.hook.get_execution.return_value = JobExecution(status="Succeeded")
    if finished_at < 4:
        assert operator.execute({}) == "execution"
        operator.hook.stop_execution.assert_not_called()
    else:
        with pytest.raises(AzureContainerAppsJobExecutionError, match="timed out"):
            operator.execute({})
        operator.hook.stop_execution.assert_called_once_with("group", "job", "execution")
    operator.hook.get_execution.assert_called_once_with("group", "job", "execution", timeout_seconds=3)
    clock.sleep.assert_not_called()


@pytest.mark.parametrize("finished_at", [2, 4])
@pytest.mark.parametrize(
    "error_type", [TimeoutError, ServiceRequestTimeoutError, ServiceResponseTimeoutError]
)
@patch(f"{MODULE}.time", autospec=True)
def test_worker_lookup_timeout_only_translated_after_deadline(clock, operator, finished_at, error_type):
    clock.monotonic.side_effect = [0, 1, finished_at]
    operator.timeout_seconds = 4
    operator.cancel_on_termination = False
    operator.hook.get_execution.side_effect = error_type("lookup timeout")
    expected = error_type if finished_at < 4 else AzureContainerAppsJobExecutionError
    with pytest.raises(expected, match="timeout|timed out"):
        operator.execute({})
    assert operator.hook.stop_execution.call_count == (finished_at >= 4)


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("stop_error", [None, AzureError("stop failed"), TimeoutError("stop timed out")])
def test_worker_termination(operator, enabled, stop_error):
    operator.cancel_on_termination = enabled
    operator.on_kill()
    operator.hook.stop_execution.assert_not_called()
    operator._execution_name = "execution"
    operator.hook.stop_execution.side_effect = stop_error
    operator.on_kill()
    assert operator.hook.stop_execution.call_count == int(enabled)


def test_unexpected_cancellation_error_propagates(operator):
    operator._execution_name = "execution"
    operator.hook.stop_execution.side_effect = RuntimeError("bug")
    with pytest.raises(RuntimeError, match="bug"):
        operator.on_kill()


@pytest.mark.parametrize("timeout", [None, 4])
def test_lookup_error_propagates(operator, timeout):
    operator.timeout_seconds = timeout
    operator.hook.get_execution.side_effect = AzureError("read failed")
    with pytest.raises(AzureError, match="read failed"):
        operator.execute({})


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"timeout_seconds": 0}, "timeout_seconds"),
        ({"poll_interval_seconds": 0}, "poll_interval_seconds"),
    ],
)
def test_invalid_configuration(kwargs, message):
    values = {"task_id": "job", "resource_group": "group", "job_name": "job", **kwargs}
    with pytest.raises(ValueError, match=message):
        AzureContainerAppsJobOperator(**values)


@pytest.mark.parametrize("field", ["resource_group", "job_name"])
def test_empty_rendered_target(operator, field):
    setattr(operator, field, "")
    with pytest.raises(ValueError, match=field):
        operator.execute({})
    operator.hook.start_job.assert_not_called()


def test_invalid_rendered_cpu_does_not_submit(operator):
    operator.cpu = "{{ params.cpu }}"
    operator.render_template_fields({"params": {"cpu": "invalid"}})
    with pytest.raises(ValueError, match="could not convert string to float"):
        operator.execute({})
    operator.hook.start_job.assert_not_called()


def test_templates_include_named_container_and_overrides():
    operator = AzureContainerAppsJobOperator(
        task_id="job",
        resource_group="{{ params.group }}",
        job_name="{{ params.job }}",
        container_name="{{ params.container }}",
        subscription_id="{{ params.subscription }}",
        azure_conn_id="{{ params.connection }}",
        image="{{ params.image }}",
        command=["{{ params.command }}"],
        args=["{{ params.arg }}"],
        env_vars={"KEY": "{{ params.value }}"},
        cpu="{{ params.cpu }}",
        memory="{{ params.memory }}",
    )
    operator.render_template_fields(
        {
            "params": {
                "group": "group",
                "job": "job",
                "container": "worker",
                "subscription": "subscription",
                "connection": "connection",
                "image": "image",
                "command": "python",
                "arg": "module",
                "value": "value",
                "cpu": "1",
                "memory": "2Gi",
            }
        }
    )
    assert (operator.resource_group, operator.job_name, operator.container_name) == ("group", "job", "worker")
    assert (operator.subscription_id, operator.azure_conn_id, operator.image) == (
        "subscription",
        "connection",
        "image",
    )
    assert operator.command == ["python"]
    assert operator.args == ["module"]
    assert operator.env_vars == {"KEY": "value"}
    assert (operator.cpu, operator.memory) == ("1", "2Gi")


@patch(f"{MODULE}.AzureContainerAppsJobHook", autospec=True)
def test_hook_uses_templated_connection_and_subscription(hook):
    operator = AzureContainerAppsJobOperator(
        task_id="job",
        resource_group="group",
        job_name="job",
        azure_conn_id="connection",
        subscription_id="subscription",
    )
    assert operator.hook is hook.return_value
    hook.assert_called_once_with("connection", "subscription")
