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

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
import time_machine
from azure.core.exceptions import AzureError
from azure.mgmt.appcontainers.models import JobExecution

from airflow.providers.microsoft.azure.triggers.container_apps import AzureContainerAppsJobTrigger

MODULE = "airflow.providers.microsoft.azure.triggers.container_apps"


@pytest.fixture
def hook():
    with patch(f"{MODULE}.AzureContainerAppsJobAsyncHook", autospec=True) as hook:
        yield hook.return_value


@pytest.fixture
def trigger():
    return AzureContainerAppsJobTrigger(resource_group="group", job_name="job", execution_name="execution")


def test_serialization_preserves_deadline_and_target():
    trigger = AzureContainerAppsJobTrigger(
        resource_group="group",
        job_name="job",
        execution_name="execution",
        azure_conn_id="connection",
        subscription_id="subscription",
        poll_interval_seconds=7,
        deadline="2026-01-01T00:01:00+00:00",
        cancel_on_termination=False,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == f"{MODULE}.AzureContainerAppsJobTrigger"
    restored = AzureContainerAppsJobTrigger(**json.loads(json.dumps(kwargs)))
    assert restored.serialize() == trigger.serialize()


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [None, "", "Running", "Processing", "Unknown"])
@patch(f"{MODULE}.asyncio.sleep", autospec=True)
async def test_active_then_success(sleep, hook, trigger, active):
    hook.async_get_execution.side_effect = [JobExecution(status=active), JobExecution(status="Succeeded")]
    result = [event async for event in trigger.run()]
    assert [event.payload for event in result] == [{"status": "success", "execution_name": "execution"}]
    hook.async_get_execution.assert_awaited_with("group", "job", "execution")
    sleep.assert_awaited_once_with(60)
    await trigger.cleanup()
    await trigger.on_kill()
    hook.async_stop_execution.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("execution", "message"),
    [
        (None, "not found"),
        (JobExecution(status="Failed", message="Azure message"), "Azure message"),
        (JobExecution(status="Stopped", reason="Azure reason"), "Azure reason"),
    ],
)
async def test_terminal_failure(hook, trigger, execution, message):
    hook.async_get_execution.return_value = execution
    result = [event async for event in trigger.run()]
    assert len(result) == 1
    assert result[0].payload["status"] == "error"
    assert message in result[0].payload["message"]
    await trigger.on_kill()
    hook.async_stop_execution.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [AzureError("read failed"), RuntimeError("bug"), TimeoutError("SDK timeout")]
)
async def test_read_errors_propagate(hook, trigger, error):
    hook.async_get_execution.side_effect = error
    with pytest.raises(type(error), match=str(error)):
        await anext(trigger.run())
    hook.async_stop_execution.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [None, AzureError("stop failed")])
@time_machine.travel(datetime(2026, 1, 1, tzinfo=UTC), tick=False)
async def test_expired_deadline_on_redistributed_trigger(hook, error):
    original = AzureContainerAppsJobTrigger(
        resource_group="group",
        job_name="job",
        execution_name="execution",
        deadline="2025-12-31T23:59:59+00:00",
        cancel_on_termination=False,
    )
    trigger = AzureContainerAppsJobTrigger(**original.serialize()[1])
    hook.async_stop_execution.side_effect = error
    events = [event async for event in trigger.run()]
    assert events[0].payload["status"] == "error"
    assert "timed out" in events[0].payload["message"]
    hook.async_get_execution.assert_not_called()
    hook.async_stop_execution.assert_awaited_once_with("group", "job", "execution")


@pytest.mark.asyncio
@time_machine.travel(datetime(2026, 1, 1, tzinfo=UTC), tick=False)
@patch(f"{MODULE}.time", autospec=True)
@patch(f"{MODULE}.asyncio.sleep", autospec=True)
async def test_monotonic_timeout_caps_poll_delay(sleep, monotonic, hook, trigger):
    monotonic.monotonic.side_effect = [0, 0, 58, 60]
    trigger.deadline = "2026-01-01T00:01:00+00:00"
    hook.async_get_execution.return_value = JobExecution(status="Running")
    events = [event async for event in trigger.run()]
    sleep.assert_awaited_once_with(2)
    assert "timed out" in events[0].payload["message"]
    hook.async_stop_execution.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("finished_at", [59, 60, 61])
@time_machine.travel(datetime(2026, 1, 1, tzinfo=UTC), tick=False)
@patch(f"{MODULE}.time", autospec=True)
async def test_terminal_lookup_must_finish_before_deadline(clock, hook, trigger, finished_at):
    clock.monotonic.side_effect = [0, 0, finished_at]
    trigger.deadline = "2026-01-01T00:01:00+00:00"
    trigger.cancel_on_termination = False
    hook.async_get_execution.return_value = JobExecution(status="Succeeded")
    events = [event async for event in trigger.run()]
    assert len(events) == 1
    if finished_at < 60:
        assert events[0].payload == {"status": "success", "execution_name": "execution"}
        hook.async_stop_execution.assert_not_called()
    else:
        assert "timed out" in events[0].payload["message"]
        hook.async_stop_execution.assert_awaited_once_with("group", "job", "execution")


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
@pytest.mark.parametrize("error", [None, AzureError("stop failed")])
async def test_explicit_termination(hook, trigger, enabled, error):
    trigger.cancel_on_termination = enabled
    hook.async_stop_execution.side_effect = error
    await trigger.on_kill()
    assert hook.async_stop_execution.await_count == int(enabled)


@pytest.mark.asyncio
async def test_cleanup_does_not_cancel_external_work(hook, trigger):
    await trigger.cleanup()
    hook.async_stop_execution.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [None, "2026-01-01T00:01:00+00:00"])
@time_machine.travel(datetime(2026, 1, 1, tzinfo=UTC), tick=False)
async def test_cancelled_poll_does_not_cancel_external_work(hook, trigger, deadline):
    trigger.deadline = deadline
    hook.async_get_execution.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await anext(trigger.run())
    await trigger.cleanup()
    hook.async_stop_execution.assert_not_called()


@pytest.mark.asyncio
async def test_unexpected_stop_error_propagates(hook, trigger):
    hook.async_stop_execution.side_effect = RuntimeError("bug")
    with pytest.raises(RuntimeError, match="bug"):
        await trigger.on_kill()


def test_invalid_poll_interval():
    with pytest.raises(ValueError, match="poll_interval_seconds"):
        AzureContainerAppsJobTrigger(
            resource_group="group", job_name="job", execution_name="execution", poll_interval_seconds=0
        )
