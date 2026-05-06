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

import time
from unittest.mock import AsyncMock, patch

import pytest

from airflow.providers.informatica.hooks.idmc import IDMCRunStatus, InformaticaIDMCError
from airflow.providers.informatica.triggers.idmc import (
    InformaticaIDMCTaskflowRunTrigger,
    InformaticaIDMCTaskRunTrigger,
)


@pytest.fixture
def task_trigger() -> InformaticaIDMCTaskRunTrigger:
    return InformaticaIDMCTaskRunTrigger(
        conn_id="idmc_test",
        run_id="42",
        end_time=time.time() + 3600,
        poll_interval=0.01,
    )


def test_serialize_round_trips_arguments(task_trigger):
    classpath, kwargs = task_trigger.serialize()
    assert classpath == ("airflow.providers.informatica.triggers.idmc.InformaticaIDMCTaskRunTrigger")
    assert kwargs["conn_id"] == "idmc_test"
    assert kwargs["run_id"] == "42"
    assert kwargs["poll_interval"] == 0.01


def test_taskflow_trigger_classpath():
    trigger = InformaticaIDMCTaskflowRunTrigger(
        conn_id="idmc_test", run_id="x", end_time=time.time() + 1, poll_interval=0.01
    )
    classpath, _ = trigger.serialize()
    assert classpath.endswith("InformaticaIDMCTaskflowRunTrigger")


@pytest.mark.asyncio
async def test_run_emits_terminal_event_on_success(task_trigger):
    fake_hook = AsyncMock()
    fake_hook.aget_task_run_status.return_value = {
        "status": IDMCRunStatus.SUCCESS.value,
        "raw_status": "SUCCESS",
    }
    with patch.object(task_trigger, "_build_hook", return_value=fake_hook):
        events = []
        async for event in task_trigger.run():
            events.append(event)
    assert len(events) == 1
    assert events[0].payload["status"] == IDMCRunStatus.SUCCESS.value
    assert events[0].payload["run_id"] == "42"


@pytest.mark.asyncio
async def test_run_polls_until_terminal(task_trigger):
    fake_hook = AsyncMock()
    fake_hook.aget_task_run_status.side_effect = [
        {"status": IDMCRunStatus.RUNNING.value},
        {"status": IDMCRunStatus.RUNNING.value},
        {"status": IDMCRunStatus.WARNING.value, "raw_status": "WARNING"},
    ]
    with patch.object(task_trigger, "_build_hook", return_value=fake_hook):
        events = []
        async for event in task_trigger.run():
            events.append(event)
    assert fake_hook.aget_task_run_status.await_count == 3
    assert events[-1].payload["status"] == IDMCRunStatus.WARNING.value


@pytest.mark.asyncio
async def test_run_returns_timeout_when_deadline_exceeded():
    trigger = InformaticaIDMCTaskRunTrigger(
        conn_id="idmc_test", run_id="42", end_time=time.time() - 1, poll_interval=0.01
    )
    fake_hook = AsyncMock()
    fake_hook.aget_task_run_status.return_value = {"status": IDMCRunStatus.RUNNING.value}
    with patch.object(trigger, "_build_hook", return_value=fake_hook):
        events = []
        async for event in trigger.run():
            events.append(event)
    assert events[-1].payload["status"] == "timeout"


@pytest.mark.asyncio
async def test_run_emits_terminal_when_final_check_succeeds_after_timeout():
    trigger = InformaticaIDMCTaskRunTrigger(
        conn_id="idmc_test", run_id="42", end_time=time.time() - 1, poll_interval=0.01
    )
    fake_hook = AsyncMock()
    fake_hook.aget_task_run_status.side_effect = [
        {"status": IDMCRunStatus.RUNNING.value},
        {"status": IDMCRunStatus.SUCCESS.value, "raw_status": "SUCCESS"},
    ]
    with patch.object(trigger, "_build_hook", return_value=fake_hook):
        events = []
        async for event in trigger.run():
            events.append(event)
    assert events[-1].payload["status"] == IDMCRunStatus.SUCCESS.value


@pytest.mark.asyncio
async def test_run_emits_error_event_on_idmc_exception(task_trigger):
    fake_hook = AsyncMock()
    fake_hook.aget_task_run_status.side_effect = InformaticaIDMCError("boom")
    with patch.object(task_trigger, "_build_hook", return_value=fake_hook):
        events = []
        async for event in task_trigger.run():
            events.append(event)
    assert events[-1].payload["status"] == "error"
    assert "boom" in events[-1].payload["message"]
