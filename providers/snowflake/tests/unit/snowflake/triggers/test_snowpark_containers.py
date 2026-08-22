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
from unittest import mock

import pytest

from airflow.providers.snowflake.triggers.snowpark_containers import SnowparkContainerJobTrigger
from airflow.triggers.base import TriggerEvent

TRIGGER_PATH = "airflow.providers.snowflake.triggers.snowpark_containers"
CLASSPATH = f"{TRIGGER_PATH}.SnowparkContainerJobTrigger"
HOOK = f"{TRIGGER_PATH}.SnowflakeHook"

JOB_NAME = "TEST_JOB"
CONN_ID = "snowflake_default"
POLL_INTERVAL = 1.0


class TestSnowparkContainerJobTrigger:
    @staticmethod
    def _describe(status):
        return {"status": status}

    def _trigger(self, end_time=None, **kwargs):
        params = {
            "job_name": JOB_NAME,
            "snowflake_conn_id": CONN_ID,
            "poll_interval": POLL_INTERVAL,
            "end_time": end_time if end_time is not None else time.time() + 3600,
        }
        params.update(kwargs)
        return SnowparkContainerJobTrigger(**params)

    def test_serialization(self):
        end_time = time.time() + 3600
        execution_deadline = time.time() + 1800
        class_path, kwargs = self._trigger(
            end_time,
            execution_deadline=execution_deadline,
            database="db",
            schema="sc",
            role="r",
            warehouse="wh",
        ).serialize()
        assert class_path == CLASSPATH
        assert kwargs == {
            "job_name": JOB_NAME,
            "snowflake_conn_id": CONN_ID,
            "poll_interval": POLL_INTERVAL,
            "end_time": end_time,
            "execution_deadline": execution_deadline,
            "database": "db",
            "schema": "sc",
            "role": "r",
            "warehouse": "wh",
        }

    @mock.patch(HOOK, autospec=True)
    def test_get_hook_builds_hook_from_connection_settings(self, mock_hook_cls):
        self._trigger(database="db", schema="sc", role="r", warehouse="wh")._get_hook()
        mock_hook_cls.assert_called_once_with(
            snowflake_conn_id=CONN_ID,
            warehouse="wh",
            database="db",
            schema="sc",
            role="r",
        )

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_describe_status_returns_none_when_no_row(self, mock_hook_cls):
        mock_hook_cls.return_value.run.return_value = None
        trigger = self._trigger()
        assert await trigger._describe_status(trigger._get_hook()) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ("DONE", "FAILED", "CANCELLED", "INTERNAL_ERROR"))
    @mock.patch(HOOK, autospec=True)
    async def test_terminal_status_yields_status_event(self, mock_hook_cls, status):
        mock_hook_cls.return_value.run.return_value = self._describe(status)
        event = await self._trigger().run().__anext__()
        assert event == TriggerEvent({"status": status, "job_name": JOB_NAME})

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_unexpected_status_yields_error(self, mock_hook_cls):
        mock_hook_cls.return_value.run.return_value = self._describe("RANDOM")
        event = await self._trigger().run().__anext__()
        assert event.payload["status"] == "error"
        assert "unexpected status" in event.payload["message"]

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_timeout_yields_timeout_event(self, mock_hook_cls):
        mock_hook_cls.return_value.run.return_value = self._describe("RUNNING")
        event = await self._trigger(end_time=time.time() - 1).run().__anext__()
        assert event.payload["status"] == "timeout"
        assert event.payload["job_name"] == JOB_NAME

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_execution_deadline_yields_timeout(self, mock_hook_cls):
        mock_hook_cls.return_value.run.return_value = self._describe("RUNNING")
        trigger = self._trigger(end_time=time.time() + 3600, execution_deadline=time.time() - 1)
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "timeout"
        assert "execution timeout" in event.payload["message"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("end_time", "execution_deadline"),
        (
            (500.0, None),
            (2000.0, 500.0),
        ),
    )
    @mock.patch(HOOK, autospec=True)
    async def test_deadline_takes_precedence_over_terminal_status(
        self, mock_hook_cls, end_time, execution_deadline, time_machine
    ):
        time_machine.move_to(1000, tick=False)
        mock_hook_cls.return_value.run.return_value = self._describe("DONE")
        trigger = self._trigger(end_time=end_time, execution_deadline=execution_deadline)
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "timeout"
        mock_hook_cls.return_value.run.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("end_time", "execution_deadline"),
        (
            (1000.0, None),
            (2000.0, 1000.0),
        ),
    )
    @mock.patch(HOOK, autospec=True)
    async def test_deadline_boundary_counts_as_expired(
        self, mock_hook_cls, end_time, execution_deadline, time_machine
    ):
        time_machine.move_to(1000, tick=False)
        mock_hook_cls.return_value.run.return_value = self._describe("RUNNING")
        trigger = self._trigger(end_time=end_time, execution_deadline=execution_deadline)
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "timeout"
        mock_hook_cls.return_value.run.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_future_execution_deadline_does_not_short_circuit(self, mock_hook_cls):
        mock_hook_cls.return_value.run.return_value = self._describe("DONE")
        trigger = self._trigger(end_time=time.time() + 3600, execution_deadline=time.time() + 1800)
        event = await trigger.run().__anext__()
        assert event == TriggerEvent({"status": "DONE", "job_name": JOB_NAME})

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_poll_exception_yields_error(self, mock_hook_cls):
        mock_hook_cls.return_value.run.side_effect = RuntimeError("boom")
        event = await self._trigger().run().__anext__()
        assert event == TriggerEvent({"status": "error", "job_name": JOB_NAME, "message": "boom"})

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.asyncio.sleep")
    @mock.patch(HOOK, autospec=True)
    async def test_polls_through_non_terminal_then_terminal(self, mock_hook_cls, mock_sleep):
        mock_hook_cls.return_value.run.side_effect = [
            self._describe("PENDING"),
            self._describe("RUNNING"),
            self._describe("DONE"),
        ]
        event = await self._trigger().run().__anext__()
        assert event == TriggerEvent({"status": "DONE", "job_name": JOB_NAME})
        assert mock_sleep.await_count == 2

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_on_kill_drops_service(self, mock_hook_cls):
        await self._trigger().on_kill()
        mock_hook_cls.return_value.run.assert_called_once_with(f"DROP SERVICE IF EXISTS {JOB_NAME}")

    @pytest.mark.asyncio
    @mock.patch(HOOK, autospec=True)
    async def test_on_kill_logs_error_on_failure(self, mock_hook_cls):
        mock_hook_cls.return_value.run.side_effect = RuntimeError("drop failed")
        trigger = self._trigger()
        with mock.patch.object(trigger.log, "error") as mock_error:
            await trigger.on_kill()
        mock_error.assert_called_once()
