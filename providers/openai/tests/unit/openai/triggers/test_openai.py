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
import itertools
import time
from typing import Literal
from unittest import mock

import pytest
from openai.types import Batch

from airflow.providers.openai.hooks.openai import BatchStatus
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger
from airflow.triggers.base import TriggerEvent

openai = pytest.importorskip("openai")


class TestOpenAIBatchTrigger:
    BATCH_ID = "batch_id"
    CONN_ID = "openai_default"
    TIMEOUT = 24 * 60 * 60
    LEGACY_END_TIME = time.time() + 24 * 60 * 60
    POLL_INTERVAL = 3.0

    def mock_get_batch(
        self,
        status: Literal[
            "validating",
            "failed",
            "in_progress",
            "finalizing",
            "completed",
            "expired",
            "cancelling",
            "cancelled",
        ],
    ) -> Batch:
        return Batch(
            id=self.BATCH_ID,
            object="batch",
            completion_window="24h",
            created_at=1699061776,
            endpoint="/v1/chat/completions",
            input_file_id="file-id",
            status=status,
        )

    def test_serialization_with_timeout(self):
        """Trigger constructed with ``timeout`` round-trips through ``serialize``."""
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            timeout=self.TIMEOUT,
        )
        class_path, kwargs = trigger.serialize()
        assert class_path == "airflow.providers.openai.triggers.openai.OpenAIBatchTrigger"
        assert kwargs == {
            "conn_id": self.CONN_ID,
            "batch_id": self.BATCH_ID,
            "poll_interval": self.POLL_INTERVAL,
            "timeout": self.TIMEOUT,
        }

    def test_serialization_with_legacy_end_time(self):
        """A trigger constructed with the legacy ``end_time`` re-serializes with ``end_time``.

        This preserves on-disk compatibility with triggers that were serialized by the
        pre-fix operator and are still in flight during a rolling upgrade.
        """
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.LEGACY_END_TIME,
        )
        _, kwargs = trigger.serialize()
        assert kwargs == {
            "conn_id": self.CONN_ID,
            "batch_id": self.BATCH_ID,
            "poll_interval": self.POLL_INTERVAL,
            "end_time": self.LEGACY_END_TIME,
        }

    def test_requires_one_of_timeout_or_end_time(self):
        with pytest.raises(ValueError, match="requires either 'timeout' or 'end_time'"):
            OpenAIBatchTrigger(
                conn_id=self.CONN_ID,
                batch_id=self.BATCH_ID,
                poll_interval=self.POLL_INTERVAL,
            )

    def test_rejects_both_timeout_and_end_time(self):
        with pytest.raises(ValueError, match="not both"):
            OpenAIBatchTrigger(
                conn_id=self.CONN_ID,
                batch_id=self.BATCH_ID,
                poll_interval=self.POLL_INTERVAL,
                timeout=self.TIMEOUT,
                end_time=self.LEGACY_END_TIME,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_batch_status", "mock_status", "mock_message"),
        [
            (str(BatchStatus.COMPLETED), "success", "Batch batch_id has completed successfully."),
            (str(BatchStatus.CANCELLING), "cancelled", "Batch batch_id has been cancelled."),
            (str(BatchStatus.CANCELLED), "cancelled", "Batch batch_id has been cancelled."),
            (str(BatchStatus.FAILED), "error", "Batch failed:\nbatch_id"),
            (
                str(BatchStatus.EXPIRED),
                "error",
                "Batch couldn't be completed within the hour time window :\nbatch_id",
            ),
        ],
    )
    @mock.patch("airflow.providers.openai.hooks.openai.OpenAIHook.get_batch")
    async def test_openai_batch_for_terminal_status(
        self, mock_batch, mock_batch_status, mock_status, mock_message
    ):
        """Assert that run trigger messages in case of job finished"""
        mock_batch.return_value = self.mock_get_batch(mock_batch_status)
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            timeout=self.TIMEOUT,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "batch_id": self.BATCH_ID,
        }
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert TriggerEvent(expected_result) == task.result()
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_batch_status",
        [
            (str(BatchStatus.FINALIZING)),
            (str(BatchStatus.IN_PROGRESS)),
            (str(BatchStatus.VALIDATING)),
        ],
    )
    @mock.patch("airflow.providers.openai.hooks.openai.OpenAIHook.get_batch")
    @mock.patch("airflow.providers.openai.triggers.openai.time.monotonic")
    async def test_openai_batch_for_timeout(self, mock_monotonic, mock_batch, mock_batch_status):
        """Trigger reports a timeout error once the monotonic elapsed time exceeds ``timeout``.

        ``time.monotonic`` is patched with an ever-increasing counter rather than a
        fixed list: the asyncio event loop also calls ``time.monotonic`` internally,
        so a finite ``side_effect`` would be exhausted by the loop and raise
        ``StopIteration``. The trigger reads the clock twice with no ``await`` in
        between, so its measured elapsed time is exactly one counter step (100s),
        which exceeds the 10s timeout.
        """
        mock_monotonic.side_effect = itertools.count(start=0.0, step=100.0).__next__
        mock_batch.return_value = self.mock_get_batch(mock_batch_status)
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            timeout=10.0,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.1)
        event = task.result()
        assert event.payload["status"] == "error"
        assert f"Batch {self.BATCH_ID} has not reached a terminal status after" in event.payload["message"]
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.openai.hooks.openai.OpenAIHook.get_batch")
    @mock.patch("airflow.providers.openai.triggers.openai.time.monotonic")
    @mock.patch("airflow.providers.openai.triggers.openai.time.time")
    async def test_timeout_uses_monotonic_not_wall_clock(self, mock_wall, mock_monotonic, mock_batch):
        """Regression: the polling timeout is decided by the monotonic clock only.

        The pre-fix trigger compared ``self.end_time`` against ``time.time()`` inside
        the loop, so a wall-clock jump (NTP correction, DST, VM pause/resume) could
        extend or shorten the timeout. A ``timeout``-constructed trigger must never
        consult the wall clock for its timeout decision; ``time.time`` is asserted
        unused. ``time.monotonic`` uses an ever-increasing counter for the same
        event-loop reason as ``test_openai_batch_for_timeout``.
        """
        mock_monotonic.side_effect = itertools.count(start=0.0, step=100.0).__next__
        mock_batch.return_value = self.mock_get_batch(str(BatchStatus.IN_PROGRESS))
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            timeout=10.0,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.1)
        event = task.result()
        assert event.payload["status"] == "error"
        mock_wall.assert_not_called()
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.openai.hooks.openai.OpenAIHook.get_batch")
    async def test_openai_batch_for_unexpected_error(self, mock_batch):
        """Assert that run trigger messages in case of unexpected error"""
        mock_batch.return_value = 1.0  # FORCE FAILURE TO TEST EXCEPTION
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            timeout=self.TIMEOUT,
        )
        expected_result = {
            "status": "error",
            "message": "'float' object has no attribute 'status'",
            "batch_id": self.BATCH_ID,
        }
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.1)
        assert TriggerEvent(expected_result) == task.result()
        asyncio.get_event_loop().stop()
