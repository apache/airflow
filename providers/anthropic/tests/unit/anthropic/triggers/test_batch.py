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

from airflow.providers.anthropic.triggers.batch import AnthropicBatchTrigger
from airflow.triggers.base import TriggerEvent

pytest.importorskip("anthropic")

TRIGGER_PATH = "airflow.providers.anthropic.triggers.batch"
HOOK_PATH = "airflow.providers.anthropic.hooks.anthropic.AnthropicHook.get_batch"


def _batch(status, succeeded=0, errored=0, canceled=0, expired=0, processing=0):
    batch = mock.MagicMock()
    batch.processing_status = status
    counts = mock.MagicMock()
    counts.succeeded, counts.errored = succeeded, errored
    counts.canceled, counts.expired, counts.processing = canceled, expired, processing
    batch.request_counts = counts
    return batch


class TestAnthropicBatchTrigger:
    CONN_ID = "anthropic_default"
    BATCH_ID = "batch_1"
    POLL = 1.0

    def _trigger(self, end_time=None):
        return AnthropicBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL,
            end_time=end_time if end_time is not None else time.time() + 3600,
        )

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.AnthropicHook", autospec=True)
    async def test_on_kill_cancels_batch(self, mock_hook_cls):
        # A user killing the deferred task cancels the still-running batch (Airflow 3.3+).
        await self._trigger().on_kill()
        mock_hook_cls.return_value.cancel_batch.assert_called_once_with(self.BATCH_ID)

    def test_serialization(self):
        end_time = time.time() + 3600
        class_path, kwargs = self._trigger(end_time).serialize()
        assert class_path == "airflow.providers.anthropic.triggers.batch.AnthropicBatchTrigger"
        assert kwargs == {
            "conn_id": self.CONN_ID,
            "batch_id": self.BATCH_ID,
            "poll_interval": self.POLL,
            "end_time": end_time,
        }

    @pytest.mark.asyncio
    @mock.patch(HOOK_PATH)
    async def test_ended_yields_success_with_counts(self, mock_get_batch):
        mock_get_batch.return_value = _batch("ended", succeeded=3, errored=1)
        event = await self._trigger().run().__anext__()
        assert event == TriggerEvent(
            {
                "status": "success",
                "batch_id": self.BATCH_ID,
                "message": f"Batch {self.BATCH_ID} has ended.",
                "request_counts": {
                    "succeeded": 3,
                    "errored": 1,
                    "canceled": 0,
                    "expired": 0,
                    "processing": 0,
                },
            }
        )

    @pytest.mark.asyncio
    @mock.patch(HOOK_PATH)
    async def test_timeout_yields_timeout_event(self, mock_get_batch):
        mock_get_batch.return_value = _batch("in_progress")
        # end_time already in the past -> timeout on first poll.
        event = await self._trigger(end_time=time.time() - 1).run().__anext__()
        assert event.payload["status"] == "timeout"
        assert event.payload["batch_id"] == self.BATCH_ID

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.asyncio.sleep")
    @mock.patch(HOOK_PATH)
    async def test_persistent_error_yields_error_after_retries(self, mock_get_batch, mock_sleep):
        mock_get_batch.side_effect = RuntimeError("kaboom")
        event = await self._trigger().run().__anext__()
        assert event == TriggerEvent({"status": "error", "batch_id": self.BATCH_ID, "message": "kaboom"})
        assert mock_get_batch.call_count == 5

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.asyncio.sleep")
    @mock.patch(HOOK_PATH)
    async def test_transient_error_then_success(self, mock_get_batch, mock_sleep):
        mock_get_batch.side_effect = [RuntimeError("blip"), _batch("ended", succeeded=1)]
        event = await self._trigger().run().__anext__()
        assert event.payload["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.asyncio.sleep")
    @mock.patch(HOOK_PATH)
    async def test_polls_until_ended(self, mock_get_batch, mock_sleep):
        mock_get_batch.side_effect = [
            _batch("in_progress"),
            _batch("canceling"),
            _batch("ended", succeeded=1),
        ]
        event = await self._trigger().run().__anext__()
        assert event.payload["status"] == "success"
        assert mock_get_batch.call_count == 3
        assert mock_sleep.await_count == 2
