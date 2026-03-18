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
    END_TIME = time.time() + 24 * 60 * 60
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

    def test_serialization(self):
        """Assert TestOpenAIBatchTrigger correctly serializes its arguments and class path."""
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
        )
        class_path, kwargs = trigger.serialize()
        assert class_path == "airflow.providers.openai.triggers.openai.OpenAIBatchTrigger"
        assert kwargs == {
            "conn_id": self.CONN_ID,
            "batch_id": self.BATCH_ID,
            "poll_interval": self.POLL_INTERVAL,
            "end_time": self.END_TIME,
        }

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
            end_time=self.END_TIME,
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
    @mock.patch("time.time")
    async def test_openai_batch_for_timeout(self, mock_check_time, mock_batch, mock_batch_status):
        """Assert that run trigger messages in case of batch is still running after timeout"""
        MOCK_TIME = 1724068066.6468632
        mock_batch.return_value = self.mock_get_batch(mock_batch_status)
        mock_check_time.return_value = MOCK_TIME + 1
        trigger = OpenAIBatchTrigger(
            conn_id=self.CONN_ID,
            batch_id=self.BATCH_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=MOCK_TIME,
        )
        expected_result = {
            "status": "error",
            "message": f"Batch {self.BATCH_ID} has not reached a terminal status after {mock_check_time.return_value - MOCK_TIME} seconds.",
            "batch_id": self.BATCH_ID,
        }
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.1)
        assert TriggerEvent(expected_result) == task.result()
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
            end_time=self.END_TIME,
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
