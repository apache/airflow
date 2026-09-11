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

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.anthropic.exceptions import (
    AnthropicBatchJobError,
    AnthropicBatchTimeout,
    AnthropicTriggerEventError,
)
from airflow.providers.anthropic.hooks.anthropic import AnthropicHook
from airflow.providers.anthropic.sensors.batch import AnthropicBatchSensor
from airflow.providers.anthropic.triggers.batch import AnthropicBatchTrigger
from airflow.providers.common.compat.sdk import AirflowSkipException

pytest.importorskip("anthropic")


def _batch(status, succeeded=0, errored=0, canceled=0, expired=0):
    batch = mock.MagicMock()
    batch.processing_status = status
    counts = batch.request_counts
    counts.succeeded, counts.errored, counts.canceled, counts.expired = (
        succeeded,
        errored,
        canceled,
        expired,
    )
    return batch


class TestAnthropicBatchSensorPoke:
    @mock.patch.object(AnthropicBatchSensor, "hook", new_callable=mock.PropertyMock)
    def test_poke_false_while_in_progress(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = _batch("in_progress")
        mock_hook_prop.return_value = hook
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1")
        assert sensor.poke({}) is False

    @mock.patch.object(AnthropicBatchSensor, "hook", new_callable=mock.PropertyMock)
    def test_poke_true_when_ended(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = _batch("ended", succeeded=5)
        mock_hook_prop.return_value = hook
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1")
        assert sensor.poke({}) is True

    @mock.patch.object(AnthropicBatchSensor, "hook", new_callable=mock.PropertyMock)
    def test_poke_skips_on_full_cancel(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = _batch("ended", canceled=3)
        mock_hook_prop.return_value = hook
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1")
        with pytest.raises(AirflowSkipException):
            sensor.poke({})

    @mock.patch.object(AnthropicBatchSensor, "hook", new_callable=mock.PropertyMock)
    def test_poke_fails_on_partial_error_when_strict(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = _batch("ended", succeeded=4, errored=1)
        mock_hook_prop.return_value = hook
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1", fail_on_partial_error=True)
        with pytest.raises(AnthropicBatchJobError):
            sensor.poke({})


class TestAnthropicBatchSensorDeferrable:
    def test_execute_defers(self):
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1", deferrable=True)
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})
        assert isinstance(exc.value.trigger, AnthropicBatchTrigger)
        assert exc.value.trigger.batch_id == "b1"
        assert exc.value.method_name == "execute_complete"

    def test_execute_complete_error_raises(self):
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1")
        with pytest.raises(AnthropicBatchJobError, match="boom"):
            sensor.execute_complete({}, {"status": "error", "batch_id": "b1", "message": "boom"})

    def test_execute_complete_timeout_raises(self):
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1")
        with pytest.raises(AnthropicBatchTimeout):
            sensor.execute_complete({}, {"status": "timeout", "batch_id": "b1", "message": "slow"})

    def test_execute_complete_success(self):
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1")
        event = {"status": "success", "batch_id": "b1", "request_counts": {"succeeded": 2}}
        assert sensor.execute_complete({}, event) is None

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param(None, id="none"),
            pytest.param({"status": "ended", "batch_id": "b1"}, id="unknown-status"),
        ],
    )
    def test_execute_complete_invalid_event_raises_instead_of_succeeding(self, event):
        sensor = AnthropicBatchSensor(task_id="s", batch_id="b1")
        with pytest.raises(AnthropicTriggerEventError):
            sensor.execute_complete({}, event)
