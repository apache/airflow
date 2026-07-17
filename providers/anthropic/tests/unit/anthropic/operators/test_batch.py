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
from airflow.providers.anthropic.operators.batch import AnthropicBatchOperator
from airflow.providers.anthropic.triggers.batch import AnthropicBatchTrigger
from airflow.providers.common.compat.sdk import AirflowSkipException

pytest.importorskip("anthropic")

REQUESTS = [{"custom_id": "a", "params": {"model": "claude-opus-4-8", "max_tokens": 8, "messages": []}}]


def _counts(succeeded=0, errored=0, canceled=0, expired=0, processing=0):
    counts = mock.MagicMock()
    counts.succeeded = succeeded
    counts.errored = errored
    counts.canceled = canceled
    counts.expired = expired
    counts.processing = processing
    return counts


def _context():
    return {"ti": mock.MagicMock()}


class TestAnthropicBatchOperatorExecute:
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_waits_and_returns_batch_id(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        hook.wait_for_batch.return_value.request_counts = _counts(succeeded=1)
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        context = _context()
        result = op.execute(context)

        assert result == "batch_1"
        hook.wait_for_batch.assert_called_once()
        context["ti"].xcom_push.assert_called_once_with(key="batch_id", value="batch_1")

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_deferrable_defers_with_trigger(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=True)
        with pytest.raises(TaskDeferred) as exc:
            op.execute(_context())
        assert isinstance(exc.value.trigger, AnthropicBatchTrigger)
        assert exc.value.trigger.batch_id == "batch_1"
        assert exc.value.method_name == "execute_complete"
        hook.wait_for_batch.assert_not_called()

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_no_wait_returns_immediately(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, wait_for_completion=False)
        assert op.execute(_context()) == "batch_1"
        hook.wait_for_batch.assert_not_called()

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_applies_policy_skip_on_full_cancel(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        hook.wait_for_batch.return_value.request_counts = _counts(canceled=2)
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        with pytest.raises(AirflowSkipException):
            op.execute(_context())

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_applies_policy_fail_on_partial_error_when_strict(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        hook.wait_for_batch.return_value.request_counts = _counts(succeeded=4, errored=1)
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(
            task_id="t", requests=REQUESTS, deferrable=False, fail_on_partial_error=True
        )
        with pytest.raises(AnthropicBatchJobError):
            op.execute(_context())

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_timeout_cancels_and_raises(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        hook.wait_for_batch.side_effect = AnthropicBatchTimeout("too slow")
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        with pytest.raises(AnthropicBatchTimeout, match="too slow"):
            op.execute(_context())
        hook.cancel_batch.assert_called_once_with("batch_1")

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_non_timeout_error_cancels_and_raises(self, mock_hook_prop):
        # A non-timeout failure while waiting (SDK 5xx, auth expiry) also leaves the batch
        # running, so the broadened except cancels it best-effort before re-raising.
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        hook.wait_for_batch.side_effect = RuntimeError("api 5xx")
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        with pytest.raises(RuntimeError, match="api 5xx"):
            op.execute(_context())
        hook.cancel_batch.assert_called_once_with("batch_1")

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_execute_forwards_model_to_hook(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(
            task_id="t", requests=REQUESTS, model="claude-haiku-4-5", wait_for_completion=False
        )
        op.execute(_context())
        hook.create_batch.assert_called_once_with(REQUESTS, model="claude-haiku-4-5")

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_empty_requests_raises_before_any_api_call(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=[])
        with pytest.raises(ValueError, match="at least one request"):
            op.execute(_context())
        hook.create_batch.assert_not_called()


class TestExecuteComplete:
    def test_success_returns_batch_id(self):
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        event = {"status": "success", "batch_id": "batch_1", "request_counts": {"succeeded": 3}}
        assert op.execute_complete(_context(), event) == "batch_1"

    @mock.patch("airflow.providers.anthropic.operators.batch.AnthropicHook", autospec=True)
    def test_error_cancels_and_raises_job_error(self, mock_hook_cls):
        # The trigger's "error" event means polling gave up while the batch may still be
        # running, so the operator cancels it best-effort before failing.
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        event = {"status": "error", "batch_id": "batch_1", "message": "boom"}
        with pytest.raises(AnthropicBatchJobError, match="boom"):
            op.execute_complete(_context(), event)
        mock_hook_cls.return_value.cancel_batch.assert_called_once_with("batch_1")

    @mock.patch("airflow.providers.anthropic.operators.batch.AnthropicHook", autospec=True)
    def test_timeout_cancels_and_raises(self, mock_hook_cls):
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        event = {"status": "timeout", "batch_id": "batch_1", "message": "too slow"}
        with pytest.raises(AnthropicBatchTimeout, match="too slow"):
            op.execute_complete(_context(), event)
        mock_hook_cls.return_value.cancel_batch.assert_called_once_with("batch_1")

    def test_fully_cancelled_skips(self):
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        event = {"status": "success", "batch_id": "batch_1", "request_counts": {"canceled": 4}}
        with pytest.raises(AirflowSkipException):
            op.execute_complete(_context(), event)

    def test_partial_error_warns_by_default(self):
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        event = {"status": "success", "batch_id": "b", "request_counts": {"succeeded": 9, "errored": 1}}
        assert op.execute_complete(_context(), event) == "b"

    def test_partial_error_fails_when_strict(self):
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, fail_on_partial_error=True)
        event = {"status": "success", "batch_id": "b", "request_counts": {"succeeded": 9, "errored": 1}}
        with pytest.raises(AnthropicBatchJobError, match="failed request"):
            op.execute_complete(_context(), event)

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param(None, id="none"),
            pytest.param({"status": "ended", "batch_id": "b"}, id="unknown-status"),
        ],
    )
    def test_invalid_event_raises_instead_of_succeeding(self, event):
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        with pytest.raises(AnthropicTriggerEventError):
            op.execute_complete(_context(), event)


class TestOnKill:
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_on_kill_cancels_known_batch(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        op.batch_id = "batch_1"
        op.on_kill()
        hook.cancel_batch.assert_called_once_with("batch_1")

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_on_kill_noop_without_batch(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook
        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS)
        op.on_kill()
        hook.cancel_batch.assert_not_called()
