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

import warnings
from typing import Any
from unittest import mock

import pytest
from anthropic import NotFoundError

from airflow.exceptions import TaskDeferred
from airflow.providers.anthropic.exceptions import (
    AnthropicBatchJobError,
    AnthropicBatchTimeout,
    AnthropicTriggerEventError,
)
from airflow.providers.anthropic.hooks.anthropic import AnthropicHook, BatchStatus
from airflow.providers.anthropic.operators import batch as batch_module
from airflow.providers.anthropic.operators.batch import AnthropicBatchOperator
from airflow.providers.anthropic.triggers.batch import AnthropicBatchTrigger
from airflow.providers.common.compat.sdk import AirflowSkipException

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

pytest.importorskip("anthropic")

REQUESTS = [{"custom_id": "a", "params": {"model": "claude-opus-4-8", "max_tokens": 8, "messages": []}}]


class FakeTaskStateStore:
    def __init__(self, stored: dict[str, Any] | None = None) -> None:
        self._store: dict[str, Any] = dict(stored or {})

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value


class WorkerCrash(BaseException):
    pass


def _counts(
    succeeded: int = 0,
    errored: int = 0,
    canceled: int = 0,
    expired: int = 0,
    processing: int = 0,
) -> mock.MagicMock:
    counts = mock.MagicMock()
    counts.succeeded = succeeded
    counts.errored = errored
    counts.canceled = canceled
    counts.expired = expired
    counts.processing = processing
    return counts


def _batch(
    *,
    batch_id: str = "batch_1",
    processing_status: str = "ended",
    succeeded: int = 0,
    errored: int = 0,
    canceled: int = 0,
    expired: int = 0,
    processing: int = 0,
) -> mock.MagicMock:
    batch = mock.MagicMock()
    batch.id = batch_id
    batch.processing_status = processing_status
    batch.request_counts = _counts(
        succeeded=succeeded,
        errored=errored,
        canceled=canceled,
        expired=expired,
        processing=processing,
    )
    return batch


def _context(task_state_store: Any = None) -> dict[str, Any]:
    ti = mock.MagicMock()
    ti.stats_tags = {}
    context = {"ti": ti}
    if task_state_store is not None:
        context["task_state_store"] = task_state_store
    return context


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

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_persists_batch_id_before_polling(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value = _batch(batch_id="batch_1")
        task_store = FakeTaskStateStore()
        persisted_before_poll = []

        def wait_for_batch(*, batch_id: str, wait_seconds: float, timeout: float) -> mock.MagicMock:
            persisted_before_poll.append(task_store.get("anthropic_batch_id"))
            return _batch(batch_id=batch_id, succeeded=1)

        hook.wait_for_batch.side_effect = wait_for_batch
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        assert op.execute(_context(task_store)) == "batch_1"
        assert persisted_before_poll == ["batch_1"]

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_retry_reconnects_to_active_batch(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = _batch(
            batch_id="batch_existing",
            processing_status=BatchStatus.IN_PROGRESS,
            processing=1,
        )
        hook.wait_for_batch.return_value = _batch(batch_id="batch_existing", succeeded=1)
        mock_hook_prop.return_value = hook
        task_store = FakeTaskStateStore({"anthropic_batch_id": "batch_existing"})
        context = _context(task_store)

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        assert op.execute(context) == "batch_existing"

        hook.create_batch.assert_not_called()
        hook.wait_for_batch.assert_called_once_with(
            batch_id="batch_existing",
            wait_seconds=op.poll_interval,
            timeout=op.timeout,
        )
        context["ti"].xcom_push.assert_called_once_with(key="batch_id", value="batch_existing")

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_retry_recovers_successful_batch(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = _batch(batch_id="batch_existing", succeeded=1)
        mock_hook_prop.return_value = hook
        context = _context(FakeTaskStateStore({"anthropic_batch_id": "batch_existing"}))

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        assert op.execute(context) == "batch_existing"

        hook.create_batch.assert_not_called()
        hook.wait_for_batch.assert_not_called()
        context["ti"].xcom_push.assert_called_once_with(key="batch_id", value="batch_existing")

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @pytest.mark.parametrize(
        ("stored_batch", "fail_on_partial_error"),
        [
            pytest.param(_batch(batch_id="batch_failed", errored=1), True, id="failed"),
            pytest.param(_batch(batch_id="batch_expired", expired=1), True, id="expired"),
            pytest.param(_batch(batch_id="batch_canceled", canceled=1), False, id="canceled"),
            pytest.param(
                _batch(batch_id="batch_partially_canceled", canceled=1, succeeded=1),
                False,
                id="partially-canceled",
            ),
            pytest.param(
                _batch(
                    batch_id="batch_canceling",
                    processing_status=BatchStatus.CANCELING,
                    processing=1,
                ),
                False,
                id="canceling",
            ),
        ],
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_retry_replaces_non_resumable_batch(
        self,
        mock_hook_prop,
        stored_batch,
        fail_on_partial_error,
    ):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = stored_batch
        hook.create_batch.return_value = _batch(batch_id="batch_new")
        hook.wait_for_batch.return_value = _batch(batch_id="batch_new", succeeded=1)
        mock_hook_prop.return_value = hook
        task_store = FakeTaskStateStore({"anthropic_batch_id": stored_batch.id})

        op = AnthropicBatchOperator(
            task_id="t",
            requests=REQUESTS,
            deferrable=False,
            fail_on_partial_error=fail_on_partial_error,
        )
        assert op.execute(_context(task_store)) == "batch_new"

        hook.create_batch.assert_called_once_with(requests=REQUESTS, model=None)
        assert task_store.get("anthropic_batch_id") == "batch_new"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_retry_resubmits_missing_batch(self, mock_hook_prop):
        response = mock.MagicMock()
        response.status_code = 404
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.side_effect = NotFoundError("missing", response=response, body=None)
        hook.create_batch.return_value = _batch(batch_id="batch_new")
        hook.wait_for_batch.return_value = _batch(batch_id="batch_new", succeeded=1)
        mock_hook_prop.return_value = hook
        task_store = FakeTaskStateStore({"anthropic_batch_id": "batch_missing"})

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        assert op.execute(_context(task_store)) == "batch_new"

        hook.create_batch.assert_called_once_with(requests=REQUESTS, model=None)
        assert task_store.get("anthropic_batch_id") == "batch_new"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_retry_rejects_non_string_batch_id(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        with pytest.raises(TypeError, match="Expected Anthropic batch ID to be a string"):
            op.execute(_context(FakeTaskStateStore({"anthropic_batch_id": 42})))

        hook.get_batch.assert_not_called()
        hook.create_batch.assert_not_called()

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_sync_retry_recovers_partial_error_when_non_strict(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.get_batch.return_value = _batch(batch_id="batch_existing", succeeded=9, errored=1)
        mock_hook_prop.return_value = hook
        context = _context(FakeTaskStateStore({"anthropic_batch_id": "batch_existing"}))

        op = AnthropicBatchOperator(
            task_id="t",
            requests=REQUESTS,
            deferrable=False,
            fail_on_partial_error=False,
        )
        assert op.execute(context) == "batch_existing"

        hook.create_batch.assert_not_called()
        hook.wait_for_batch.assert_not_called()
        context["ti"].xcom_push.assert_called_once_with(key="batch_id", value="batch_existing")

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_durable_false_submits_fresh(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value = _batch(batch_id="batch_new")
        hook.wait_for_batch.return_value = _batch(batch_id="batch_new", succeeded=1)
        mock_hook_prop.return_value = hook
        task_store = mock.MagicMock(spec_set=["get", "set"])

        op = AnthropicBatchOperator(
            task_id="t",
            requests=REQUESTS,
            deferrable=False,
            durable=False,
        )
        assert op.execute(_context(task_store)) == "batch_new"

        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_retry_reconnects_to_first_submission(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value = _batch(batch_id="batch_1")
        hook.get_batch.return_value = _batch(
            batch_id="batch_1",
            processing_status=BatchStatus.IN_PROGRESS,
            processing=1,
        )
        hook.wait_for_batch.side_effect = [WorkerCrash(), _batch(batch_id="batch_1", succeeded=1)]
        mock_hook_prop.return_value = hook
        task_store = FakeTaskStateStore()

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        first_context = _context(task_store)
        with pytest.raises(WorkerCrash):
            op.execute(first_context)

        retry_op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        retry_context = _context(task_store)
        assert retry_op.execute(retry_context) == "batch_1"

        hook.create_batch.assert_called_once_with(requests=REQUESTS, model=None)
        hook.get_batch.assert_called_once_with("batch_1")
        assert hook.wait_for_batch.call_count == 2
        hook.cancel_batch.assert_not_called()
        assert task_store.get("anthropic_batch_id") == "batch_1"
        first_context["ti"].xcom_push.assert_called_once_with(key="batch_id", value="batch_1")
        retry_context["ti"].xcom_push.assert_called_once_with(key="batch_id", value="batch_1")

    @pytest.mark.skipif(
        not AIRFLOW_V_3_3_PLUS,
        reason="ResumableJobMixin reconnect requires task_state_store, available in Airflow 3.3+",
    )
    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_retry_replaces_batch_canceled_by_previous_attempt(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.side_effect = [
            _batch(batch_id="batch_1"),
            _batch(batch_id="batch_2"),
        ]
        hook.get_batch.return_value = _batch(
            batch_id="batch_1",
            processing_status=BatchStatus.CANCELING,
            processing=1,
        )
        waited_batch_ids: list[str] = []

        def wait_for_batch(*, batch_id: str, wait_seconds: float, timeout: float) -> mock.MagicMock:
            waited_batch_ids.append(batch_id)
            if len(waited_batch_ids) == 1:
                raise AnthropicBatchTimeout("too slow")
            return _batch(batch_id=batch_id, succeeded=1)

        hook.wait_for_batch.side_effect = wait_for_batch
        mock_hook_prop.return_value = hook
        task_store = FakeTaskStateStore()

        first_op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        with pytest.raises(AnthropicBatchTimeout, match="too slow"):
            first_op.execute(_context(task_store))

        retry_op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=False)
        assert retry_op.execute(_context(task_store)) == "batch_2"

        assert hook.create_batch.call_count == 2
        hook.cancel_batch.assert_called_once_with("batch_1")
        assert waited_batch_ids == ["batch_1", "batch_2"]
        assert task_store.get("anthropic_batch_id") == "batch_2"

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_deferrable_defers_with_trigger(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, deferrable=True)
        task_store = mock.MagicMock(spec_set=["get", "set"])
        with pytest.raises(TaskDeferred) as exc:
            op.execute(_context(task_store))
        assert isinstance(exc.value.trigger, AnthropicBatchTrigger)
        assert exc.value.trigger.batch_id == "batch_1"
        assert exc.value.method_name == "execute_complete"
        hook.wait_for_batch.assert_not_called()
        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_no_wait_returns_immediately(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        hook.create_batch.return_value.id = "batch_1"
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=REQUESTS, wait_for_completion=False)
        task_store = mock.MagicMock(spec_set=["get", "set"])
        assert op.execute(_context(task_store)) == "batch_1"
        hook.wait_for_batch.assert_not_called()
        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

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
        hook.create_batch.assert_called_once_with(requests=REQUESTS, model="claude-haiku-4-5")

    @mock.patch.object(AnthropicBatchOperator, "hook", new_callable=mock.PropertyMock)
    def test_empty_requests_raises_before_any_api_call(self, mock_hook_prop):
        hook = mock.MagicMock(spec=AnthropicHook)
        mock_hook_prop.return_value = hook

        op = AnthropicBatchOperator(task_id="t", requests=[])
        with pytest.raises(ValueError, match="at least one request"):
            op.execute(_context())
        hook.create_batch.assert_not_called()

    def test_default_args_durable_reaches_operator(self):
        op = AnthropicBatchOperator(
            task_id="t",
            requests=REQUESTS,
            default_args={"durable": False},
        )
        assert op.durable is False


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


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = batch_module._warn_and_disable_durable_pre_3_3(batch_module._DURABLE_UNSET)
        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value):
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result = batch_module._warn_and_disable_durable_pre_3_3(value)
        assert result is False
