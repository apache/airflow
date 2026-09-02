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

import importlib
import sys
import warnings
from types import ModuleType
from typing import Any, Literal, cast
from unittest.mock import Mock

import httpx
import pytest
from openai.types.batch import Batch
from openai.types.responses import Response

from airflow.providers.common.compat.sdk import Context, TaskDeferred
from airflow.providers.openai.exceptions import OpenAIBatchJobException, OpenAITriggerEventError
from airflow.providers.openai.hooks.openai import BatchStatus, OpenAIHook
from airflow.providers.openai.operators import openai as openai_operators
from airflow.providers.openai.operators.openai import (
    OpenAIEmbeddingOperator,
    OpenAIResponseOperator,
    OpenAITriggerBatchOperator,
)
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

openai = pytest.importorskip("openai")
TASK_ID = "TaskId"
CONN_ID = "test_conn_id"
BATCH_ID = "batch_id"
NEW_BATCH_ID = "new_batch_id"
FILE_ID = "file_id"
BATCH_ENDPOINT: Literal["/v1/chat/completions"] = "/v1/chat/completions"


@pytest.fixture
def mock_batch():
    return Batch(
        id=BATCH_ID,
        object="batch",
        completion_window="24h",
        created_at=1699061776,
        endpoint=BATCH_ENDPOINT,
        input_file_id=FILE_ID,
        status="in_progress",
    )


def test_execute_with_input_text():
    operator = OpenAIEmbeddingOperator(
        task_id=TASK_ID, conn_id=CONN_ID, model="test_model", input_text="Test input text"
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.create_embeddings.return_value = [1.0, 2.0, 3.0]
    operator.hook = mock_hook_instance

    context = Context()
    embeddings = operator.execute(context)

    assert embeddings == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("invalid_input", ["", None, 123])
def test_execute_with_invalid_input(invalid_input):
    operator = OpenAIEmbeddingOperator(
        task_id=TASK_ID, conn_id=CONN_ID, model="test_model", input_text=invalid_input
    )
    context = Context()
    with pytest.raises(
        ValueError,
        match="The 'input_text' must be a non-empty string, list of strings, list of integers, or list of lists of integers.",
    ):
        operator.execute(context)


def test_openai_response_operator_execute():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Write a haiku.",
        model="test_model",
        response_kwargs={"instructions": "Be concise.", "previous_response_id": "resp_prev"},
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.create_response.return_value = Mock(
        spec=Response, output_text="haiku text", id="resp_123", status="completed"
    )
    operator.hook = mock_hook_instance

    result = operator.execute(Context())

    assert result == "haiku text"
    mock_hook_instance.create_response.assert_called_once_with(
        input="Write a haiku.",
        model="test_model",
        instructions="Be concise.",
        previous_response_id="resp_prev",
    )


@pytest.mark.parametrize("wait_for_completion", [True, False])
def test_openai_trigger_batch_operator_not_deferred(mock_batch, wait_for_completion):
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        wait_for_completion=wait_for_completion,
        deferrable=False,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    context = Context()
    batch_id = operator.execute(context)
    assert batch_id == BATCH_ID


@pytest.mark.parametrize("wait_for_completion", [True, False])
def test_openai_trigger_batch_operator_with_deferred(mock_batch, wait_for_completion):
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        deferrable=True,
        wait_for_completion=wait_for_completion,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    context = Context()
    if wait_for_completion:
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)
        assert isinstance(exc.value.trigger, OpenAIBatchTrigger)
    else:
        batch_id = operator.execute(context)
        assert batch_id == BATCH_ID


class FakeTaskStateStore:
    def __init__(self, stored: dict[str, Any] | None = None) -> None:
        self.values: dict[str, Any] = dict(stored or {})
        self.get_keys: list[str] = []
        self.set_items: list[tuple[str, Any]] = []

    def get(self, key: str) -> Any:
        self.get_keys.append(key)
        return self.values.get(key)

    def set(self, key: str, value: Any) -> None:
        self.set_items.append((key, value))
        self.values[key] = value


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS,
    reason="ResumableJobMixin requires task_state_store, available in Airflow 3.3+",
)
class TestOpenAITriggerBatchOperatorResumable:
    def _operator(self, **kwargs: Any) -> OpenAITriggerBatchOperator:
        return OpenAITriggerBatchOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            file_id=FILE_ID,
            endpoint=BATCH_ENDPOINT,
            deferrable=False,
            **kwargs,
        )

    def _hook(self, *, status: str = BatchStatus.IN_PROGRESS) -> Mock:
        hook = Mock(spec=OpenAIHook)
        hook.create_batch.return_value = Mock(spec=Batch, id=NEW_BATCH_ID)
        hook.get_batch.return_value = Mock(spec=Batch, status=status)
        return hook

    def _context(self, store: FakeTaskStateStore) -> Context:
        return cast("Context", {"task_state_store": store, "ti": Mock(stats_tags={})})

    def test_first_run_persists_batch_id_before_waiting(self):
        operator = self._operator()
        operator.hook = hook = self._hook()
        store = FakeTaskStateStore()
        persisted_before_wait: list[Any] = []
        hook.wait_for_batch.side_effect = lambda *_, **__: persisted_before_wait.append(
            store.values.get("openai_batch_id")
        )

        result = operator.execute(self._context(store))

        assert result == NEW_BATCH_ID
        assert persisted_before_wait == [NEW_BATCH_ID]
        hook.create_batch.assert_called_once_with(file_id=FILE_ID, endpoint=BATCH_ENDPOINT)

    @pytest.mark.parametrize(
        "status",
        [BatchStatus.VALIDATING, BatchStatus.IN_PROGRESS, BatchStatus.FINALIZING],
    )
    def test_retry_reconnects_active_batch_and_restores_on_kill(self, status):
        operator = self._operator()
        operator.hook = hook = self._hook(status=status)
        store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})
        batch_id_before_lookup: list[str | None] = []
        hook.get_batch.side_effect = lambda **_: (
            batch_id_before_lookup.append(operator.batch_id) or Mock(spec=Batch, status=status)
        )

        result = operator.execute(self._context(store))
        operator.on_kill()

        assert result == BATCH_ID
        assert batch_id_before_lookup == [BATCH_ID]
        assert operator.batch_id == BATCH_ID
        hook.create_batch.assert_not_called()
        hook.wait_for_batch.assert_called_once_with(
            batch_id=BATCH_ID,
            wait_seconds=operator.wait_seconds,
            timeout=operator.timeout,
        )
        hook.cancel_batch.assert_called_once_with(batch_id=BATCH_ID)

    def test_retry_returns_completed_batch_without_waiting(self):
        operator = self._operator()
        operator.hook = hook = self._hook(status=BatchStatus.COMPLETED)
        store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})

        result = operator.execute(self._context(store))

        assert result == BATCH_ID
        assert operator.batch_id == BATCH_ID
        hook.create_batch.assert_not_called()
        hook.wait_for_batch.assert_not_called()

    @pytest.mark.parametrize(
        "status",
        [BatchStatus.FAILED, BatchStatus.EXPIRED, BatchStatus.CANCELLING, BatchStatus.CANCELLED],
    )
    def test_retry_submits_fresh_after_terminal_batch(self, status):
        operator = self._operator()
        operator.hook = hook = self._hook(status=status)
        store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})

        result = operator.execute(self._context(store))

        assert result == NEW_BATCH_ID
        assert store.values["openai_batch_id"] == NEW_BATCH_ID
        hook.create_batch.assert_called_once_with(file_id=FILE_ID, endpoint=BATCH_ENDPOINT)
        hook.wait_for_batch.assert_called_once_with(
            batch_id=NEW_BATCH_ID,
            wait_seconds=operator.wait_seconds,
            timeout=operator.timeout,
        )

    def test_retry_submits_fresh_when_persisted_batch_is_missing(self) -> None:
        operator = self._operator()
        operator.hook = hook = self._hook()
        store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})
        request = httpx.Request(method="GET", url=f"https://api.openai.com/v1/batches/{BATCH_ID}")
        hook.get_batch.side_effect = openai.NotFoundError(
            "Batch not found",
            response=httpx.Response(status_code=404, request=request),
            body={"error": {"message": "Batch not found"}},
        )

        result = operator.execute(self._context(store))

        assert result == NEW_BATCH_ID
        assert store.values["openai_batch_id"] == NEW_BATCH_ID
        hook.create_batch.assert_called_once_with(file_id=FILE_ID, endpoint=BATCH_ENDPOINT)

    def test_retry_propagates_other_api_errors(self) -> None:
        operator = self._operator()
        operator.hook = hook = self._hook()
        store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})
        error = openai.APIConnectionError(
            request=httpx.Request(method="GET", url=f"https://api.openai.com/v1/batches/{BATCH_ID}")
        )
        hook.get_batch.side_effect = error

        with pytest.raises(openai.APIConnectionError) as exc_info:
            operator.execute(self._context(store))

        assert exc_info.value is error
        hook.create_batch.assert_not_called()

    def test_durable_false_submits_fresh_without_touching_store(self):
        operator = self._operator(durable=False)
        operator.hook = hook = self._hook()
        store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})

        result = operator.execute(self._context(store))

        assert result == NEW_BATCH_ID
        assert store.get_keys == []
        assert store.set_items == []
        hook.create_batch.assert_called_once_with(file_id=FILE_ID, endpoint=BATCH_ENDPOINT)

    def test_default_args_durable_reaches_operator(self):
        operator = OpenAITriggerBatchOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            file_id=FILE_ID,
            endpoint=BATCH_ENDPOINT,
            default_args={"durable": False},
        )

        assert operator.durable is False


@pytest.mark.parametrize("deferrable", [False, True])
def test_fire_and_forget_does_not_use_task_state_store(deferrable):
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        deferrable=deferrable,
        wait_for_completion=False,
    )
    operator.hook = hook = Mock(spec=OpenAIHook)
    hook.create_batch.return_value = Mock(spec=Batch, id=NEW_BATCH_ID)
    store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})

    result = operator.execute({"task_state_store": store})

    assert result == NEW_BATCH_ID
    assert store.get_keys == []
    assert store.set_items == []
    hook.wait_for_batch.assert_not_called()


def test_deferrable_wait_does_not_use_task_state_store():
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        deferrable=True,
    )
    operator.hook = hook = Mock(spec=OpenAIHook)
    hook.create_batch.return_value = Mock(spec=Batch, id=NEW_BATCH_ID)
    store = FakeTaskStateStore({"openai_batch_id": BATCH_ID})

    with pytest.raises(TaskDeferred):
        operator.execute({"task_state_store": store})

    assert store.get_keys == []
    assert store.set_items == []


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = openai_operators._warn_and_disable_durable_pre_3_3(openai_operators._DURABLE_UNSET)
        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value):
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result = openai_operators._warn_and_disable_durable_pre_3_3(value)
        assert result is False

    def test_operator_submits_without_resumable_mixin(self, monkeypatch, mock_batch):
        try:
            with monkeypatch.context() as patch:
                patch.setitem(sys.modules, "airflow.sdk", ModuleType("airflow.sdk"))
                compatibility_module = importlib.reload(openai_operators)
                with pytest.warns(UserWarning, match="durable.*no effect"):
                    operator = compatibility_module.OpenAITriggerBatchOperator(
                        task_id=TASK_ID,
                        conn_id=CONN_ID,
                        file_id=FILE_ID,
                        endpoint=BATCH_ENDPOINT,
                        durable=True,
                    )
                operator.hook = hook = Mock(spec=OpenAIHook)
                hook.create_batch.return_value = mock_batch

                result = operator.execute(Context())
        finally:
            importlib.reload(openai_operators)

        assert result == BATCH_ID
        hook.create_batch.assert_called_once_with(file_id=FILE_ID, endpoint=BATCH_ENDPOINT)
        hook.wait_for_batch.assert_called_once_with(
            batch_id=BATCH_ID,
            wait_seconds=operator.wait_seconds,
            timeout=operator.timeout,
        )


class TestOpenAITriggerBatchOperatorExecuteComplete:
    def _operator(self):
        return OpenAITriggerBatchOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            file_id=FILE_ID,
            endpoint=BATCH_ENDPOINT,
        )

    def test_success_returns_batch_id(self):
        event = {"status": "success", "message": "done", "batch_id": BATCH_ID}
        assert self._operator().execute_complete(Context(), event) == BATCH_ID

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param({"status": "error", "message": "boom", "batch_id": BATCH_ID}, id="error"),
            pytest.param(
                {"status": "cancelled", "message": "Batch has been cancelled.", "batch_id": BATCH_ID},
                id="cancelled",
            ),
        ],
    )
    def test_failed_event_raises(self, event):
        with pytest.raises(OpenAIBatchJobException, match=event["message"]):
            self._operator().execute_complete(Context(), event)

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param(None, id="none"),
            pytest.param({"status": "expired", "batch_id": BATCH_ID}, id="unknown-status"),
        ],
    )
    def test_invalid_event_raises_instead_of_succeeding(self, event):
        with pytest.raises(OpenAITriggerEventError):
            self._operator().execute_complete(Context(), event)
