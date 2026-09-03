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

from unittest.mock import Mock

import pytest
from openai.types.batch import Batch
from openai.types.responses import Response
from openai.types.responses.response import IncompleteDetails

from airflow.providers.common.compat.sdk import Context, TaskDeferred
from airflow.providers.openai.exceptions import OpenAIBatchJobException, OpenAITriggerEventError
from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.providers.openai.operators.openai import (
    OpenAIEmbeddingOperator,
    OpenAIResponseOperator,
    OpenAITriggerBatchOperator,
)
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger

openai = pytest.importorskip("openai")
TASK_ID = "TaskId"
CONN_ID = "test_conn_id"
BATCH_ID = "batch_id"
FILE_ID = "file_id"
BATCH_ENDPOINT = "/v1/chat/completions"


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
    # Backward-compat lock: without max_output_tokens/max_tool_calls, create_response must be
    # called with exactly the arguments it received before those parameters existed.
    mock_hook_instance.create_response.assert_called_once_with(
        input="Write a haiku.",
        model="test_model",
        instructions="Be concise.",
        previous_response_id="resp_prev",
    )


def _completed_response(**overrides):
    defaults = {"output_text": "haiku text", "id": "resp_123", "status": "completed"}
    return Mock(spec=Response, **{**defaults, **overrides})


class TestOpenAIResponseOperatorTokenCeilings:
    @pytest.mark.parametrize(
        ("kwargs", "expected_extra"),
        [
            pytest.param({"max_output_tokens": 100}, {"max_output_tokens": 100}, id="max_output_tokens-int"),
            pytest.param({"max_tool_calls": 5}, {"max_tool_calls": 5}, id="max_tool_calls-int"),
            pytest.param(
                {"max_output_tokens": "100"}, {"max_output_tokens": 100}, id="max_output_tokens-numeric-str"
            ),
            pytest.param({"max_tool_calls": "5"}, {"max_tool_calls": 5}, id="max_tool_calls-numeric-str"),
            pytest.param(
                {"max_output_tokens": 100, "max_tool_calls": 5},
                {"max_output_tokens": 100, "max_tool_calls": 5},
                id="both",
            ),
        ],
    )
    def test_valid_ceiling_forwarded_as_int(self, kwargs, expected_extra):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", **kwargs
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _completed_response()
        operator.hook = mock_hook_instance

        operator.execute(Context())

        mock_hook_instance.create_response.assert_called_once_with(
            input="Write a haiku.", model="gpt-4o-mini", **expected_extra
        )

    @pytest.mark.parametrize(
        "invalid_value",
        [
            pytest.param("not-a-number", id="non-integer-string"),
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
            pytest.param("-5", id="negative-string"),
            pytest.param(10.5, id="float"),
            pytest.param(True, id="bool-true"),
            pytest.param(False, id="bool-false"),
        ],
    )
    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_invalid_ceiling_raises_before_request(self, param_name, invalid_value):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", **{param_name: invalid_value}
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        operator.hook = mock_hook_instance

        with pytest.raises(ValueError, match=param_name):
            operator.execute(Context())

        mock_hook_instance.create_response.assert_not_called()

    @pytest.mark.parametrize("param_name", ["max_output_tokens", "max_tool_calls"])
    def test_ceiling_conflicting_with_response_kwargs_raises(self, param_name):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            response_kwargs={param_name: 50},
            **{param_name: 100},
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        operator.hook = mock_hook_instance

        with pytest.raises(ValueError, match=param_name):
            operator.execute(Context())

        mock_hook_instance.create_response.assert_not_called()

    def test_max_output_tokens_and_max_tool_calls_are_templated(self):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Write a haiku.",
            max_output_tokens="{{ params.tokens }}",
            max_tool_calls="{{ params.calls }}",
        )

        operator.render_template_fields(Context(params={"tokens": 100, "calls": 5}))

        assert operator.max_output_tokens == "100"
        assert operator.max_tool_calls == "5"
        assert "max_output_tokens" in operator.template_fields
        assert "max_tool_calls" in operator.template_fields

        # The rendered strings must still make it to the SDK as real ints, not left as strings.
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _completed_response()
        operator.hook = mock_hook_instance

        operator.execute(Context())

        call_kwargs = mock_hook_instance.create_response.call_args.kwargs
        for key, expected in (("max_output_tokens", 100), ("max_tool_calls", 5)):
            assert call_kwargs[key] == expected
            assert isinstance(call_kwargs[key], int)
            assert not isinstance(call_kwargs[key], bool)

    def test_incomplete_details_reason_is_logged(self, caplog):
        operator = OpenAIResponseOperator(
            task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.", max_output_tokens=10
        )
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _completed_response(
            status="incomplete",
            incomplete_details=IncompleteDetails(reason="max_output_tokens"),
            output_text="Truncated hai",
        )
        operator.hook = mock_hook_instance

        with caplog.at_level("WARNING"):
            result = operator.execute(Context())

        assert result == "Truncated hai"
        assert any(
            "incomplete_details.reason=max_output_tokens" in message and "truncated, not empty" in message
            for message in caplog.messages
        )
        assert not any("may be empty" in message for message in caplog.messages)

    def test_non_completed_without_incomplete_details_keeps_may_be_empty_message(self, caplog):
        operator = OpenAIResponseOperator(task_id=TASK_ID, conn_id=CONN_ID, input_text="Write a haiku.")
        mock_hook_instance = Mock(spec=OpenAIHook)
        mock_hook_instance.create_response.return_value = _completed_response(status="failed", output_text="")
        operator.hook = mock_hook_instance

        with caplog.at_level("WARNING"):
            operator.execute(Context())

        assert any(
            "ended with status failed" in message and "may be empty" in message for message in caplog.messages
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
