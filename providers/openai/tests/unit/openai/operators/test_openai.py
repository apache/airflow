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

from enum import Enum
from typing import Any
from unittest.mock import Mock

import pytest
from openai.types.batch import Batch
from openai.types.responses import (
    ParsedResponse,
    ParsedResponseOutputMessage,
    ParsedResponseOutputText,
    Response,
    ResponseError,
    ResponseFunctionToolCall,
    ResponseOutputRefusal,
)
from openai.types.responses.response import IncompleteDetails
from pydantic import BaseModel, ValidationError
from pydantic.dataclasses import dataclass as pydantic_dataclass

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
    mock_hook_instance.create_response.assert_called_once_with(
        input="Write a haiku.",
        model="test_model",
        instructions="Be concise.",
        previous_response_id="resp_prev",
    )


class _StructuredPerson(BaseModel):
    """Pydantic model used by the structured-output operator tests."""

    name: str


class _Priority(Enum):
    LOW = "low"
    HIGH = "high"


class _StructuredTask(BaseModel):
    title: str
    priority: _Priority


def _build_parsed_response(
    output_parsed: BaseModel | None = None,
    *,
    response_id: str = "resp_structured",
    status: str = "completed",
    error: ResponseError | None = None,
    incomplete_details: IncompleteDetails | None = None,
    refusal: str | None = None,
    output_items: list[Any] | None = None,
) -> ParsedResponse:
    content: list[ParsedResponseOutputText[BaseModel] | ResponseOutputRefusal]
    if output_items is not None:
        output = output_items
    elif output_parsed is not None:
        content = [
            ParsedResponseOutputText[BaseModel](
                annotations=[],
                text=output_parsed.model_dump_json(),
                type="output_text",
                parsed=output_parsed,
            )
        ]
        output = [
            ParsedResponseOutputMessage[BaseModel](
                id=f"msg_{response_id}",
                content=content,
                role="assistant",
                status="completed",
                type="message",
            )
        ]
    elif refusal is not None:
        content = [ResponseOutputRefusal(refusal=refusal, type="refusal")]
        output = [
            ParsedResponseOutputMessage[BaseModel](
                id=f"msg_{response_id}",
                content=content,
                role="assistant",
                status="completed",
                type="message",
            )
        ]
    else:
        output = []
    return ParsedResponse[BaseModel].model_construct(
        id=response_id,
        status=status,
        output=output,
        error=error,
        incomplete_details=incomplete_details,
    )


def test_openai_response_operator_structured_output_returns_dict():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Extract: Alice",
        model="test_model",
        text_format=_StructuredPerson,
        response_kwargs={"instructions": "Be precise."},
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.parse_response.return_value = _build_parsed_response(
        _StructuredPerson(name="Alice"), response_id="resp_str_1"
    )
    operator.hook = mock_hook_instance

    result = operator.execute(Context())

    assert result == {"name": "Alice"}
    mock_hook_instance.parse_response.assert_called_once_with(
        input="Extract: Alice",
        model="test_model",
        text_format=_StructuredPerson,
        instructions="Be precise.",
    )
    mock_hook_instance.create_response.assert_not_called()


def test_openai_response_operator_structured_output_dumps_enum_as_json():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Classify",
        model="test_model",
        text_format=_StructuredTask,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.parse_response.return_value = _build_parsed_response(
        _StructuredTask(title="Deploy", priority=_Priority.HIGH), response_id="resp_str_2"
    )
    operator.hook = mock_hook_instance

    result = operator.execute(Context())

    assert result == {"title": "Deploy", "priority": "high"}
    assert isinstance(result["priority"], str)


def test_openai_response_operator_structured_output_refusal_raises():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Extract: Alice",
        model="test_model",
        text_format=_StructuredPerson,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.parse_response.return_value = _build_parsed_response(
        response_id="resp_refused",
        refusal="I cannot help with that request.",
    )
    operator.hook = mock_hook_instance

    with pytest.raises(ValueError, match="did not return a structured output") as excinfo:
        operator.execute(Context())
    message = str(excinfo.value)
    assert "status='completed'" in message
    assert "refusal='I cannot help with that request.'" in message


def test_openai_response_operator_structured_output_tools_only_raises():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Extract: Alice",
        model="test_model",
        text_format=_StructuredPerson,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.parse_response.return_value = _build_parsed_response(
        response_id="resp_tool_call",
        output_items=[
            ResponseFunctionToolCall(
                arguments='{"name": "Alice"}',
                call_id="call_1",
                name="extract_person",
                type="function_call",
                status="completed",
            )
        ],
    )
    operator.hook = mock_hook_instance

    with pytest.raises(ValueError, match="did not return a structured output") as excinfo:
        operator.execute(Context())

    assert "output_types=['function_call']" in str(excinfo.value)


def test_openai_response_operator_structured_output_incomplete_raises_with_valid_model():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Extract: Alice",
        model="test_model",
        text_format=_StructuredPerson,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.parse_response.return_value = _build_parsed_response(
        _StructuredPerson(name="Alice"),
        response_id="resp_incomplete",
        status="incomplete",
        incomplete_details=IncompleteDetails(reason="max_output_tokens"),
    )
    operator.hook = mock_hook_instance

    with pytest.raises(ValueError, match="did not complete") as excinfo:
        operator.execute(Context())

    message = str(excinfo.value)
    assert "status='incomplete'" in message
    assert "reason='max_output_tokens'" in message


def test_openai_response_operator_structured_output_failed_raises_with_error():
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Extract: Alice",
        model="test_model",
        text_format=_StructuredPerson,
    )
    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.parse_response.return_value = _build_parsed_response(
        response_id="resp_failed",
        status="failed",
        error=ResponseError(code="server_error", message="The model failed."),
    )
    operator.hook = mock_hook_instance

    with pytest.raises(ValueError, match="did not complete") as excinfo:
        operator.execute(Context())

    message = str(excinfo.value)
    assert "status='failed'" in message
    assert "code='server_error'" in message
    assert "message='The model failed.'" in message


def test_openai_response_operator_structured_output_validation_error_raises():
    # ``responses.parse`` raises ``pydantic.ValidationError`` when the model's JSON output
    # can't be coerced into ``text_format`` (e.g. truncated mid-JSON on ``max_output_tokens``).
    # The operator converts it to a ``ValueError`` so callers see one exception type across
    # all parse failures.
    operator = OpenAIResponseOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        input_text="Extract: Alice",
        model="test_model",
        text_format=_StructuredPerson,
    )
    with pytest.raises(ValidationError) as exc_info:
        _StructuredPerson.model_validate({})

    mock_hook_instance = Mock(spec=OpenAIHook)
    mock_hook_instance.parse_response.side_effect = exc_info.value
    operator.hook = mock_hook_instance

    with pytest.raises(ValueError, match="max_output_tokens"):
        operator.execute(Context())


def test_openai_response_operator_rejects_non_base_model_text_format():
    @pydantic_dataclass
    class StructuredPerson:
        name: str

    with pytest.raises(TypeError, match="Pydantic BaseModel subclass"):
        OpenAIResponseOperator(
            task_id=TASK_ID,
            conn_id=CONN_ID,
            input_text="Extract: Alice",
            text_format=StructuredPerson,
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
