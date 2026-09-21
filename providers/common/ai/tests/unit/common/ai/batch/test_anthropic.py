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

from typing import Any

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.batch.anthropic import AnthropicBatchAdapter
from airflow.providers.common.ai.batch.base import RawResultItem
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError, LLMBatchModelMismatchError

anthropic = pytest.importorskip("anthropic")

from anthropic.types import Message, TextBlock, ToolUseBlock  # noqa: E402
from anthropic.types.messages import MessageBatch, MessageBatchIndividualResponse  # noqa: E402

KEY = "k" * 16


def _message_batch(
    *,
    processing_status: str = "ended",
    cancel_initiated_at: str | None = None,
    succeeded: int = 1,
    errored: int = 0,
    canceled: int = 0,
    expired: int = 0,
    processing: int = 0,
) -> MessageBatch:
    """A real SDK ``MessageBatch`` so the adapter is exercised against the shape the API returns."""
    return MessageBatch.model_validate(
        {
            "id": "msgbatch_123",
            "type": "message_batch",
            "processing_status": processing_status,
            "created_at": "2026-09-11T00:00:00+00:00",
            "expires_at": "2026-09-12T00:00:00+00:00",
            "ended_at": "2026-09-11T01:00:00+00:00" if processing_status == "ended" else None,
            "archived_at": None,
            "cancel_initiated_at": cancel_initiated_at,
            "results_url": "https://example.invalid/results" if processing_status == "ended" else None,
            "request_counts": {
                "succeeded": succeeded,
                "errored": errored,
                "canceled": canceled,
                "expired": expired,
                "processing": processing,
            },
        }
    )


def _succeeded(index: int, text: str = "hi") -> MessageBatchIndividualResponse:
    return MessageBatchIndividualResponse.model_validate(
        {
            "custom_id": f"{KEY}-{index}",
            "result": {
                "type": "succeeded",
                "message": {
                    "id": "msg_1",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-sonnet-5",
                    "content": [{"type": "text", "text": text}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 5, "output_tokens": 2},
                },
            },
        }
    )


def _errored(index: int, *, error_type: str = "overloaded_error", message: str = "Overloaded") -> Any:
    return MessageBatchIndividualResponse.model_validate(
        {
            "custom_id": f"{KEY}-{index}",
            "result": {
                "type": "errored",
                "error": {
                    "type": "error",
                    "request_id": "req_1",
                    "error": {"type": error_type, "message": message},
                },
            },
        }
    )


def _terminal(index: int, result_type: str) -> Any:
    return MessageBatchIndividualResponse.model_validate(
        {"custom_id": f"{KEY}-{index}", "result": {"type": result_type}}
    )


class _FakeBatches:
    def __init__(self):
        self.created: list[dict] = []
        self.cancelled: list[str] = []
        self._results: list = []
        self.batch = _message_batch()

    def create(self, *, requests):
        self.created.append({"requests": requests})
        return self.batch

    def retrieve(self, batch_id):
        return self.batch

    def cancel(self, batch_id):
        self.cancelled.append(batch_id)

    def set_results(self, results):
        self._results = results

    def results(self, batch_id):
        return iter(self._results)


class _FakeMessages:
    def __init__(self):
        self.batches = _FakeBatches()


class _FakeAnthropicClient:
    def __init__(self):
        self.messages = _FakeMessages()
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    return _FakeAnthropicClient()


@pytest.fixture
def adapter(client):
    return AnthropicBatchAdapter(client=client)


@pytest.fixture
def str_spec():
    return build_output_spec(str)


class Diagnosis(BaseModel):
    age: int


def _submit(adapter, requests, spec, **kwargs):
    return adapter.submit(
        requests,
        model="claude-sonnet-5",
        idempotency_key=KEY,
        input_fingerprint="fp" * 8,
        output_spec=spec,
        **kwargs,
    )


class TestClientConstruction:
    def test_builds_the_sdk_client_from_api_key_and_base_url(self):
        adapter = AnthropicBatchAdapter(api_key="sk-ant-test", base_url="https://gateway.example")
        try:
            assert isinstance(adapter._client, anthropic.Anthropic)
            assert adapter._client.api_key == "sk-ant-test"
            assert str(adapter._client.base_url).rstrip("/") == "https://gateway.example"
        finally:
            adapter.close()

    def test_close_closes_the_client(self, adapter, client):
        adapter.close()
        assert client.closed is True


class TestSubmitRequestShape:
    def test_custom_id_and_no_batch_level_metadata(self, adapter, client, str_spec):
        """The SDK's ``batches.create`` has no ``metadata`` kwarg; only ``requests`` may be passed."""
        _submit(adapter, [{"prompt": "a"}], str_spec)
        call = client.messages.batches.created[0]
        assert set(call) == {"requests"}
        assert call["requests"][0]["custom_id"] == f"{KEY}-0"

    def test_per_request_model_override_is_allowed(self, adapter, client, str_spec):
        requests = [{"prompt": "a", "model": "anthropic:claude-opus-5"}, {"prompt": "b"}]
        adapter.validate_requests(
            requests, model="claude-sonnet-5", idempotency_key=KEY, output_spec=str_spec
        )
        _submit(adapter, requests, str_spec)
        params = [r["params"] for r in client.messages.batches.created[0]["requests"]]
        assert params[0]["model"] == "claude-opus-5"
        assert params[1]["model"] == "claude-sonnet-5"

    def test_foreign_provider_per_request_model_rejected(self, adapter, str_spec):
        with pytest.raises(LLMBatchModelMismatchError, match="Request 0"):
            adapter.validate_requests(
                [{"prompt": "a", "model": "openai:gpt-5"}],
                model="claude-sonnet-5",
                idempotency_key=KEY,
                output_spec=str_spec,
            )

    def test_tool_choice_forces_the_declared_tool(self, adapter, client):
        spec = build_output_spec(Diagnosis)
        _submit(adapter, [{"prompt": "a"}], spec)
        params = client.messages.batches.created[0]["requests"][0]["params"]
        assert params["tool_choice"] == {"type": "tool", "name": "Diagnosis"}
        assert params["tools"][0]["input_schema"] == spec.json_schema

    def test_system_prompt_is_a_top_level_field_not_a_message(self, adapter, client, str_spec):
        _submit(adapter, [{"prompt": "a"}], str_spec, system_prompt="be terse")
        params = client.messages.batches.created[0]["requests"][0]["params"]
        assert params["system"] == "be terse"
        assert params["messages"] == [{"role": "user", "content": "a"}]

    def test_request_params_cannot_override_the_managed_keys(self, adapter, client, str_spec):
        _submit(
            adapter,
            [{"prompt": "a", "params": {"messages": [{"role": "user", "content": "CLOBBER"}]}}],
            str_spec,
            request_params={"model": "someone-elses-model", "temperature": 0.2, "max_tokens": 5},
        )
        params = client.messages.batches.created[0]["requests"][0]["params"]
        assert params["model"] == "claude-sonnet-5"
        assert params["messages"] == [{"role": "user", "content": "a"}]
        assert params["max_tokens"] == 1024
        assert params["temperature"] == 0.2

    def test_over_length_custom_id_rejected(self, adapter, str_spec):
        with pytest.raises(LLMBatchLimitExceededError, match="custom_id"):
            adapter.validate_requests(
                [{"prompt": "a"}] * 100_000,
                model="claude-sonnet-5",
                idempotency_key="k" * 60,
                output_spec=str_spec,
            )

    def test_over_limit_request_count_rejected(self, adapter, str_spec):
        adapter.max_requests = 1
        with pytest.raises(LLMBatchLimitExceededError, match="at most 1 requests"):
            adapter.validate_requests(
                [{"prompt": "a"}, {"prompt": "b"}],
                model="claude-sonnet-5",
                idempotency_key=KEY,
                output_spec=str_spec,
            )


class TestFindOrphanedBatch:
    def test_always_returns_none(self, adapter):
        assert adapter.find_orphaned_batch("any-key", "any-fp", "1970-01-01T00:00:00+00:00") is None


class TestGetBatchAndCancel:
    def test_ended_status_maps_to_completed_with_full_counts(self, adapter, client):
        client.messages.batches.batch = _message_batch(succeeded=3, errored=1, expired=2, canceled=0)
        state = adapter.get_batch("msgbatch_123")
        assert state.status == "completed"
        assert state.counts == {"succeeded": 3, "errored": 1, "expired": 2, "cancelled": 0}

    def test_in_progress_and_canceling_are_in_progress(self, adapter, client):
        for status in ("in_progress", "canceling"):
            client.messages.batches.batch = _message_batch(processing_status=status, processing=4)
            assert adapter.get_batch("msgbatch_123").status == "in_progress"

    def test_cancel_batch_calls_sdk_cancel(self, adapter, client):
        adapter.cancel_batch("msgbatch_123")
        assert client.messages.batches.cancelled == ["msgbatch_123"]

    def test_ended_with_cancel_initiated_at_maps_to_cancelled_not_completed(self, adapter, client):
        """``processing_status`` alone conflates a normal end with an end after cancellation."""
        client.messages.batches.batch = _message_batch(
            cancel_initiated_at="2026-09-11T00:30:00+00:00", canceled=4
        )
        state = adapter.get_batch("msgbatch_123")
        assert state.status == "cancelled"
        assert state.counts["cancelled"] == 4

    def test_ended_with_null_cancel_initiated_at_maps_to_completed(self, adapter, client):
        client.messages.batches.batch = _message_batch(cancel_initiated_at=None)
        assert adapter.get_batch("msgbatch_123").status == "completed"


class TestExtractOutput:
    def _raw(self, content: list) -> RawResultItem:
        return RawResultItem(
            custom_id=f"{KEY}-0",
            index=0,
            provider_status="success",
            model="claude-sonnet-5",
            usage={"input_tokens": 1, "output_tokens": 1},
            finish_reason="end_turn",
            error=None,
            raw=content,
        )

    def test_absent_when_only_text_block_is_returned(self, adapter):
        """A forced tool_choice still permits a plain-text response; it must map to kind='absent'."""
        extracted = adapter.extract_output(
            self._raw([TextBlock(type="text", text="I cannot comply.")]), build_output_spec(Diagnosis)
        )
        assert extracted.kind == "absent"
        assert extracted.text == "I cannot comply."

    def test_tool_use_block_yields_json_value(self, adapter):
        block = ToolUseBlock(type="tool_use", id="tu_1", name="Diagnosis", input={"age": 30})
        extracted = adapter.extract_output(self._raw([block]), build_output_spec(Diagnosis))
        assert extracted.kind == "json_value"
        assert extracted.value == {"age": 30}

    def test_unstructured_joins_text_blocks(self, adapter, str_spec):
        blocks = [TextBlock(type="text", text="a"), TextBlock(type="text", text="b")]
        assert adapter.extract_output(self._raw(blocks), str_spec).text == "ab"

    def test_unstructured_with_no_text_is_none_not_empty_string(self, adapter, str_spec):
        assert adapter.extract_output(self._raw([]), str_spec).text is None


class TestIterResults:
    def test_mixed_results_stream(self, adapter, client):
        client.messages.batches.set_results([_succeeded(0), _errored(1)])
        items = list(adapter.iter_results("msgbatch_123"))
        assert items[0].provider_status == "success"
        assert items[0].usage == {"input_tokens": 5, "output_tokens": 2}
        assert isinstance(items[0].raw[0], TextBlock)
        assert items[1].provider_status == "errored"

    def test_errored_item_exposes_the_providers_message_and_error_type(self, adapter, client):
        """The SDK nests the message at ``result.error.error.message``; the row must carry it."""
        client.messages.batches.set_results(
            [_errored(0, error_type="invalid_request_error", message="max_tokens: must be greater than 0")]
        )
        (item,) = adapter.iter_results("msgbatch_123")
        assert item.error == {
            "type": "errored",
            "message": "max_tokens: must be greater than 0",
            "provider_code": "invalid_request_error",
            "stage": "provider",
        }

    def test_per_item_expired_and_canceled_are_not_folded_into_errored(self, adapter, client):
        client.messages.batches.set_results([_terminal(0, "expired"), _terminal(1, "canceled")])
        items = list(adapter.iter_results("msgbatch_123"))
        assert items[0].provider_status == "expired"
        assert items[0].error["type"] == "expired"
        assert items[0].error["message"] == "batch item expired"
        assert items[1].provider_status == "cancelled"
        assert items[1].error["type"] == "canceled"

    def test_a_malformed_item_is_skipped_and_the_stream_continues(self, adapter, client):
        bad = MessageBatchIndividualResponse.model_validate(
            {"custom_id": "no-index-here", "result": {"type": "expired"}}
        )
        client.messages.batches.set_results([bad, _succeeded(1)])
        items = list(adapter.iter_results("msgbatch_123"))
        assert [item.index for item in items] == [1]

    def test_succeeded_message_is_a_real_sdk_message(self, adapter, client):
        client.messages.batches.set_results([_succeeded(0)])
        (item,) = adapter.iter_results("msgbatch_123")
        assert isinstance(client.messages.batches._results[0].result.message, Message)
        assert item.model == "claude-sonnet-5"
        assert item.finish_reason == "end_turn"
