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

import contextlib
import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel, Field

from airflow.providers.common.ai.batch.base import RawResultItem
from airflow.providers.common.ai.batch.openai import OpenAIBatchAdapter
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError, LLMBatchModelMismatchError

openai = pytest.importorskip("openai")

from openai.types import Batch  # noqa: E402

KEY = "k" * 16


def _batch(
    *,
    status: str = "completed",
    completed: int = 1,
    failed: int = 0,
    total: int = 1,
    errors: list[dict] | None = None,
    metadata: dict | None = None,
    created_at: int = 1_000_000,
    id_: str = "batch_123",
) -> Batch:
    """A real SDK ``Batch`` so the adapter is exercised against the shape the API returns."""
    return Batch.model_validate(
        {
            "id": id_,
            "object": "batch",
            "endpoint": "/v1/chat/completions",
            "input_file_id": "file-input-xyz",
            "completion_window": "24h",
            "status": status,
            "created_at": created_at,
            "output_file_id": "file-out",
            "error_file_id": "file-err",
            "request_counts": {"completed": completed, "failed": failed, "total": total},
            "errors": {"object": "list", "data": errors} if errors is not None else None,
            "metadata": metadata,
        }
    )


@dataclass
class _FakeUploadedFile:
    id: str = "file-input-xyz"


class _FakeStreamingContent:
    def __init__(self, contents: dict[str, list[str]]):
        self._contents = contents

    @contextlib.contextmanager
    def content(self, file_id: str):
        lines = self._contents.get(file_id, [])
        yield SimpleNamespace(iter_lines=lambda: iter(lines))


class _FakeFiles:
    """Fake for ``client.files``: records the upload and serves canned result files through the streaming API."""

    def __init__(self):
        self.created: list[dict] = []
        self._contents: dict[str, list[str]] = {}
        self.with_streaming_response = _FakeStreamingContent(self._contents)

    def create(self, *, file, purpose):
        self.created.append({"file": file, "purpose": purpose})
        return _FakeUploadedFile()

    def set_content(self, file_id: str, lines: list[dict | str]):
        self._contents[file_id] = [line if isinstance(line, str) else json.dumps(line) for line in lines]


class _FakeBatches:
    def __init__(self):
        self.created: list[dict] = []
        self.cancelled: list[str] = []
        self._listing: list[Any] = []
        self.batch = _batch()

    def create(self, **kwargs):
        self.created.append(kwargs)
        return self.batch

    def retrieve(self, batch_id):
        return self.batch

    def cancel(self, batch_id):
        self.cancelled.append(batch_id)
        return self.batch

    def set_listing(self, batches: list[Any]) -> None:
        self._listing = batches

    def list(self, *, limit=100):
        return iter(self._listing)


class _FakeOpenAIClient:
    def __init__(self):
        self.files = _FakeFiles()
        self.batches = _FakeBatches()
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    return _FakeOpenAIClient()


@pytest.fixture
def adapter(client):
    return OpenAIBatchAdapter(client=client)


@pytest.fixture
def str_spec():
    return build_output_spec(str)


class Diagnosis(BaseModel):
    age: int


def _submit(adapter, requests, spec, **kwargs):
    return adapter.submit(
        requests, model="gpt-5", idempotency_key=KEY, input_fingerprint="fp" * 8, output_spec=spec, **kwargs
    )


def _uploaded_lines(client) -> list[dict]:
    uploaded_bytes = client.files.created[0]["file"][1]
    return [json.loads(line) for line in uploaded_bytes.decode().splitlines()]


def _success_line(index: int, content: str | None = "hello", *, finish_reason: str = "stop") -> dict:
    return {
        "custom_id": f"{KEY}-{index}",
        "response": {
            "status_code": 200,
            "body": {
                "model": "gpt-5",
                "choices": [{"message": {"content": content}, "finish_reason": finish_reason}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 1},
            },
        },
    }


class TestClientConstruction:
    def test_builds_the_sdk_client_from_api_key_and_base_url(self):
        adapter = OpenAIBatchAdapter(api_key="sk-test", base_url="https://gateway.example/v1")
        try:
            assert isinstance(adapter._client, openai.OpenAI)
            assert adapter._client.api_key == "sk-test"
            assert str(adapter._client.base_url).rstrip("/") == "https://gateway.example/v1"
        finally:
            adapter.close()

    def test_close_closes_the_client(self, adapter, client):
        adapter.close()
        assert client.closed is True


class TestSubmitRequestShape:
    def test_custom_id_is_key16_dash_index(self, adapter, client, str_spec):
        _submit(adapter, [{"prompt": "a"}, {"prompt": "b"}], str_spec)
        assert [line["custom_id"] for line in _uploaded_lines(client)] == [f"{KEY}-0", f"{KEY}-1"]

    def test_upload_is_newline_terminated_jsonl(self, adapter, client, str_spec):
        _submit(adapter, [{"prompt": "a"}, {"prompt": "b"}], str_spec)
        name, uploaded_bytes = client.files.created[0]["file"]
        assert name == f"{KEY}.jsonl"
        assert uploaded_bytes.endswith(b"\n")
        assert uploaded_bytes.count(b"\n") == 2
        assert client.files.created[0]["purpose"] == "batch"

    def test_body_uses_max_completion_tokens_never_max_tokens(self, adapter, client, str_spec):
        """``max_tokens`` is deprecated and rejected by reasoning models such as gpt-5."""
        _submit(adapter, [{"prompt": "a"}, {"prompt": "b", "max_tokens": 7}], str_spec, max_tokens=99)
        bodies = [line["body"] for line in _uploaded_lines(client)]
        assert bodies[0]["max_completion_tokens"] == 99
        assert bodies[1]["max_completion_tokens"] == 7
        assert all("max_tokens" not in body for body in bodies)

    def test_request_params_cannot_override_the_managed_keys(self, adapter, client, str_spec):
        _submit(
            adapter,
            [{"prompt": "a", "params": {"messages": [{"role": "user", "content": "CLOBBER"}]}}],
            str_spec,
            request_params={"model": "someone-elses-model", "temperature": 0.2, "max_tokens": 5},
        )
        body = _uploaded_lines(client)[0]["body"]
        assert body["model"] == "gpt-5"
        assert body["messages"] == [{"role": "user", "content": "a"}]
        assert body["max_completion_tokens"] == 1024
        assert "max_tokens" not in body
        assert body["temperature"] == 0.2

    def test_submit_records_idempotency_key_and_fingerprint_in_metadata(self, adapter, client, str_spec):
        adapter.submit(
            [{"prompt": "a"}],
            model="gpt-5",
            idempotency_key="a1b2c3d4e5f60718",
            input_fingerprint="0123456789abcdef" + "extra-material-hashed-away",
            output_spec=str_spec,
        )
        metadata = client.batches.created[0]["metadata"]
        assert metadata == {"idempotency_key": "a1b2c3d4e5f60718", "input_fingerprint": "0123456789abcdef"}

    def test_response_format_is_merged_inside_body_not_alongside_it(self, adapter, client):
        spec = build_output_spec(Diagnosis)
        _submit(adapter, [{"prompt": "a"}], spec)
        line = _uploaded_lines(client)[0]
        assert "response_format" not in line
        assert line["body"]["response_format"]["json_schema"]["name"] == "Diagnosis"
        assert line["body"]["response_format"]["json_schema"]["schema"]["type"] == "object"
        assert line["body"]["messages"][-1] == {"role": "user", "content": "a"}

    def test_submit_returns_batch_id_and_input_file_ref(self, adapter, str_spec):
        result = _submit(adapter, [{"prompt": "a"}], str_spec)
        assert result.batch_id == "batch_123"
        assert result.provider_input_ref == "file-input-xyz"


class TestValidateRequests:
    def test_over_limit_request_count_rejected(self, adapter, str_spec):
        adapter.max_requests = 2
        with pytest.raises(LLMBatchLimitExceededError, match="at most 2 requests"):
            adapter.validate_requests(
                [{"prompt": "a"}, {"prompt": "b"}, {"prompt": "c"}],
                model="gpt-5",
                idempotency_key=KEY,
                output_spec=str_spec,
            )

    def test_mixed_per_request_models_rejected(self, adapter, str_spec):
        with pytest.raises(LLMBatchModelMismatchError, match="Per-request model overrides"):
            adapter.validate_requests(
                [{"prompt": "a", "model": "openai:gpt-5-mini"}, {"prompt": "b"}],
                model="gpt-5",
                idempotency_key=KEY,
                output_spec=str_spec,
            )

    def test_uniform_models_pass(self, adapter, str_spec):
        adapter.validate_requests(
            [{"prompt": "a", "model": "openai:gpt-5"}, {"prompt": "b"}],
            model="gpt-5",
            idempotency_key=KEY,
            output_spec=str_spec,
        )

    def test_foreign_provider_per_request_model_rejected(self, adapter, str_spec):
        with pytest.raises(LLMBatchModelMismatchError, match="Request 0"):
            adapter.validate_requests(
                [{"prompt": "a", "model": "anthropic:claude-3-opus"}],
                model="gpt-5",
                idempotency_key=KEY,
                output_spec=str_spec,
            )

    def test_payload_size_estimate_includes_output_directive(self, adapter):
        class Big(BaseModel):
            field: str = Field(description="x" * 5000)

        adapter.max_payload_bytes = 100
        with pytest.raises(LLMBatchLimitExceededError, match="output_type schema adds"):
            adapter.validate_requests(
                [{"prompt": "a"}], model="gpt-5", idempotency_key=KEY, output_spec=build_output_spec(Big)
            )

    def test_payload_size_estimate_matches_the_uploaded_bytes(self, adapter, client, str_spec):
        requests = [{"prompt": "a" * 50}, {"prompt": "b" * 70}]
        _submit(adapter, requests, str_spec)
        uploaded = len(client.files.created[0]["file"][1])
        adapter.max_payload_bytes = uploaded
        adapter.validate_requests(requests, model="gpt-5", idempotency_key=KEY, output_spec=str_spec)
        adapter.max_payload_bytes = uploaded - 1
        with pytest.raises(LLMBatchLimitExceededError):
            adapter.validate_requests(requests, model="gpt-5", idempotency_key=KEY, output_spec=str_spec)

    def test_over_length_custom_id_rejected(self, adapter, str_spec):
        with pytest.raises(LLMBatchLimitExceededError, match="custom_id"):
            adapter.validate_requests(
                [{"prompt": "a"}] * 100_000, model="gpt-5", idempotency_key="k" * 60, output_spec=str_spec
            )


class TestFindOrphanedBatch:
    _FP = "f" * 16

    def _batch(self, id_, *, key="the-key", fp=None, created_at=1_000_000):
        return _batch(
            id_=id_,
            created_at=created_at,
            metadata={"idempotency_key": key, "input_fingerprint": fp if fp is not None else self._FP},
        )

    def test_finds_batch_with_matching_metadata(self, adapter, client):
        client.batches.set_listing([self._batch("batch_other", key="other-key"), self._batch("batch_match")])
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") == "batch_match"

    def test_returns_none_when_no_batch_matches(self, adapter, client):
        client.batches.set_listing([self._batch("batch_other", key="x")])
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") is None

    def test_returns_none_when_metadata_is_missing(self, adapter, client):
        client.batches.set_listing([_batch(id_="batch_other", metadata=None, created_at=0)])
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") is None

    def test_fingerprint_mismatch_is_not_recovered(self, adapter, client):
        """The key alone is stable across a ``clear`` with different prompts; only the fingerprint ties a batch to this input."""
        client.batches.set_listing([self._batch("batch_stale", fp="stale-fingerprint")])
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") is None

    def test_candidate_created_well_before_not_before_is_rejected(self, adapter, client):
        not_before = "1970-01-12T13:46:50+00:00"  # epoch 1_000_010
        client.batches.set_listing([self._batch("batch_too_old", created_at=999_000)])
        assert adapter.find_orphaned_batch("the-key", self._FP, not_before) is None

    def test_candidate_created_within_clock_skew_slack_is_still_recovered(self, adapter, client):
        """``created_at`` is whole seconds while ``not_before`` carries microseconds."""
        not_before = "1970-01-12T13:46:50.500000+00:00"  # epoch 1_000_010.5
        client.batches.set_listing([self._batch("batch_same_second", created_at=1_000_010)])
        assert adapter.find_orphaned_batch("the-key", self._FP, not_before) == "batch_same_second"

    def test_candidate_created_120s_before_not_before_is_still_recovered(self, adapter, client):
        not_before = "1970-01-12T13:46:50+00:00"
        client.batches.set_listing([self._batch("batch_120s_early", created_at=1_000_010 - 120)])
        assert adapter.find_orphaned_batch("the-key", self._FP, not_before) == "batch_120s_early"

    def test_most_recently_created_match_wins_among_ties(self, adapter, client):
        client.batches.set_listing(
            [
                self._batch("batch_older", created_at=1_000_000),
                self._batch("batch_newer", created_at=2_000_000),
            ]
        )
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") == "batch_newer"


class TestGetBatchAndCancel:
    def test_get_batch_maps_completed_status(self, adapter):
        state = adapter.get_batch("batch_123")
        assert state.status == "completed"
        assert state.counts == {"succeeded": 1, "errored": 0, "expired": 0, "cancelled": 0}
        assert state.error_message is None

    def test_cancel_batch_calls_sdk_cancel(self, adapter, client):
        adapter.cancel_batch("batch_123")
        assert client.batches.cancelled == ["batch_123"]

    def test_expired_batch_attributes_remainder_to_expired(self, adapter, client):
        client.batches.batch = _batch(status="expired", completed=2, failed=1, total=10)
        assert adapter.get_batch("batch_123").counts == {
            "succeeded": 2,
            "errored": 1,
            "expired": 7,
            "cancelled": 0,
        }

    def test_cancelled_batch_attributes_remainder_to_cancelled(self, adapter, client):
        client.batches.batch = _batch(status="cancelled", completed=3, failed=0, total=5)
        assert adapter.get_batch("batch_123").counts == {
            "succeeded": 3,
            "errored": 0,
            "expired": 0,
            "cancelled": 2,
        }

    def test_failed_batch_renders_the_providers_errors_as_text(self, adapter, client):
        client.batches.batch = _batch(
            status="failed",
            completed=0,
            total=0,
            errors=[
                {
                    "code": "token_limit_exceeded",
                    "message": "Enqueued token limit reached",
                    "line": None,
                    "param": None,
                }
            ],
        )
        state = adapter.get_batch("batch_123")
        assert state.status == "failed"
        assert state.error_message == "token_limit_exceeded: Enqueued token limit reached"

    def test_empty_errors_list_is_no_message(self, adapter, client):
        client.batches.batch = _batch(errors=[])
        assert adapter.get_batch("batch_123").error_message is None


class TestIterResults:
    def test_success_and_error_files_both_yielded(self, adapter, client):
        client.files.set_content("file-out", [_success_line(0)])
        client.files.set_content(
            "file-err", [{"custom_id": f"{KEY}-1", "error": {"message": "rate limited"}, "response": None}]
        )
        items = list(adapter.iter_results("batch_123"))
        assert [item.index for item in items] == [0, 1]
        assert items[0].provider_status == "success"
        assert items[0].raw == "hello"
        assert items[1].provider_status == "errored"
        assert items[1].error["message"] == "rate limited"

    def test_batch_expired_error_lines_are_expired_not_errored(self, adapter, client):
        """OpenAI writes requests it never ran to the error file with code ``batch_expired``."""
        client.files.set_content("file-out", [_success_line(0)])
        client.files.set_content(
            "file-err",
            [
                {
                    "custom_id": f"{KEY}-1",
                    "response": None,
                    "error": {
                        "code": "batch_expired",
                        "message": "This request could not be executed before the completion window expired.",
                    },
                }
            ],
        )
        items = list(adapter.iter_results("batch_123"))
        assert items[1].provider_status == "expired"
        assert items[1].error["type"] == "expired"
        assert items[1].error["provider_code"] == "batch_expired"

    def test_per_request_http_error_uses_the_body_error_message(self, adapter, client):
        client.files.set_content(
            "file-err",
            [
                {
                    "custom_id": f"{KEY}-0",
                    "error": None,
                    "response": {
                        "status_code": 400,
                        "request_id": "req_x",
                        "body": {
                            "error": {
                                "message": "Unsupported parameter: 'max_tokens'",
                                "code": "unsupported_parameter",
                            }
                        },
                    },
                }
            ],
        )
        client.files.set_content("file-out", [])
        (item,) = adapter.iter_results("batch_123")
        assert item.provider_status == "errored"
        assert item.error["message"] == "Unsupported parameter: 'max_tokens'"
        assert item.error["provider_code"] == "unsupported_parameter"

    def test_a_malformed_line_is_skipped_and_the_stream_continues(self, adapter, client):
        client.files.set_content(
            "file-out", ["<html>502 Bad Gateway</html>", _success_line(1), json.dumps({"custom_id": None})]
        )
        items = list(adapter.iter_results("batch_123"))
        assert [item.index for item in items] == [1]

    def test_files_are_read_through_the_streaming_response(self, adapter, client):
        """A plain ``files.content()`` reads the whole file into memory; the streaming form must be used."""
        assert not hasattr(client.files, "content")
        client.files.set_content("file-out", [_success_line(0)])
        assert len(list(adapter.iter_results("batch_123"))) == 1


class TestExtractOutput:
    def _raw(self, content) -> RawResultItem:
        return RawResultItem(
            custom_id=f"{KEY}-0",
            index=0,
            provider_status="success",
            model="gpt-5",
            usage=None,
            finish_reason="stop",
            error=None,
            raw=content,
        )

    def test_unstructured_is_plain_text(self, adapter, str_spec):
        extracted = adapter.extract_output(self._raw("hello"), str_spec)
        assert extracted.kind == "text"
        assert extracted.text == "hello"

    def test_structured_is_json_text(self, adapter):
        extracted = adapter.extract_output(self._raw('{"age": 3}'), build_output_spec(Diagnosis))
        assert extracted.kind == "json_text"

    @pytest.mark.parametrize("output_type", [str, Diagnosis])
    def test_null_content_is_absent_not_success(self, adapter, output_type):
        """A refusal, or a reasoning model that spent its budget before visible output, has no content."""
        assert adapter.extract_output(self._raw(None), build_output_spec(output_type)).kind == "absent"
