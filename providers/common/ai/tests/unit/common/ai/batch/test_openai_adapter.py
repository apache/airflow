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

import json
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from airflow.providers.common.ai.batch.openai import OpenAIBatchAdapter
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError, LLMBatchModelMismatchError

# Every test here injects a fake client, so nothing exercises the real SDK import path --
# but this module is what the openai extra / dev-group dependency (pyproject.toml) exists
# for, and guarding consistently with the rest of the suite keeps that intent visible.
pytest.importorskip("openai")


@dataclass
class _FakeUploadedFile:
    id: str = "file-input-xyz"


class _FakeFiles:
    """Fake for ``client.files`` -- records the upload, and serves back canned result files."""

    def __init__(self):
        self.created: list[dict] = []
        self._contents: dict[str, list[str]] = {}

    def create(self, *, file, purpose):
        self.created.append({"file": file, "purpose": purpose})
        return _FakeUploadedFile()

    def set_content(self, file_id: str, lines: list[dict]):
        self._contents[file_id] = [json.dumps(line) for line in lines]

    def content(self, file_id: str):
        lines = self._contents.get(file_id, [])
        return SimpleNamespace(iter_lines=lambda: iter(lines))


class _FakeBatches:
    def __init__(self):
        self.created: list[dict] = []
        self.cancelled: list[str] = []
        self._listing: list[Any] = []
        self._batch = SimpleNamespace(
            id="batch_123",
            status="completed",
            output_file_id="file-out",
            error_file_id="file-err",
            request_counts=SimpleNamespace(completed=1, failed=0, total=1),
            errors=None,
        )

    def create(self, **kwargs):
        self.created.append(kwargs)
        return SimpleNamespace(id="batch_123")

    def retrieve(self, batch_id):
        return self._batch

    def cancel(self, batch_id):
        self.cancelled.append(batch_id)
        return self._batch

    def set_listing(self, batches: list[Any]) -> None:
        self._listing = batches

    def list(self, *, limit=100):
        return iter(self._listing)


class _FakeOpenAIClient:
    def __init__(self):
        self.files = _FakeFiles()
        self.batches = _FakeBatches()


@pytest.fixture
def client():
    return _FakeOpenAIClient()


@pytest.fixture
def adapter(client):
    return OpenAIBatchAdapter(client=client)


@pytest.fixture
def str_spec():
    return build_output_spec(str)


class TestSubmitRequestShape:
    def test_custom_id_is_key16_dash_index(self, adapter, client, str_spec):
        """M9: not ':' -- Anthropic's documented custom_id charset excludes it; kept consistent
        across both adapters."""
        adapter.submit(
            [{"prompt": "a"}, {"prompt": "b"}],
            model="gpt-5",
            idempotency_key="a1b2c3d4e5f60718",
            input_fingerprint="fp" * 8,
            output_spec=str_spec,
        )
        uploaded_bytes = client.files.created[0]["file"][1]
        lines = [json.loads(line) for line in uploaded_bytes.decode().splitlines()]
        assert [line["custom_id"] for line in lines] == ["a1b2c3d4e5f60718-0", "a1b2c3d4e5f60718-1"]

    def test_submit_records_idempotency_key_and_fingerprint_in_metadata(self, adapter, client, str_spec):
        """N1: find_orphaned_batch can only recover on fingerprint match if submit actually wrote
        it into metadata alongside idempotency_key."""
        adapter.submit(
            [{"prompt": "a"}],
            model="gpt-5",
            idempotency_key="a1b2c3d4e5f60718",
            input_fingerprint="0123456789abcdef" + "extra-material-hashed-away",
            output_spec=str_spec,
        )
        metadata = client.batches.created[0]["metadata"]
        assert metadata["idempotency_key"] == "a1b2c3d4e5f60718"
        assert metadata["input_fingerprint"] == "0123456789abcdef"  # truncated to 16 hex chars

    def test_response_format_is_merged_inside_body_not_alongside_it(self, adapter, client):
        from pydantic import BaseModel

        class Diagnosis(BaseModel):
            age: int

        spec = build_output_spec(Diagnosis)
        adapter.submit(
            [{"prompt": "a"}],
            model="gpt-5",
            idempotency_key="k" * 16,
            input_fingerprint="fp" * 8,
            output_spec=spec,
        )
        uploaded_bytes = client.files.created[0]["file"][1]
        line = json.loads(uploaded_bytes.decode().splitlines()[0])
        assert "response_format" not in line  # must not be a sibling of "body"
        assert line["body"]["response_format"]["json_schema"]["name"] == "Diagnosis"
        assert line["body"]["model"] == "gpt-5"
        assert line["body"]["messages"][-1] == {"role": "user", "content": "a"}

    def test_submit_returns_batch_id_and_input_file_ref(self, adapter, str_spec):
        result = adapter.submit(
            [{"prompt": "a"}],
            model="gpt-5",
            idempotency_key="k" * 16,
            input_fingerprint="fp" * 8,
            output_spec=str_spec,
        )
        assert result.batch_id == "batch_123"
        assert result.provider_input_ref == "file-input-xyz"


class TestValidateRequests:
    def test_over_limit_request_count_rejected(self, adapter, str_spec):
        adapter.max_requests = 2  # shrink for a fast test
        with pytest.raises(LLMBatchLimitExceededError, match="at most 2 requests"):
            adapter.validate_requests(
                [{"prompt": "a"}, {"prompt": "b"}, {"prompt": "c"}],
                model="gpt-5",
                idempotency_key="k" * 16,
                output_spec=str_spec,
            )

    def test_mixed_per_request_models_rejected(self, adapter, str_spec):
        with pytest.raises(LLMBatchModelMismatchError, match="Per-request model overrides"):
            adapter.validate_requests(
                [{"prompt": "a", "model": "openai:gpt-5-mini"}, {"prompt": "b"}],
                model="gpt-5",
                idempotency_key="k" * 16,
                output_spec=str_spec,
            )

    def test_uniform_models_pass(self, adapter, str_spec):
        adapter.validate_requests(
            [{"prompt": "a", "model": "openai:gpt-5"}, {"prompt": "b"}],
            model="gpt-5",
            idempotency_key="k" * 16,
            output_spec=str_spec,
        )

    def test_foreign_provider_per_request_model_rejected(self, adapter, str_spec):
        """M10: a per-request model naming a different provider must be rejected, not silently sent."""
        with pytest.raises(LLMBatchModelMismatchError, match="Request 0"):
            adapter.validate_requests(
                [{"prompt": "a", "model": "anthropic:claude-3-opus"}],
                model="gpt-5",
                idempotency_key="k" * 16,
                output_spec=str_spec,
            )

    def test_payload_size_estimate_includes_output_directive(self, adapter):
        from pydantic import BaseModel, Field

        class Big(BaseModel):
            field: str = Field(description="x" * 5000)

        spec = build_output_spec(Big)
        adapter.max_payload_bytes = 100  # force the schema-inflated body to exceed it
        with pytest.raises(LLMBatchLimitExceededError, match="output_type schema adds"):
            adapter.validate_requests(
                [{"prompt": "a"}], model="gpt-5", idempotency_key="k" * 16, output_spec=spec
            )

    def test_over_length_custom_id_rejected(self, adapter, str_spec):
        """N7: the worst-case custom_id (idempotency_key + '-' + largest index) must be checked
        pre-submit, not left to fail (or silently truncate) at the provider."""
        adapter.max_requests = 100_000
        with pytest.raises(LLMBatchLimitExceededError, match="custom_id"):
            adapter.validate_requests(
                [{"prompt": "a"}] * 100_000,
                model="gpt-5",
                idempotency_key="k" * 60,  # way over any real key16, forces an over-length id
                output_spec=str_spec,
            )


class TestFindOrphanedBatch:
    """
    M3/N1: recover a Phase A orphan by matching ``metadata["idempotency_key"]`` *and*
    ``metadata["input_fingerprint"]``, created at or after ``not_before``.
    """

    _FP = "f" * 16

    def _batch(self, id_, *, key="the-key", fp=None, created_at=1_000_000):
        return SimpleNamespace(
            id=id_,
            metadata={"idempotency_key": key, "input_fingerprint": fp if fp is not None else self._FP},
            created_at=created_at,
        )

    def test_finds_batch_with_matching_metadata(self, adapter, client):
        client.batches.set_listing(
            [
                self._batch("batch_other", key="other-key"),
                self._batch("batch_match", key="the-key"),
            ]
        )
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") == "batch_match"

    def test_returns_none_when_no_batch_matches(self, adapter, client):
        client.batches.set_listing([self._batch("batch_other", key="x")])
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") is None

    def test_returns_none_when_metadata_is_missing(self, adapter, client):
        client.batches.set_listing([SimpleNamespace(id="batch_other", metadata=None, created_at=0)])
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") is None

    def test_fingerprint_mismatch_is_not_recovered(self, adapter, client):
        """
        N1: a batch matching the idempotency_key alone (stable across a ``clear`` even with
        different content) must NOT be recovered if its recorded input_fingerprint differs from
        the fingerprint of the content actually being submitted now -- otherwise a retry after
        editing the prompts could silently re-attach to, and return, an older, unrelated batch's
        results as if they belonged to the current input.
        """
        client.batches.set_listing([self._batch("batch_stale", key="the-key", fp="stale-fingerprint")])
        assert adapter.find_orphaned_batch("the-key", self._FP, "1970-01-01T00:00:00+00:00") is None

    def test_candidate_created_well_before_not_before_is_rejected(self, adapter, client):
        """N1: a candidate created well before the Phase A intent's own write time -- outside
        even the R3-1 clock-skew slack -- must not be recovered; it cannot be the batch this
        orphaned submit attempt produced."""
        not_before = "1970-01-12T13:46:50+00:00"  # epoch 1_000_010.0
        # 710s before not_before -- comfortably past the 300s slack, so still a real rejection.
        old_epoch = 999_000
        client.batches.set_listing([self._batch("batch_too_old", created_at=old_epoch)])
        assert adapter.find_orphaned_batch("the-key", self._FP, not_before) is None

    def test_candidate_created_within_clock_skew_slack_is_still_recovered(self, adapter, client):
        """
        R3-1: OpenAI's ``Batch.created_at`` is whole seconds while ``not_before`` (the intent
        record's write time) carries microseconds -- a batch genuinely created a fraction of a
        second before the intent write (because the real create call landed before the intent
        record's timestamp was captured, or just floating-point/truncation slop) must still be
        recovered, not silently excluded. Without the clock-skew slack, this candidate --
        created less than one second before ``not_before`` -- would be wrongly rejected.
        """
        not_before = "1970-01-12T13:46:50.500000+00:00"  # epoch 1_000_010.5
        created_at = 1_000_010  # 0.5s before not_before, due to whole-second truncation
        client.batches.set_listing([self._batch("batch_same_second", created_at=created_at)])
        assert adapter.find_orphaned_batch("the-key", self._FP, not_before) == "batch_same_second"

    def test_candidate_created_120s_before_not_before_is_still_recovered(self, adapter, client):
        """
        Pins ``_CLOCK_SKEW_SLACK_SECONDS`` itself (as opposed to ``math.floor``'s truncation
        handling, covered above): 120s is far outside what whole-second truncation could ever
        account for (at most ~1s), but comfortably inside the 300s slack -- so only the slack
        subtraction, not the floor, can make this candidate pass.
        """
        not_before = "1970-01-12T13:46:50+00:00"  # epoch 1_000_010.0, no fractional part at all
        created_at = 1_000_010 - 120
        client.batches.set_listing([self._batch("batch_120s_early", created_at=created_at)])
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

    def test_cancel_batch_calls_sdk_cancel(self, adapter, client):
        adapter.cancel_batch("batch_123")
        assert client.batches.cancelled == ["batch_123"]

    def test_expired_batch_attributes_remainder_to_expired_not_zero(self, adapter, client):
        """N2: OpenAI has no per-item expired signal -- the remainder (total - completed -
        failed) must be attributed to 'expired' when the job itself expired, not silently 0."""
        client.batches._batch = SimpleNamespace(
            id="batch_123",
            status="expired",
            output_file_id="file-out",
            error_file_id="file-err",
            request_counts=SimpleNamespace(completed=2, failed=1, total=10),
            errors=None,
        )
        state = adapter.get_batch("batch_123")
        assert state.counts == {"succeeded": 2, "errored": 1, "expired": 7, "cancelled": 0}

    def test_cancelled_batch_attributes_remainder_to_cancelled_not_zero(self, adapter, client):
        client.batches._batch = SimpleNamespace(
            id="batch_123",
            status="cancelled",
            output_file_id="file-out",
            error_file_id="file-err",
            request_counts=SimpleNamespace(completed=3, failed=0, total=5),
            errors=None,
        )
        state = adapter.get_batch("batch_123")
        assert state.counts == {"succeeded": 3, "errored": 0, "expired": 0, "cancelled": 2}


class TestIterResultsStreamsOutputThenErrorFile:
    def test_success_and_error_files_both_yielded(self, adapter, client, str_spec):
        client.files.set_content(
            "file-out",
            [
                {
                    "custom_id": "k" * 16 + "-0",
                    "response": {
                        "status_code": 200,
                        "body": {
                            "model": "gpt-5",
                            "choices": [{"message": {"content": "hello"}, "finish_reason": "stop"}],
                            "usage": {"prompt_tokens": 3, "completion_tokens": 1},
                        },
                    },
                }
            ],
        )
        client.files.set_content(
            "file-err",
            [{"custom_id": "k" * 16 + "-1", "error": {"message": "rate limited"}, "response": None}],
        )
        items = list(adapter.iter_results("batch_123"))
        assert [item.index for item in items] == [0, 1]
        assert items[0].provider_status == "success"
        assert items[0].raw == "hello"
        assert items[1].provider_status == "errored"
        assert items[1].error["message"] == "rate limited"

    def test_extract_output_unstructured_is_plain_text(self, adapter, str_spec):
        from airflow.providers.common.ai.batch.base import RawResultItem

        raw = RawResultItem(
            custom_id="k-0",
            index=0,
            provider_status="success",
            model="gpt-5",
            usage=None,
            finish_reason="stop",
            error=None,
            raw="hello",
        )
        extracted = adapter.extract_output(raw, str_spec)
        assert extracted.kind == "text"
        assert extracted.text == "hello"
