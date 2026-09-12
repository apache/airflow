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
import re
from collections.abc import Iterator
from typing import Any

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.batch.base import BatchAdapter, ExtractedOutput, RawResultItem
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.batch.results import (
    MergeDiagnostics,
    assemble_manifest,
    missing_indexes,
    stream_results_to_jsonl,
)
from airflow.sdk import ObjectStoragePath


class _FakeAdapter(BatchAdapter):
    """Streams a fixed, pre-baked sequence of ``RawResultItem`` -- no network, no SDK."""

    name = "fake"
    max_requests = 100_000
    max_payload_bytes = 100_000_000
    allows_per_request_model = False

    def __init__(self, items: list[RawResultItem]):
        self._items = items

    def validate_requests(self, requests, *, model, output_spec, idempotency_key) -> None:
        pass

    def submit(self, requests, *, model, idempotency_key, input_fingerprint, output_spec, **kwargs):
        raise NotImplementedError

    def get_batch(self, batch_id):
        raise NotImplementedError

    def cancel_batch(self, batch_id) -> None:
        pass

    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        return iter(self._items)

    def build_output_directive(self, spec) -> dict[str, Any]:
        return {}

    def extract_output(self, raw: RawResultItem, spec) -> ExtractedOutput:
        # Unstructured str output_type in these tests: the "provider response" is just text.
        return ExtractedOutput(kind="text", text=raw.raw)

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        return None


def _success_item(index: int, text: str) -> RawResultItem:
    return RawResultItem(
        custom_id=f"abc1234567890123-{index}",
        index=index,
        provider_status="success",
        model="gpt-5",
        usage={"input_tokens": 10, "output_tokens": 5},
        finish_reason="stop",
        error=None,
        raw=text,
    )


def _errored_item(index: int) -> RawResultItem:
    return RawResultItem(
        custom_id=f"abc1234567890123-{index}",
        index=index,
        provider_status="errored",
        model="gpt-5",
        usage=None,
        finish_reason=None,
        error={"type": "rate_limit", "message": "rate limited", "provider_code": "429", "stage": "provider"},
        raw=None,
    )


def _expired_item(index: int) -> RawResultItem:
    """N2: a provider (Anthropic) reporting per-item expiry -- not the same as 'errored'."""
    return RawResultItem(
        custom_id=f"abc1234567890123-{index}",
        index=index,
        provider_status="expired",
        model="gpt-5",
        usage=None,
        finish_reason=None,
        error={
            "type": "expired",
            "message": "batch item expired",
            "provider_code": None,
            "stage": "provider",
        },
        raw=None,
    )


def _cancelled_item(index: int) -> RawResultItem:
    """N2: a provider (Anthropic) reporting per-item cancellation -- not the same as 'errored'."""
    return RawResultItem(
        custom_id=f"abc1234567890123-{index}",
        index=index,
        provider_status="cancelled",
        model="gpt-5",
        usage=None,
        finish_reason=None,
        error={
            "type": "cancelled",
            "message": "batch item cancelled",
            "provider_code": None,
            "stage": "provider",
        },
        raw=None,
    )


@pytest.fixture
def destination(tmp_path):
    return ObjectStoragePath(f"file://{tmp_path.as_posix()}") / "abc1234567890123.jsonl"


@pytest.fixture
def str_output_spec():
    return build_output_spec(str)


def _read_jsonl(path: ObjectStoragePath) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


class TestStreamResultsToJsonlMissingRows:
    def test_missing_index_gets_exactly_one_missing_row(self, destination, str_output_spec):
        """§6 acceptance (a): the output stream skips index 7 -- it must land as exactly one 'missing' row."""
        items = [_success_item(i, f"text-{i}") for i in range(10) if i != 7]
        adapter = _FakeAdapter(items)

        counts, diagnostics = stream_results_to_jsonl(
            adapter=adapter,
            batch_id="batch_1",
            output_spec=str_output_spec,
            request_count=10,
            custom_id_prefix="abc1234567890123",
            destination=destination,
        )

        rows = _read_jsonl(destination)
        missing_rows = [row for row in rows if row["index"] == 7]
        assert len(missing_rows) == 1
        assert missing_rows[0]["status"] == "missing"
        assert counts["missing"] == 1
        assert diagnostics == MergeDiagnostics()


class TestStreamResultsToJsonlAnomalies:
    """M7: a duplicate or out-of-range index must never inflate ``counts`` past ``request_count``."""

    def test_duplicate_index_is_written_once_and_counted_once(self, destination, str_output_spec):
        items = [_success_item(0, "first"), _success_item(0, "duplicate-should-be-dropped")]
        adapter = _FakeAdapter(items)

        counts, diagnostics = stream_results_to_jsonl(
            adapter=adapter,
            batch_id="batch_1",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="abc1234567890123",
            destination=destination,
        )

        rows = _read_jsonl(destination)
        assert len(rows) == 1
        assert rows[0]["output"] == "first"
        assert counts["success"] == 1
        assert sum(counts.values()) == 1
        assert diagnostics.duplicate_result_count == 1

    def test_out_of_range_index_is_dropped_and_counted_separately(self, destination, str_output_spec):
        """An index the adapter yields that is >= request_count cannot be rejoined to any input."""
        items = [_success_item(0, "in range"), _success_item(99, "corrupt/foreign index")]
        adapter = _FakeAdapter(items)

        counts, diagnostics = stream_results_to_jsonl(
            adapter=adapter,
            batch_id="batch_1",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="abc1234567890123",
            destination=destination,
        )

        rows = _read_jsonl(destination)
        assert len(rows) == 1
        assert rows[0]["index"] == 0
        assert sum(counts.values()) == 1
        assert diagnostics.out_of_range_result_count == 1

    def test_anomalies_never_prevent_the_manifest_from_being_produced(self, destination, str_output_spec):
        """The whole point of M7: dirty data must not make assemble_manifest raise on every retry."""
        items = [
            _success_item(0, "ok"),
            _success_item(0, "dup"),  # duplicate
            _success_item(5, "out of range"),  # out of range for request_count=1
        ]
        adapter = _FakeAdapter(items)
        counts, diagnostics = stream_results_to_jsonl(
            adapter=adapter,
            batch_id="batch_1",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="abc1234567890123",
            destination=destination,
        )
        manifest = assemble_manifest(
            batch_id="batch_1",
            adapter_name="fake",
            llm_conn_id="conn",
            model_id="fake:model",
            output_spec=str_output_spec,
            result_uri=str(destination),
            request_count=1,
            merge_counts=counts,
            merge_diagnostics=diagnostics,
            custom_id_prefix="abc1234567890123",
            submitted_at="t0",
            completed_at="t1",
        )
        assert manifest["request_count"] == sum(manifest["counts"].values())
        assert manifest["duplicate_result_count"] == 1
        assert manifest["out_of_range_result_count"] == 1


class TestStreamResultsToJsonlReconciliation:
    def test_four_status_mix_reconciles_against_request_count(self, destination):
        """§7 acceptance (b): request_count == sum(counts.values()) even with all four statuses present."""

        # Use a structured spec so one item can fail output validation (invalid_output).
        class DiagnosisModel(BaseModel):
            age: int

        spec = build_output_spec(DiagnosisModel)

        class _StructuredAdapter(_FakeAdapter):
            def extract_output(self, raw: RawResultItem, spec) -> ExtractedOutput:
                return ExtractedOutput(kind="json_text", text=raw.raw)

        items = [
            _success_item(0, '{"age": 30}'),
            _errored_item(1),
            _success_item(2, '{"age": "not-a-number"}'),  # fails schema -> invalid_output
            # index 3 is missing
        ]
        adapter = _StructuredAdapter(items)

        counts, _ = stream_results_to_jsonl(
            adapter=adapter,
            batch_id="batch_1",
            output_spec=spec,
            request_count=4,
            custom_id_prefix="abc1234567890123",
            destination=destination,
        )

        assert counts == {
            "success": 1,
            "error": 1,
            "invalid_output": 1,
            "expired": 0,
            "cancelled": 0,
            "missing": 1,
        }
        assert sum(counts.values()) == 4

        manifest = assemble_manifest(
            batch_id="batch_1",
            adapter_name="fake",
            llm_conn_id="conn",
            model_id="fake:model",
            output_spec=spec,
            result_uri=str(destination),
            request_count=4,
            merge_counts=counts,
            custom_id_prefix="abc1234567890123",
            submitted_at="2026-09-11T00:00:00+00:00",
            completed_at="2026-09-11T01:00:00+00:00",
        )
        assert manifest["request_count"] == sum(manifest["counts"].values())
        assert manifest["counts"]["invalid_output"] == 1


class TestStreamResultsToJsonlRowContent:
    def test_provider_error_row_has_no_output(self, destination, str_output_spec):
        adapter = _FakeAdapter([_errored_item(0)])
        stream_results_to_jsonl(
            adapter=adapter,
            batch_id="b",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="p",
            destination=destination,
        )
        row = _read_jsonl(destination)[0]
        assert row["status"] == "error"
        assert row["output"] is None
        assert row["error"]["stage"] == "provider"

    def test_success_row_carries_usage_and_finish_reason(self, destination, str_output_spec):
        adapter = _FakeAdapter([_success_item(0, "hello")])
        stream_results_to_jsonl(
            adapter=adapter,
            batch_id="b",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="p",
            destination=destination,
        )
        row = _read_jsonl(destination)[0]
        assert row["status"] == "success"
        assert row["output"] == "hello"
        assert row["usage"] == {"input_tokens": 10, "output_tokens": 5}
        assert row["finish_reason"] == "stop"

    def test_expired_item_gets_its_own_row_status_not_error(self, destination, str_output_spec):
        """N2: a per-item 'expired' result must not be folded into 'error'."""
        adapter = _FakeAdapter([_expired_item(0)])
        counts, _ = stream_results_to_jsonl(
            adapter=adapter,
            batch_id="b",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="p",
            destination=destination,
        )
        row = _read_jsonl(destination)[0]
        assert row["status"] == "expired"
        assert row["output"] is None
        assert row["error"]["type"] == "expired"
        assert counts["expired"] == 1
        assert counts["error"] == 0
        assert counts["missing"] == 0

    def test_cancelled_item_gets_its_own_row_status_not_error(self, destination, str_output_spec):
        """N2: a per-item 'cancelled' result must not be folded into 'error'."""
        adapter = _FakeAdapter([_cancelled_item(0)])
        counts, _ = stream_results_to_jsonl(
            adapter=adapter,
            batch_id="b",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="p",
            destination=destination,
        )
        row = _read_jsonl(destination)[0]
        assert row["status"] == "cancelled"
        assert row["output"] is None
        assert row["error"]["type"] == "cancelled"
        assert counts["cancelled"] == 1
        assert counts["error"] == 0
        assert counts["missing"] == 0

    @pytest.mark.parametrize(
        "item_factory",
        [_success_item, _errored_item, _expired_item, _cancelled_item],
    )
    def test_every_row_status_has_the_same_keys(self, destination, str_output_spec, item_factory):
        """B9: every row must carry the same key set (in particular ``raw_output``), regardless
        of status -- a downstream reader must not need a status-specific key lookup."""
        item = item_factory(0) if item_factory is not _success_item else item_factory(0, "x")
        adapter = _FakeAdapter([item])
        stream_results_to_jsonl(
            adapter=adapter,
            batch_id="b",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="p",
            destination=destination,
        )
        row = _read_jsonl(destination)[0]
        assert set(row) == {
            "custom_id",
            "index",
            "status",
            "output",
            "raw_output",
            "error",
            "model",
            "usage",
            "finish_reason",
        }


class TestCustomIdCharset:
    """N4/N9: every custom_id this module produces must stay within Anthropic's documented
    ``^[a-zA-Z0-9_-]{1,64}$`` -- in particular, never contain ``:`` (the old, wrong separator)."""

    _ALLOWED = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

    def test_missing_row_custom_id_matches_allowed_charset(self, destination, str_output_spec):
        adapter = _FakeAdapter([])  # every index is missing
        stream_results_to_jsonl(
            adapter=adapter,
            batch_id="b",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="abc1234567890123",
            destination=destination,
        )
        row = _read_jsonl(destination)[0]
        assert self._ALLOWED.match(row["custom_id"]), row["custom_id"]

    def test_provider_result_custom_id_matches_allowed_charset(self, destination, str_output_spec):
        adapter = _FakeAdapter([_success_item(0, "hello")])
        stream_results_to_jsonl(
            adapter=adapter,
            batch_id="b",
            output_spec=str_output_spec,
            request_count=1,
            custom_id_prefix="abc1234567890123",
            destination=destination,
        )
        row = _read_jsonl(destination)[0]
        assert self._ALLOWED.match(row["custom_id"]), row["custom_id"]


class TestAssembleManifestInvariant:
    def test_mismatched_counts_raise_value_error(self, str_output_spec):
        with pytest.raises(ValueError, match="do not reconcile"):
            assemble_manifest(
                batch_id="b",
                adapter_name="fake",
                llm_conn_id="conn",
                model_id="fake:model",
                output_spec=str_output_spec,
                result_uri="file:///tmp/x.jsonl",
                request_count=10,
                merge_counts={"success": 1},  # only sums to 1, not 10
                custom_id_prefix="p",
                submitted_at="t0",
                completed_at="t1",
            )

    def test_extra_counts_default_to_zero(self, str_output_spec):
        manifest = assemble_manifest(
            batch_id="b",
            adapter_name="fake",
            llm_conn_id="conn",
            model_id="fake:model",
            output_spec=str_output_spec,
            result_uri="file:///tmp/x.jsonl",
            request_count=1,
            merge_counts={"success": 1},
            custom_id_prefix="p",
            submitted_at="t0",
            completed_at="t1",
        )
        assert manifest["counts"]["expired"] == 0
        assert manifest["counts"]["cancelled"] == 0
        assert manifest["terminal_reason"] == "succeeded"


class TestAssembleManifestExpiredRelabeling:
    """
    M5/M8: an ``expired``/``cancelled`` terminal event still routes through here (never
    discarded), and the un-processed portion already counted once as ``missing`` gets
    re-labeled from ``extra_counts`` -- never added on top, which would double-count and
    break reconciliation.
    """

    def test_expired_extra_counts_relabels_missing_without_inflating_total(self, str_output_spec):
        # 2 succeeded, 3 never processed (all counted generically as "missing" by the merge step).
        merge_counts = {"success": 2, "error": 0, "invalid_output": 0, "missing": 3}
        manifest = assemble_manifest(
            batch_id="b",
            adapter_name="fake",
            llm_conn_id="conn",
            model_id="fake:model",
            output_spec=str_output_spec,
            result_uri="file:///tmp/x.jsonl",
            request_count=5,
            merge_counts=merge_counts,
            extra_counts={"expired": 3, "cancelled": 0},
            custom_id_prefix="p",
            submitted_at="t0",
            completed_at="t1",
        )
        assert manifest["counts"] == {
            "succeeded": 2,
            "errored": 0,
            "invalid_output": 0,
            "expired": 3,
            "cancelled": 0,
            "missing": 0,
        }
        assert manifest["request_count"] == sum(manifest["counts"].values())
        assert manifest["terminal_reason"] == "expired"

    def test_extra_counts_larger_than_missing_does_not_overcount(self, str_output_spec):
        """A provider self-report larger than our own gap must not push the total past request_count."""
        merge_counts = {"success": 5, "error": 0, "invalid_output": 0, "missing": 1}
        manifest = assemble_manifest(
            batch_id="b",
            adapter_name="fake",
            llm_conn_id="conn",
            model_id="fake:model",
            output_spec=str_output_spec,
            result_uri="file:///tmp/x.jsonl",
            request_count=6,
            merge_counts=merge_counts,
            extra_counts={"expired": 999},  # implausible, must be clamped by our own gap
            custom_id_prefix="p",
            submitted_at="t0",
            completed_at="t1",
        )
        assert manifest["request_count"] == sum(manifest["counts"].values())
        assert manifest["counts"]["expired"] == 1
        assert manifest["counts"]["missing"] == 0

    def test_cancelled_takes_the_remainder_after_expired(self, str_output_spec):
        merge_counts = {"success": 0, "error": 0, "invalid_output": 0, "missing": 4}
        manifest = assemble_manifest(
            batch_id="b",
            adapter_name="fake",
            llm_conn_id="conn",
            model_id="fake:model",
            output_spec=str_output_spec,
            result_uri="file:///tmp/x.jsonl",
            request_count=4,
            merge_counts=merge_counts,
            extra_counts={"expired": 1, "cancelled": 3},
            custom_id_prefix="p",
            submitted_at="t0",
            completed_at="t1",
        )
        assert manifest["counts"]["expired"] == 1
        assert manifest["counts"]["cancelled"] == 3
        assert manifest["counts"]["missing"] == 0
        assert manifest["terminal_reason"] == "expired"


class TestMissingIndexes:
    def test_returns_sorted_gaps(self):
        assert missing_indexes(seen=[0, 2, 4], request_count=5) == [1, 3]
