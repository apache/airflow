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

from collections.abc import Iterator
from typing import Any

import pytest

from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchRequest,
    BatchState,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
    evaluate_batch_counts,
)
from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError, LLMBatchModelMismatchError


class _FakeAdapter(BatchAdapter):
    """Minimal concrete subclass, just enough to prove the ABC is implementable."""

    name = "fake"
    max_requests = 10
    allows_per_request_model = False
    max_payload_bytes = 1_000

    def validate_requests(
        self, requests: list[BatchRequest], *, model: str, output_spec: Any, idempotency_key: str
    ) -> None:
        pass

    def submit(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        input_fingerprint: str,
        output_spec: Any,
        **kwargs: Any,
    ) -> SubmitResult:
        return SubmitResult(batch_id="fake-batch", provider_input_ref=None)

    def get_batch(self, batch_id: str) -> BatchState:
        return BatchState(status="completed", counts=None, error_message=None)

    def cancel_batch(self, batch_id: str) -> None:
        pass

    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        return iter(())

    def build_output_directive(self, spec: Any) -> dict[str, Any]:
        return {}

    def extract_output(self, raw: RawResultItem, spec: Any) -> ExtractedOutput:
        return ExtractedOutput(kind="text", text="")

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        return None


class TestBatchRequest:
    def test_prompt_only_shorthand_is_a_plain_dict(self):
        """``list[str]`` is a sugar form of ``list[BatchRequest]``; a bare string maps to just ``prompt``."""
        request: BatchRequest = {"prompt": "hello"}
        assert request["prompt"] == "hello"
        assert "model" not in request

    def test_supports_all_optional_fields(self):
        request: BatchRequest = {
            "prompt": "hello",
            "model": "openai:gpt-5-mini",
            "system_prompt": "be terse",
            "max_tokens": 256,
            "params": {"temperature": 0.2},
        }
        assert request["model"] == "openai:gpt-5-mini"
        assert request["params"] == {"temperature": 0.2}


class TestExtractedOutput:
    def test_text_kind_defaults_value_to_none(self):
        extracted = ExtractedOutput(kind="text", text="hello")
        assert extracted.value is None

    def test_json_value_kind_defaults_text_to_none(self):
        extracted = ExtractedOutput(kind="json_value", value={"a": 1})
        assert extracted.text is None

    def test_absent_kind_needs_no_payload(self):
        extracted = ExtractedOutput(kind="absent")
        assert extracted.text is None
        assert extracted.value is None


class TestBatchAdapterABC:
    def test_cannot_instantiate_directly(self):
        """The ABC must force every adapter to implement the full contract."""
        with pytest.raises(TypeError, match="abstract"):
            BatchAdapter()  # type: ignore[abstract]

    def test_concrete_subclass_is_instantiable(self):
        adapter = _FakeAdapter()
        assert adapter.get_batch("x").status == "completed"
        result = adapter.submit([], model="m", idempotency_key="k", input_fingerprint="fp", output_spec=None)
        assert result.batch_id == "fake-batch"


class TestFindOrphanedBatchIsPartOfTheContract:
    def test_missing_implementation_cannot_instantiate(self):
        """M3: every adapter must decide (even if the decision is 'always None') -- not omit it."""

        class _MissingOrphanRecovery(BatchAdapter):
            name = "incomplete"
            max_requests = 10
            allows_per_request_model = False
            max_payload_bytes = 1_000

            def validate_requests(self, requests, *, model, output_spec, idempotency_key):
                pass

            def submit(self, requests, *, model, idempotency_key, input_fingerprint, output_spec, **kwargs):
                return SubmitResult(batch_id="x", provider_input_ref=None)

            def get_batch(self, batch_id):
                return BatchState(status="completed", counts=None, error_message=None)

            def cancel_batch(self, batch_id):
                pass

            def iter_results(self, batch_id):
                return iter(())

            def build_output_directive(self, spec):
                return {}

            def extract_output(self, raw, spec):
                return ExtractedOutput(kind="text", text="")

        with pytest.raises(TypeError, match="find_orphaned_batch"):
            _MissingOrphanRecovery()


class TestResolveRequestModel:
    """M10: the per-request model cross-adapter check lives once, concretely, on the ABC."""

    def test_none_falls_back_to_the_batch_level_default(self):
        adapter = _FakeAdapter()
        assert adapter.resolve_request_model(None, default_bare_model="m-1", request_index=0) == "m-1"

    def test_matching_prefix_resolves_to_bare_model(self):
        adapter = _FakeAdapter()
        result = adapter.resolve_request_model("fake:m-2", default_bare_model="m-1", request_index=0)
        assert result == "m-2"

    def test_foreign_provider_prefix_is_rejected(self):
        adapter = _FakeAdapter()
        with pytest.raises(LLMBatchModelMismatchError, match="Request 3"):
            adapter.resolve_request_model("other:m-2", default_bare_model="m-1", request_index=3)

    def test_missing_colon_is_rejected(self):
        """A bare model name with no provider prefix is not dispatch-resolvable -- reject it."""
        adapter = _FakeAdapter()
        with pytest.raises(LLMBatchModelMismatchError):
            adapter.resolve_request_model("m-2", default_bare_model="m-1", request_index=0)


class TestCheckCustomIdLength:
    """N7: Anthropic documents custom_id as ``^[a-zA-Z0-9_-]{1,64}$`` -- the 64-char cap must be
    enforced pre-submit, not left to fail (or silently truncate) at the provider."""

    def test_zero_requests_is_a_noop(self):
        _FakeAdapter().check_custom_id_length("k" * 16, 0)

    def test_within_limit_does_not_raise(self):
        # worst case is "<key>-<largest index>"; 16 + 1 + 4 (index 9999) = 21 chars, well under 64.
        _FakeAdapter().check_custom_id_length("k" * 16, 10_000)

    def test_over_limit_is_rejected(self):
        """A worst-case custom_id over max_length must raise before any network call."""
        with pytest.raises(LLMBatchLimitExceededError, match="custom_id"):
            _FakeAdapter().check_custom_id_length("k" * 16, 10_000, max_length=20)

    def test_error_message_reports_the_actual_generated_id_and_length(self):
        with pytest.raises(LLMBatchLimitExceededError, match=r"'k{16}-9', 18 characters"):
            _FakeAdapter().check_custom_id_length("k" * 16, 10, max_length=17)


class TestEvaluateBatchCounts:
    def test_all_succeeded_is_succeeded(self):
        assert evaluate_batch_counts({"succeeded": 10}) == "succeeded"

    def test_any_non_succeeded_bucket_is_partial(self):
        assert evaluate_batch_counts({"succeeded": 8, "errored": 2}) == "partial"

    def test_all_failed_is_partial(self):
        assert evaluate_batch_counts({"succeeded": 0, "errored": 10}) == "partial"

    def test_empty_counts_is_partial(self):
        """An empty breakdown (e.g. a zero-request batch) must not vacuously read as success."""
        assert evaluate_batch_counts({}) == "partial"
