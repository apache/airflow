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

from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.batch.anthropic import AnthropicBatchAdapter
from airflow.providers.common.ai.batch.base import RawResultItem
from airflow.providers.common.ai.batch.output_schema import build_output_spec

# Every test here injects a fake client, so nothing exercises the real SDK import path --
# but this module is what the anthropic extra / dev-group dependency (pyproject.toml)
# exists for, and guarding consistently with the rest of the suite keeps that intent visible.
pytest.importorskip("anthropic")


class _FakeBatches:
    def __init__(self):
        self.created: list[dict] = []
        self.cancelled: list[str] = []
        self._results: list = []
        self._batch = SimpleNamespace(
            id="msgbatch_123",
            processing_status="ended",
            request_counts=SimpleNamespace(succeeded=1, errored=0, canceled=0, expired=0, processing=0),
        )

    def create(self, *, requests):
        self.created.append({"requests": requests})
        return SimpleNamespace(id="msgbatch_123")

    def retrieve(self, batch_id):
        return self._batch

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


class TestSubmitRequestShape:
    def test_custom_id_and_no_batch_level_metadata(self, adapter, client, str_spec):
        """R8: the SDK's ``batches.create`` has no ``metadata`` kwarg -- only ``requests`` may be passed."""
        adapter.submit(
            [{"prompt": "a"}],
            model="claude-sonnet-5",
            idempotency_key="k" * 16,
            input_fingerprint="fp" * 8,
            output_spec=str_spec,
        )
        call = client.messages.batches.created[0]
        assert set(call) == {"requests"}
        assert call["requests"][0]["custom_id"] == "k" * 16 + "-0"

    def test_per_request_model_override_is_allowed(self, adapter, client, str_spec):
        """D3: unlike OpenAI, Anthropic has no batch-level uniform-model requirement."""
        adapter.validate_requests(
            [{"prompt": "a", "model": "anthropic:claude-opus-5"}, {"prompt": "b"}],
            model="claude-sonnet-5",
            idempotency_key="k" * 16,
            output_spec=str_spec,
        )  # must not raise
        adapter.submit(
            [{"prompt": "a", "model": "anthropic:claude-opus-5"}, {"prompt": "b"}],
            model="claude-sonnet-5",
            idempotency_key="k" * 16,
            input_fingerprint="fp" * 8,
            output_spec=str_spec,
        )
        params = [r["params"] for r in client.messages.batches.created[0]["requests"]]
        assert params[0]["model"] == "claude-opus-5"
        assert params[1]["model"] == "claude-sonnet-5"

    def test_foreign_provider_per_request_model_rejected(self, adapter, str_spec):
        """M10: a per-request model naming a different provider must be rejected, not sent through."""
        from airflow.providers.common.ai.exceptions import LLMBatchModelMismatchError

        with pytest.raises(LLMBatchModelMismatchError, match="Request 0"):
            adapter.validate_requests(
                [{"prompt": "a", "model": "openai:gpt-5"}],
                model="claude-sonnet-5",
                idempotency_key="k" * 16,
                output_spec=str_spec,
            )

    def test_tool_choice_forces_the_declared_tool(self, adapter, client):
        spec = build_output_spec(Diagnosis)
        adapter.submit(
            [{"prompt": "a"}],
            model="claude-sonnet-5",
            idempotency_key="k" * 16,
            input_fingerprint="fp" * 8,
            output_spec=spec,
        )
        params = client.messages.batches.created[0]["requests"][0]["params"]
        assert params["tool_choice"] == {"type": "tool", "name": "Diagnosis"}
        assert params["tools"][0]["input_schema"] == spec.json_schema

    def test_system_prompt_is_a_top_level_field_not_a_message(self, adapter, client, str_spec):
        adapter.submit(
            [{"prompt": "a"}],
            model="claude-sonnet-5",
            idempotency_key="k" * 16,
            input_fingerprint="fp" * 8,
            output_spec=str_spec,
            system_prompt="be terse",
        )
        params = client.messages.batches.created[0]["requests"][0]["params"]
        assert params["system"] == "be terse"
        assert params["messages"] == [{"role": "user", "content": "a"}]

    def test_over_length_custom_id_rejected(self, adapter, str_spec):
        """N7: Anthropic documents custom_id as ^[a-zA-Z0-9_-]{1,64}$ -- the worst-case id must
        be checked pre-submit."""
        from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError

        with pytest.raises(LLMBatchLimitExceededError, match="custom_id"):
            adapter.validate_requests(
                [{"prompt": "a"}] * 100_000,
                model="claude-sonnet-5",
                idempotency_key="k" * 60,  # way over any real key16, forces an over-length id
                output_spec=str_spec,
            )


class TestFindOrphanedBatch:
    """M3: Anthropic has no way to recover a Phase A orphan -- must return None, documented why."""

    def test_always_returns_none(self, adapter):
        assert adapter.find_orphaned_batch("any-key", "any-fp", "1970-01-01T00:00:00+00:00") is None


class TestGetBatchAndCancel:
    def test_ended_status_maps_to_completed(self, adapter):
        state = adapter.get_batch("msgbatch_123")
        assert state.status == "completed"
        assert state.counts["succeeded"] == 1

    def test_cancel_batch_calls_sdk_cancel(self, adapter, client):
        adapter.cancel_batch("msgbatch_123")
        assert client.messages.batches.cancelled == ["msgbatch_123"]

    def test_ended_with_cancel_initiated_at_maps_to_cancelled_not_completed(self, adapter, client):
        """
        B4: Anthropic's own ``processing_status`` conflates normal completion and
        completion-after-cancellation into the same ``"ended"`` value -- ``cancel_initiated_at``
        being set is the only signal that distinguishes the two. Without this check, a cancelled
        batch would be reported as ``"completed"`` and would never route through the
        partial-results path the operator's N-M4 fix depends on.
        """
        client.messages.batches._batch = SimpleNamespace(
            id="msgbatch_123",
            processing_status="ended",
            cancel_initiated_at="2026-09-11T00:00:00+00:00",
            request_counts=SimpleNamespace(succeeded=1, errored=0, canceled=4, expired=0, processing=0),
        )
        state = adapter.get_batch("msgbatch_123")
        assert state.status == "cancelled"

    def test_ended_without_cancel_initiated_at_still_maps_to_completed(self, adapter, client):
        """A normal, non-cancelled 'ended' batch (no cancel_initiated_at attribute at all) must
        not be misread as cancelled."""
        client.messages.batches._batch = SimpleNamespace(
            id="msgbatch_123",
            processing_status="ended",
            request_counts=SimpleNamespace(succeeded=1, errored=0, canceled=0, expired=0, processing=0),
        )
        state = adapter.get_batch("msgbatch_123")
        assert state.status == "completed"


class TestExtractOutputNoToolUseBlock:
    def test_absent_when_only_text_block_is_returned(self, adapter):
        """§10.3: forced tool_choice still permits a plain-text response -- must map to kind='absent'."""
        spec = build_output_spec(Diagnosis)
        raw = RawResultItem(
            custom_id="k-0",
            index=0,
            provider_status="success",
            model="claude-sonnet-5",
            usage={"input_tokens": 1, "output_tokens": 1},
            finish_reason="end_turn",
            error=None,
            raw=[SimpleNamespace(type="text", text="I cannot comply.")],
        )
        extracted = adapter.extract_output(raw, spec)
        assert extracted.kind == "absent"
        assert extracted.text == "I cannot comply."

    def test_tool_use_block_yields_json_value(self, adapter):
        spec = build_output_spec(Diagnosis)
        raw = RawResultItem(
            custom_id="k-0",
            index=0,
            provider_status="success",
            model="claude-sonnet-5",
            usage={"input_tokens": 1, "output_tokens": 1},
            finish_reason="tool_use",
            error=None,
            raw=[SimpleNamespace(type="tool_use", name="Diagnosis", input={"age": 30})],
        )
        extracted = adapter.extract_output(raw, spec)
        assert extracted.kind == "json_value"
        assert extracted.value == {"age": 30}


class TestIterResultsParsesSucceededAndErroredItems:
    def test_mixed_results_stream(self, adapter, client):
        succeeded_message = SimpleNamespace(
            model="claude-sonnet-5",
            usage=SimpleNamespace(input_tokens=5, output_tokens=2),
            stop_reason="end_turn",
            content=[SimpleNamespace(type="text", text="hi")],
        )
        client.messages.batches.set_results(
            [
                SimpleNamespace(
                    custom_id="k" * 16 + "-0",
                    result=SimpleNamespace(type="succeeded", message=succeeded_message),
                ),
                SimpleNamespace(
                    custom_id="k" * 16 + "-1",
                    result=SimpleNamespace(type="errored", error=SimpleNamespace(message="overloaded")),
                ),
            ]
        )
        items = list(adapter.iter_results("msgbatch_123"))
        assert items[0].provider_status == "success"
        assert items[0].usage == {"input_tokens": 5, "output_tokens": 2}
        assert items[1].provider_status == "errored"
        assert items[1].error["message"] == "overloaded"

    def test_per_item_expired_and_canceled_are_not_folded_into_errored(self, adapter, client):
        """
        N2: previously untested -- only "provider never responded at all" was covered. Anthropic
        can report *per-item* ``result.type`` of ``"expired"``/``"canceled"`` (note: Anthropic
        spells it with one "l") for individual requests inside an otherwise-terminal batch; both
        must produce their own ``provider_status``, not the generic ``"errored"`` -- folding them
        in made the expired/cancelled relabeling in ``assemble_manifest`` permanently unable to
        ever see nonzero per-item expired/cancelled counts for this adapter.
        """
        client.messages.batches.set_results(
            [
                SimpleNamespace(
                    custom_id="k" * 16 + "-0",
                    result=SimpleNamespace(type="expired", error=None),
                ),
                SimpleNamespace(
                    custom_id="k" * 16 + "-1",
                    result=SimpleNamespace(type="canceled", error=None),
                ),
            ]
        )
        items = list(adapter.iter_results("msgbatch_123"))
        assert items[0].provider_status == "expired"
        assert items[0].error["type"] == "expired"
        assert items[1].provider_status == "cancelled"  # normalized spelling
        assert items[1].error["type"] == "canceled"  # provider's own spelling preserved in error detail
