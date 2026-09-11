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
from collections.abc import Iterator
from types import SimpleNamespace
from unittest import mock

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchState,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.exceptions import (
    LLMBatchPartialFailureError,
    LLMBatchStaleStateError,
)
from airflow.providers.common.ai.operators.llm_batch import LLMBatchOperator
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.sdk import ObjectStoragePath


class Diagnosis(BaseModel):
    age: int


class _FakeAdapter(BatchAdapter):
    """Records calls and serves canned results -- no network, no SDK."""

    name = "openai"
    max_requests = 100_000
    max_payload_bytes = 100_000_000
    allows_per_request_model = False

    def __init__(self):
        self.submit_calls: list[dict] = []
        self.cancel_calls: list[str] = []
        self.get_batch_status = "in_progress"
        self.get_batch_counts: dict[str, int] | None = None
        self.results: list[RawResultItem] = []
        self.orphan_recovery_batch_id: str | None = None
        # N1: when set, find_orphaned_batch only "recovers" a call whose input_fingerprint
        # matches this value -- None means "match anything" (most tests don't care).
        self.orphan_recovery_fingerprint: str | None = None
        self.find_orphaned_batch_calls: list[dict] = []
        self._next_id = 1

    def validate_requests(self, requests, *, model, output_spec, **kwargs):
        pass

    def submit(self, requests, *, model, idempotency_key, input_fingerprint, output_spec, **kwargs):
        batch_id = f"batch_{self._next_id}"
        self._next_id += 1
        self.submit_calls.append(
            {
                "requests": list(requests),
                "idempotency_key": idempotency_key,
                "input_fingerprint": input_fingerprint,
            }
        )
        return SubmitResult(batch_id=batch_id, provider_input_ref=None)

    def get_batch(self, batch_id):
        return BatchState(status=self.get_batch_status, counts=self.get_batch_counts, error_message=None)

    def cancel_batch(self, batch_id):
        self.cancel_calls.append(batch_id)

    def iter_results(self, batch_id) -> Iterator[RawResultItem]:
        return iter(self.results)

    def build_output_directive(self, spec):
        return {}

    def extract_output(self, raw, spec):
        if not spec.is_structured:
            return ExtractedOutput(kind="text", text=raw.raw)
        return ExtractedOutput(kind="json_text", text=raw.raw)

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        self.find_orphaned_batch_calls.append(
            {
                "idempotency_key": idempotency_key,
                "input_fingerprint": input_fingerprint,
                "not_before": not_before,
            }
        )
        if (
            self.orphan_recovery_fingerprint is not None
            and input_fingerprint != self.orphan_recovery_fingerprint
        ):
            return None
        return self.orphan_recovery_batch_id


@pytest.fixture
def fake_adapter():
    return _FakeAdapter()


@pytest.fixture
def result_path(tmp_path):
    return f"file://{tmp_path.as_posix()}"


def _context(*, dag_id="dag", task_id="t", run_id="run_1", map_index=-1):
    ti = SimpleNamespace(
        dag_id=dag_id, task_id=task_id, run_id=run_id, map_index=map_index, xcom_push=lambda **kw: None
    )
    return {"task_instance": ti, "ti": ti}


def _patched(fake_adapter):
    """Patch connection lookup (real conn_type -> real dispatch decision) and adapter construction."""
    return (
        mock.patch(
            "airflow.providers.common.ai.operators.llm_batch.BaseHook.get_connection",
            return_value=SimpleNamespace(conn_type="pydanticai", password=None, host=None),
        ),
        mock.patch("airflow.providers.common.ai.batch.dispatch.build_adapter", return_value=fake_adapter),
    )


def _make_operator(
    task_id="batch_task", requests=None, result_path=None, deferrable=False, llm_conn_id="my_openai", **kwargs
):
    return LLMBatchOperator(
        task_id=task_id,
        requests=requests or ["hello"],
        result_path=result_path,
        llm_conn_id=llm_conn_id,
        model_id="openai:gpt-5",
        deferrable=deferrable,
        **kwargs,
    )


class TestReattachDoesNotResubmit:
    """Step 9 acceptance (a): retry with the state file intact must not call adapter.submit again."""

    def test_second_execute_with_same_identity_and_input_skips_submit(self, fake_adapter, result_path):
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op1 = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            # A fresh operator instance, same task/run/map identity, same input: a retry.
            op2 = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op2.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1  # still 1 -- no resubmit


class TestFingerprintMismatchTriggersCancelAndResubmit:
    """Step 9 acceptance (b)."""

    def test_changed_prompt_cancels_old_batch_and_submits_new_one(self, fake_adapter, result_path):
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op1 = _make_operator(requests=["hello"], result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)
            first_batch_id = fake_adapter.submit_calls[0]["idempotency_key"]  # noqa: F841 (documents intent)
            assert len(fake_adapter.submit_calls) == 1

            op2 = _make_operator(requests=["goodbye"], result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op2.execute(ctx)
            assert len(fake_adapter.submit_calls) == 2
            assert fake_adapter.cancel_calls == ["batch_1"]

    def test_on_stale_state_fail_raises_instead_of_resubmitting(self, fake_adapter, result_path):
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op1 = _make_operator(requests=["hello"], result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)

            op2 = _make_operator(
                requests=["goodbye"], result_path=result_path, deferrable=True, on_stale_state="fail"
            )
            with pytest.raises(LLMBatchStaleStateError):
                op2.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1  # rejected, not resubmitted


class TestOutputTypeChangeDoesNotReattach:
    """Step 9 acceptance (c): prompt unchanged, output_type schema changed -> must not reattach."""

    def test_output_type_change_with_identical_prompt_forces_resubmit(self, fake_adapter, result_path):
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op1 = _make_operator(
                requests=["hello"], result_path=result_path, deferrable=True, output_type=str
            )
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            op2 = _make_operator(
                requests=["hello"], result_path=result_path, deferrable=True, output_type=Diagnosis
            )
            with pytest.raises(TaskDeferred):
                op2.execute(ctx)
            assert len(fake_adapter.submit_calls) == 2


class TestEndToEndStructuredBatch:
    """Step 9 acceptance (d)."""

    def test_success_and_invalid_output_rows_land_and_counts_reconcile(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "completed"
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(
                requests=["a", "b"], result_path=result_path, deferrable=False, output_type=Diagnosis
            )
            # submit() runs before results exist -- register them once we know the custom_id prefix.
            manifest = None

            def _submit_then_seed(
                requests, *, model, idempotency_key, input_fingerprint, output_spec, **kwargs
            ):
                fake_adapter.results = [
                    RawResultItem(
                        custom_id=f"{idempotency_key}-0",
                        index=0,
                        provider_status="success",
                        model="gpt-5",
                        usage={"input_tokens": 1, "output_tokens": 1},
                        finish_reason="stop",
                        error=None,
                        raw='{"age": 30}',
                    ),
                    RawResultItem(
                        custom_id=f"{idempotency_key}-1",
                        index=1,
                        provider_status="success",
                        model="gpt-5",
                        usage={"input_tokens": 1, "output_tokens": 1},
                        finish_reason="stop",
                        error=None,
                        raw='{"age": "not-a-number"}',
                    ),
                ]
                return SubmitResult(batch_id="batch_x", provider_input_ref=None)

            fake_adapter.submit = _submit_then_seed
            manifest = op.execute(ctx)

        assert manifest["request_count"] == 2
        assert manifest["counts"]["succeeded"] == 1
        assert manifest["counts"]["invalid_output"] == 1
        assert manifest["request_count"] == sum(manifest["counts"].values())

        result_uri = ObjectStoragePath(manifest["result_uri"])
        rows = [json.loads(line) for line in result_uri.read_text().splitlines()]
        statuses = {row["status"] for row in rows}
        assert statuses == {"success", "invalid_output"}

        # The state file must be gone once the manifest has landed (§5.3/§8 invariant).
        state_dir = ObjectStoragePath(result_path) / "_airflow_batch_state"
        assert not list(state_dir.iterdir())

    def test_fail_on_partial_error_raises_after_landing_results(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "completed"
        fake_adapter.results = [
            RawResultItem(
                custom_id="k" * 16 + "-0",
                index=0,
                provider_status="errored",
                model=None,
                usage=None,
                finish_reason=None,
                error={
                    "type": "rate_limit",
                    "message": "rate limited",
                    "provider_code": "429",
                    "stage": "provider",
                },
                raw=None,
            )
        ]
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(
                requests=["a"], result_path=result_path, deferrable=False, fail_on_partial_error=True
            )
            with pytest.raises(LLMBatchPartialFailureError):
                op.execute(ctx)

        # Results must have landed even though the task raises.
        jsonl_files = list(ObjectStoragePath(result_path).glob("*.jsonl"))
        assert len(jsonl_files) == 1

        # M1: state must NOT be deleted -- a retry needs to re-attach to this same batch and
        # reach the same conclusion, not pay for a brand new submission.
        state_dir = ObjectStoragePath(result_path) / "_airflow_batch_state"
        assert list(state_dir.iterdir()), "state file was deleted despite fail_on_partial_error raising"

        # And the retry must actually do that: re-attach (no new submit call) and raise the
        # same conclusion again, rather than resubmitting.
        submit_calls_before_retry = len(fake_adapter.submit_calls)
        with conn_patch, adapter_patch:
            op_retry = _make_operator(
                requests=["a"], result_path=result_path, deferrable=False, fail_on_partial_error=True
            )
            with pytest.raises(LLMBatchPartialFailureError):
                op_retry.execute(_context())
        assert len(fake_adapter.submit_calls) == submit_calls_before_retry  # no new submit -- reattached

    def test_fail_on_partial_error_triggers_on_invalid_output_alone(self, fake_adapter, result_path):
        """S2: invalid_output>0 with errored=0 must trigger fail_on_partial_error on its own."""
        fake_adapter.get_batch_status = "completed"
        fake_adapter.results = [
            RawResultItem(
                custom_id="k" * 16 + "-0",
                index=0,
                provider_status="success",
                model="gpt-5",
                usage={"input_tokens": 1, "output_tokens": 1},
                finish_reason="stop",
                error=None,
                raw='{"age": "not-a-number"}',
            )
        ]
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(
                requests=["a"],
                result_path=result_path,
                deferrable=False,
                fail_on_partial_error=True,
                output_type=Diagnosis,
            )
            with pytest.raises(LLMBatchPartialFailureError, match="1 output-validation failure"):
                op.execute(ctx)


class TestStateReadFailurePropagates:
    """M2: a corrupt/unreadable state file must fail the task (retryable), never read as 'no state'."""

    def test_corrupt_state_file_raises_instead_of_resubmitting(self, fake_adapter, result_path):
        from airflow.providers.common.ai.exceptions import LLMBatchStateReadError

        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op1 = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            # Corrupt the state file a previous attempt wrote.
            state_dir = ObjectStoragePath(result_path) / "_airflow_batch_state"
            state_file = next(iter(state_dir.iterdir()))
            state_file.write_text("{not valid json")

            op2 = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(LLMBatchStateReadError):
                op2.execute(ctx)
        assert len(fake_adapter.submit_calls) == 1  # must not have resubmitted


def _expected_fingerprint(requests, *, llm_conn_id="my_openai", model_id="openai:gpt-5", output_type=str):
    """Compute the same fingerprint ``LLMBatchOperator.execute()`` would, for tests that need to
    seed a Phase A record with a fingerprint that will (or, deliberately, will not) match."""
    from airflow.providers.common.ai.batch import state as state_module
    from airflow.providers.common.ai.batch.output_schema import build_output_spec

    normalized = [{"prompt": r} if isinstance(r, str) else r for r in requests]
    spec = build_output_spec(output_type)
    output_schema = spec.json_schema if spec.is_structured else "str"
    return state_module.compute_fingerprint(
        requests=normalized,
        llm_conn_id=llm_conn_id,
        model_id=model_id,
        system_prompt="",
        max_tokens=1024,
        request_params=None,
        output_schema=output_schema,
    )


class TestOrphanRecovery:
    """M3: a Phase A orphan (no batch id recorded) tries provider-side recovery before resubmitting."""

    def test_recovered_orphan_reattaches_without_resubmitting(self, fake_adapter, result_path):
        from airflow.providers.common.ai.batch import state as state_module

        result_path_osp = ObjectStoragePath(result_path)
        ctx = _context()
        key, _ = LLMBatchOperator._identity(ctx)
        fingerprint = _expected_fingerprint(["hello"])
        # Simulate a Phase A intent record left behind by a crash between submit and recording
        # the response -- batch_id is null. N1: the fingerprint here matches what execute() will
        # actually compute for these requests -- this test is the "recovery succeeds when content
        # is unchanged" case, not the mismatched one (see TestOrphanFingerprintMismatch below).
        state_module.write_intent(
            result_path_osp,
            key=key,
            input_fingerprint=fingerprint,
            output_schema_digest="x",
            intent_at="2020-01-01T00:00:00+00:00",
        )
        fake_adapter.orphan_recovery_batch_id = "recovered_batch_123"
        fake_adapter.orphan_recovery_fingerprint = fingerprint
        fake_adapter.get_batch_status = "in_progress"

        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred) as exc_info:
                op.execute(ctx)

        assert len(fake_adapter.submit_calls) == 0  # recovered, never resubmitted
        assert exc_info.value.trigger.batch_id == "recovered_batch_123"
        # N1: the operator must pass the *current* fingerprint through, not skip the check.
        assert fake_adapter.find_orphaned_batch_calls[0]["input_fingerprint"] == fingerprint

    def test_recovered_orphan_anchors_end_time_to_intent_at_not_now(self, fake_adapter, result_path):
        """
        R3-3: a recovered orphan's Phase A record never has ``submitted_at`` set (only Phase B
        sets it) -- falling back to "now" would anchor the C5 timeout budget to recovery time
        instead of the batch's real (approximate) submit time. ``intent_at`` is the best
        available stand-in.
        """
        from datetime import datetime

        from airflow.providers.common.ai.batch import state as state_module

        result_path_osp = ObjectStoragePath(result_path)
        ctx = _context()
        key, _ = LLMBatchOperator._identity(ctx)
        fingerprint = _expected_fingerprint(["hello"])
        intent_at = "2020-01-01T00:00:00+00:00"
        state_module.write_intent(
            result_path_osp,
            key=key,
            input_fingerprint=fingerprint,
            output_schema_digest="x",
            intent_at=intent_at,
        )
        fake_adapter.orphan_recovery_batch_id = "recovered_batch_123"
        fake_adapter.orphan_recovery_fingerprint = fingerprint
        fake_adapter.get_batch_status = "in_progress"

        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(result_path=result_path, deferrable=True, timeout=1000)
            with pytest.raises(TaskDeferred) as exc_info:
                op.execute(ctx)

        expected_end_time = datetime.fromisoformat(intent_at).timestamp() + 1000
        assert exc_info.value.trigger.end_time == pytest.approx(expected_end_time, abs=2)

    def test_no_orphan_found_submits_a_new_batch(self, fake_adapter, result_path):
        from airflow.providers.common.ai.batch import state as state_module

        result_path_osp = ObjectStoragePath(result_path)
        ctx = _context()
        key, _ = LLMBatchOperator._identity(ctx)
        state_module.write_intent(
            result_path_osp,
            key=key,
            input_fingerprint=_expected_fingerprint(["hello"]),
            output_schema_digest="x",
            intent_at="2020-01-01T00:00:00+00:00",
        )
        fake_adapter.orphan_recovery_batch_id = None  # nothing found

        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op.execute(ctx)
        assert len(fake_adapter.submit_calls) == 1


class TestOrphanFingerprintMismatch:
    """
    N1: a candidate batch on the provider side must not be recovered just because the
    idempotency_key matches -- the key is stable across a ``clear`` even when the prompts
    changed, so matching on it alone risks silently attaching a different, older submission's
    results. Only a matching input_fingerprint makes recovery safe.
    """

    def test_fingerprint_mismatch_falls_back_to_a_fresh_submit(self, fake_adapter, result_path):
        from airflow.providers.common.ai.batch import state as state_module

        result_path_osp = ObjectStoragePath(result_path)
        ctx = _context()
        key, _ = LLMBatchOperator._identity(ctx)
        state_module.write_intent(
            result_path_osp,
            key=key,
            input_fingerprint="whatever-was-recorded-before",
            output_schema_digest="x",
            intent_at="2020-01-01T00:00:00+00:00",
        )
        fake_adapter.orphan_recovery_batch_id = "recovered_batch_123"
        # Simulates: the batch actually on the provider (if any) was submitted under different
        # content than what this attempt is about to submit -- the real adapter's own metadata
        # match would reject it; here the fake enforces that same contract explicitly.
        fake_adapter.orphan_recovery_fingerprint = "some-completely-different-fingerprint"

        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op.execute(ctx)

        # Not recovered -- a fresh batch was submitted instead of silently attaching to a
        # mismatched candidate.
        assert len(fake_adapter.submit_calls) == 1
        assert fake_adapter.submit_calls[0]["input_fingerprint"] == _expected_fingerprint(["hello"])


class TestOnOrphanedIntent:
    """R3-6: when no orphan is found, on_orphaned_intent controls resubmit vs. fail."""

    def _write_unrecoverable_intent(self, result_path):
        from airflow.providers.common.ai.batch import state as state_module

        ctx = _context()
        key, _ = LLMBatchOperator._identity(ctx)
        state_module.write_intent(
            ObjectStoragePath(result_path),
            key=key,
            input_fingerprint=_expected_fingerprint(["hello"]),
            output_schema_digest="x",
            intent_at="2020-01-01T00:00:00+00:00",
        )
        return ctx

    def test_default_resubmit_submits_a_new_batch(self, fake_adapter, result_path):
        ctx = self._write_unrecoverable_intent(result_path)
        fake_adapter.orphan_recovery_batch_id = None  # nothing found

        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(result_path=result_path, deferrable=True)  # default: "resubmit"
            with pytest.raises(TaskDeferred):
                op.execute(ctx)
        assert len(fake_adapter.submit_calls) == 1

    def test_fail_raises_instead_of_resubmitting(self, fake_adapter, result_path):
        from airflow.providers.common.ai.exceptions import LLMBatchOrphanedIntentError

        ctx = self._write_unrecoverable_intent(result_path)
        fake_adapter.orphan_recovery_batch_id = None  # nothing found

        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(result_path=result_path, deferrable=True, on_orphaned_intent="fail")
            with pytest.raises(LLMBatchOrphanedIntentError):
                op.execute(ctx)
        assert len(fake_adapter.submit_calls) == 0  # rejected, not resubmitted


class TestCancelledRoutesThroughFinalizeLikeExpired:
    """
    N-M4: a cancelled batch is handled exactly like an expired one -- fetched/validated/landed
    via ``_finalize``, not treated as an immediate dead end that unconditionally clears state and
    raises. Whether the task then fails is ``fail_on_partial_error``'s job.
    """

    def test_cancelled_status_produces_a_manifest_and_clears_state_like_success(
        self, fake_adapter, result_path
    ):
        fake_adapter.get_batch_status = "cancelled"
        fake_adapter.get_batch_counts = {"succeeded": 1, "errored": 0, "expired": 0, "cancelled": 1}
        fake_adapter.results = [
            RawResultItem(
                custom_id="k-0",
                index=0,
                provider_status="success",
                model="gpt-5",
                usage={"input_tokens": 1, "output_tokens": 1},
                finish_reason="stop",
                error=None,
                raw="hello",
            )
        ]
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(requests=["a", "b"], result_path=result_path, deferrable=False)
            manifest = op.execute(ctx)

        assert manifest["counts"]["succeeded"] == 1
        assert manifest["counts"]["cancelled"] == 1
        assert manifest["terminal_reason"] == "cancelled"
        assert manifest["request_count"] == sum(manifest["counts"].values())
        # Results were actually landed, not discarded.
        result_uri = ObjectStoragePath(manifest["result_uri"])
        assert result_uri.read_text().strip() != ""
        # fail_on_partial_error defaults to False -- state clears like any other finalized batch.
        state_dir = ObjectStoragePath(result_path) / "_airflow_batch_state"
        assert not list(state_dir.iterdir())

    def test_cancelled_status_with_fail_on_partial_error_raises_but_keeps_state(
        self, fake_adapter, result_path
    ):
        fake_adapter.get_batch_status = "cancelled"
        fake_adapter.get_batch_counts = {"succeeded": 0, "errored": 0, "expired": 0, "cancelled": 1}
        fake_adapter.results = []  # the one request never completed
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(
                requests=["a"], result_path=result_path, deferrable=False, fail_on_partial_error=True
            )
            with pytest.raises(LLMBatchPartialFailureError, match="1 cancelled request"):
                op.execute(ctx)

        state_dir = ObjectStoragePath(result_path) / "_airflow_batch_state"
        assert list(state_dir.iterdir()), "state must be kept so a retry re-attaches to the same batch"


class TestFailOnPartialErrorIncludesMissing:
    """
    N3: ``missing`` (no per-item or job-level signal explains it at all) must also trigger
    ``fail_on_partial_error`` -- an expired-at-5%-completion batch must not report bare success
    just because none of the uncompleted requests individually came back "errored".
    """

    def test_missing_alone_triggers_fail_on_partial_error(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "completed"
        fake_adapter.results = []  # both requests are unaccounted for -- pure "missing"
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(
                requests=["a", "b"], result_path=result_path, deferrable=False, fail_on_partial_error=True
            )
            with pytest.raises(LLMBatchPartialFailureError, match=r"2 request\(s\) with no recorded outcome"):
                op.execute(ctx)


class TestPollTimeoutAnchoredToSubmittedAt:
    """
    C5: the poll timeout is "seconds from submission" (per the class/``timeout`` docstring), not
    "seconds from whenever this particular execute() attempt happens to run" -- a resumed
    reattach (a retry, or a still-running batch) must not silently get a fresh full budget.
    """

    def test_reattach_to_a_still_running_batch_anchors_end_time_to_recorded_submitted_at(
        self, fake_adapter, result_path
    ):
        from datetime import datetime

        from airflow.providers.common.ai.batch import state as state_module

        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op1 = _make_operator(result_path=result_path, deferrable=True, timeout=1000)
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)

            key, _ = LLMBatchOperator._identity(ctx)
            record = state_module.read_state(ObjectStoragePath(result_path), key)
            expected_end_time = datetime.fromisoformat(record.submitted_at).timestamp() + 1000

            # Retry, resuming a still-in_progress batch -- "now" is mocked to a value wildly
            # different from the real submitted_at, so a bug that recomputes
            # end_time = time.time() + timeout would produce a wildly different end_time too.
            op2 = _make_operator(result_path=result_path, deferrable=True, timeout=1000)
            with mock.patch("airflow.providers.common.ai.operators.llm_batch.time.time", return_value=0.0):
                with pytest.raises(TaskDeferred) as exc_info:
                    op2.execute(ctx)

        assert exc_info.value.trigger.end_time == pytest.approx(expected_end_time, abs=2)


class TestExpiredRoutesThroughFinalize:
    """M5/M8: an 'expired' terminal status must still fetch/validate/merge/land results, and the
    trigger/poll-reported counts (M8) must reach the manifest, not be discarded."""

    def test_expired_status_produces_a_manifest_with_expired_counts(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "expired"
        fake_adapter.get_batch_counts = {"succeeded": 1, "errored": 0, "expired": 1, "cancelled": 0}
        fake_adapter.results = [
            RawResultItem(
                custom_id="k-0",
                index=0,
                provider_status="success",
                model="gpt-5",
                usage={"input_tokens": 1, "output_tokens": 1},
                finish_reason="stop",
                error=None,
                raw="hello",
            )
        ]
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(requests=["a", "b"], result_path=result_path, deferrable=False)
            manifest = op.execute(ctx)

        assert manifest["counts"]["succeeded"] == 1
        assert manifest["counts"]["expired"] == 1
        assert manifest["terminal_reason"] == "expired"
        assert manifest["request_count"] == sum(manifest["counts"].values())
        # Results were actually landed, not discarded.
        result_uri = ObjectStoragePath(manifest["result_uri"])
        assert result_uri.read_text().strip() != ""


class TestLlmConnIdChangeForcesResubmit:
    """M6: switching llm_conn_id must never silently re-attach to a batch billed to another account."""

    def test_different_llm_conn_id_with_identical_everything_else_forces_resubmit(
        self, fake_adapter, result_path
    ):
        ctx = _context()
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op1 = _make_operator(
                requests=["hello"], result_path=result_path, deferrable=True, llm_conn_id="account_a"
            )
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            op2 = _make_operator(
                requests=["hello"], result_path=result_path, deferrable=True, llm_conn_id="account_b"
            )
            with pytest.raises(TaskDeferred):
                op2.execute(ctx)
            assert len(fake_adapter.submit_calls) == 2


class TestDoXcomPushGuard:
    """S3: the observability-only batch_id XCom push must honor do_xcom_push=False."""

    def test_do_xcom_push_false_skips_the_batch_id_push(self, fake_adapter, result_path):
        pushed = []
        ti = SimpleNamespace(
            dag_id="dag",
            task_id="t",
            run_id="run_1",
            map_index=-1,
            xcom_push=lambda **kw: pushed.append(kw),
        )
        ctx = {"task_instance": ti, "ti": ti}
        conn_patch, adapter_patch = _patched(fake_adapter)
        with conn_patch, adapter_patch:
            op = _make_operator(result_path=result_path, deferrable=True, do_xcom_push=False)
            with pytest.raises(TaskDeferred):
                op.execute(ctx)
        assert pushed == []
