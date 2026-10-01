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
import time
from collections.abc import Iterator
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.batch import dispatch, state as state_module
from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchState,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.exceptions import (
    LLMBatchCancelledError,
    LLMBatchInputError,
    LLMBatchJobError,
    LLMBatchOrphanedIntentError,
    LLMBatchOrphanLookupError,
    LLMBatchPartialFailureError,
    LLMBatchStaleStateError,
    LLMBatchStateReadError,
    LLMBatchTimeoutError,
)
from airflow.providers.common.ai.operators import llm_batch as llm_batch_module
from airflow.providers.common.ai.operators.llm_batch import LLMBatchOperator
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.sdk import Connection, ObjectStoragePath


class Diagnosis(BaseModel):
    age: int


class _NotFound(Exception):
    status_code = 404


class _FakeAdapter(BatchAdapter):
    """Records calls and serves canned results; no network, no SDK."""

    name = "openai"
    max_requests = 100_000
    max_payload_bytes = 100_000_000
    allows_per_request_model = False

    def __init__(self):
        self.submit_calls: list[dict] = []
        self.cancel_calls: list[str] = []
        self.close_calls = 0
        self.get_batch_status = "in_progress"
        self.get_batch_counts: dict[str, int] | None = None
        self.get_batch_error: Exception | None = None
        self.results: list[RawResultItem] = []
        self.orphan_recovery_batch_id: str | None = None
        self.orphan_recovery_fingerprint: str | None = None
        self.orphan_lookup_error: Exception | None = None
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
                "model": model,
                "idempotency_key": idempotency_key,
                "input_fingerprint": input_fingerprint,
            }
        )
        return SubmitResult(batch_id=batch_id, provider_input_ref=None)

    def get_batch(self, batch_id):
        if self.get_batch_error is not None:
            raise self.get_batch_error
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
        if self.orphan_lookup_error is not None:
            raise self.orphan_lookup_error
        if (
            self.orphan_recovery_fingerprint is not None
            and input_fingerprint != self.orphan_recovery_fingerprint
        ):
            return None
        return self.orphan_recovery_batch_id

    def close(self) -> None:
        self.close_calls += 1


def _success_item(index: int, raw: str, *, prefix: str = "k" * 16) -> RawResultItem:
    return RawResultItem(
        custom_id=f"{prefix}-{index}",
        index=index,
        provider_status="success",
        model="gpt-5",
        usage={"input_tokens": 1, "output_tokens": 1},
        finish_reason="stop",
        error=None,
        raw=raw,
    )


@pytest.fixture
def fake_adapter():
    return _FakeAdapter()


@pytest.fixture
def result_path(tmp_path):
    return f"file://{tmp_path.as_posix()}"


def _context(*, dag_id="dag", task_id="t", run_id="run_1", map_index=-1, pushed: list | None = None):
    ti = SimpleNamespace(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
        xcom_push=lambda **kw: pushed.append(kw) if pushed is not None else None,
    )
    return {"task_instance": ti, "ti": ti}


def _connection(*, conn_type: str = "pydanticai", extra: dict | None = None) -> Connection:
    return Connection(
        conn_id="c", conn_type=conn_type, password=None, host=None, extra=json.dumps(extra) if extra else None
    )


@contextlib.contextmanager
def _running(fake_adapter: _FakeAdapter, *, conn: Connection | None = None):
    """Patch the connection lookup and both adapter constructors so the real dispatch decision runs against the fake."""
    with (
        mock.patch.object(
            llm_batch_module.BaseHook, "get_connection", autospec=True, return_value=conn or _connection()
        ),
        mock.patch.object(
            dispatch, "build_adapter_from_connection", autospec=True, return_value=fake_adapter
        ),
        mock.patch.object(
            dispatch, "build_adapter", autospec=True, return_value=fake_adapter
        ) as build_adapter,
    ):
        yield build_adapter


def _make_operator(
    task_id="batch_task", requests=None, result_path=None, deferrable=False, llm_conn_id="my_openai", **kwargs
):
    kwargs.setdefault("model_id", "openai:gpt-5")
    return LLMBatchOperator(
        task_id=task_id,
        requests=requests or ["hello"],
        result_path=result_path,
        llm_conn_id=llm_conn_id,
        deferrable=deferrable,
        **kwargs,
    )


def _state_files(result_path: str) -> list:
    state_dir = ObjectStoragePath(result_path) / "_airflow_batch_state"
    return list(state_dir.iterdir()) if state_dir.exists() else []


def _expected_fingerprint(requests, *, llm_conn_id="my_openai", model_id="openai:gpt-5", output_type=str):
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


def _write_intent(result_path: str, ctx, *, fingerprint: str, intent_at="2020-01-01T00:00:00+00:00") -> None:
    key, _ = LLMBatchOperator._identity(ctx)
    state_module.write_intent(
        ObjectStoragePath(result_path),
        key=key,
        input_fingerprint=fingerprint,
        output_schema_digest="x",
        intent_at=intent_at,
    )


class TestConstructorValidation:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"poll_interval": 10}, "poll_interval must be at least 30"),
            ({"timeout": 0}, "timeout must be a positive"),
            ({"on_stale_state": "resubmit"}, "on_stale_state must be one of"),
            ({"on_orphaned_intent": "whatever"}, "on_orphaned_intent must be one of"),
        ],
    )
    def test_invalid_arguments_fail_at_parse_time(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            _make_operator(result_path="file:///tmp/x", **kwargs)


class TestNormalizeRequests:
    def test_strings_and_dicts_are_accepted(self):
        assert LLMBatchOperator._normalize_requests(["a", {"prompt": "b", "params": {"x": 1}}]) == [
            {"prompt": "a"},
            {"prompt": "b", "params": {"x": 1}},
        ]

    def test_a_bare_string_is_rejected_instead_of_iterated_per_character(self):
        with pytest.raises(LLMBatchInputError, match="must return the whole list"):
            LLMBatchOperator._normalize_requests("summarize this")

    @pytest.mark.parametrize("bad_item", [42, None, ["nested"], {"prompt": 42}, {"text": "no prompt key"}])
    def test_an_item_that_is_not_a_prompt_is_rejected_with_its_index(self, bad_item):
        with pytest.raises(LLMBatchInputError, match="Request 1 must be a string or a dict"):
            LLMBatchOperator._normalize_requests(["ok", bad_item])

    def test_empty_list_is_rejected(self):
        with pytest.raises(LLMBatchInputError, match="at least one request"):
            LLMBatchOperator._normalize_requests([])


class TestModelFromConnection:
    def test_model_id_falls_back_to_the_connection_model_field(self, fake_adapter, result_path):
        ctx = _context()
        conn = _connection(extra={"model": "openai:gpt-5-mini"})
        with _running(fake_adapter, conn=conn):
            op = _make_operator(result_path=result_path, deferrable=True, model_id=None)
            with pytest.raises(TaskDeferred):
                op.execute(ctx)

        assert fake_adapter.submit_calls[0]["model"] == "gpt-5-mini"


class TestReattachDoesNotResubmit:
    def test_second_execute_with_same_identity_and_input_skips_submit(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            op1 = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op1.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            op2 = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(TaskDeferred):
                op2.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

    def test_adapter_is_closed_after_a_deferral_and_after_a_sync_run(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)
            assert fake_adapter.close_calls == 1

            fake_adapter.get_batch_status = "completed"
            fake_adapter.results = [_success_item(0, "hi")]
            _make_operator(result_path=result_path, deferrable=False).execute(ctx)
        # execute() closes its adapter, and _land() closes the one it builds for the download.
        assert fake_adapter.close_calls == 3


class TestIntentIsRecordedBeforeSubmit:
    def test_a_submit_that_raises_leaves_an_intent_record_with_no_batch_id(self, fake_adapter, result_path):
        ctx = _context()

        def _boom(*args, **kwargs):
            raise ConnectionError("socket closed mid-request")

        fake_adapter.submit = _boom
        with _running(fake_adapter):
            op = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(ConnectionError):
                op.execute(ctx)

        key, _ = LLMBatchOperator._identity(ctx)
        record = state_module.read_state(ObjectStoragePath(result_path), key)
        assert record is not None
        assert record.batch_id is None
        assert record.input_fingerprint == _expected_fingerprint(["hello"])
        assert record.intent_at is not None


class TestFingerprintMismatchTriggersCancelAndResubmit:
    def test_changed_prompt_cancels_old_batch_and_submits_new_one(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(requests=["hello"], result_path=result_path, deferrable=True).execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            with pytest.raises(TaskDeferred):
                _make_operator(requests=["goodbye"], result_path=result_path, deferrable=True).execute(ctx)
            assert len(fake_adapter.submit_calls) == 2
            assert fake_adapter.cancel_calls == ["batch_1"]

    def test_on_stale_state_fail_raises_instead_of_resubmitting(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(requests=["hello"], result_path=result_path, deferrable=True).execute(ctx)

            op2 = _make_operator(
                requests=["goodbye"], result_path=result_path, deferrable=True, on_stale_state="fail"
            )
            with pytest.raises(LLMBatchStaleStateError, match="prompts or other request content changed"):
                op2.execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

    def test_stale_batch_is_cancelled_through_the_connection_that_owns_it(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter) as build_adapter:
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True, llm_conn_id="account_a").execute(ctx)

            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True, llm_conn_id="account_b").execute(ctx)

        build_adapter.assert_any_call("openai", llm_conn_id="account_a")
        assert fake_adapter.cancel_calls == ["batch_1"]
        assert len(fake_adapter.submit_calls) == 2


class TestOutputTypeChangeDoesNotReattach:
    def test_output_type_change_with_identical_prompt_forces_resubmit(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(
                    requests=["hello"], result_path=result_path, deferrable=True, output_type=str
                ).execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            with pytest.raises(TaskDeferred):
                _make_operator(
                    requests=["hello"], result_path=result_path, deferrable=True, output_type=Diagnosis
                ).execute(ctx)
            assert len(fake_adapter.submit_calls) == 2


class TestEndToEndStructuredBatch:
    def test_success_and_invalid_output_rows_land_and_counts_reconcile(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "completed"
        ctx = _context()
        real_submit = fake_adapter.submit

        def _submit_then_seed(requests, *, idempotency_key, **kwargs):
            fake_adapter.results = [
                _success_item(0, '{"age": 30}', prefix=idempotency_key),
                _success_item(1, '{"age": "not-a-number"}', prefix=idempotency_key),
            ]
            return real_submit(requests, idempotency_key=idempotency_key, **kwargs)

        fake_adapter.submit = _submit_then_seed
        with _running(fake_adapter):
            op = _make_operator(
                requests=["a", "b"], result_path=result_path, deferrable=False, output_type=Diagnosis
            )
            manifest = op.execute(ctx)

        assert manifest["request_count"] == 2
        assert manifest["counts"]["succeeded"] == 1
        assert manifest["counts"]["invalid_output"] == 1
        assert manifest["request_count"] == sum(manifest["counts"].values())
        assert manifest["terminal_reason"] == "partial"

        rows = [
            json.loads(line) for line in ObjectStoragePath(manifest["result_uri"]).read_text().splitlines()
        ]
        assert {row["status"] for row in rows} == {"success", "invalid_output"}
        assert len(_state_files(result_path)) == 1, "state is kept so a clear re-lands the same results"

    def test_clearing_a_finished_task_relands_without_resubmitting(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "completed"
        fake_adapter.results = [_success_item(0, "hi")]
        ctx = _context()
        with _running(fake_adapter):
            first = _make_operator(result_path=result_path, deferrable=False).execute(ctx)
            second = _make_operator(result_path=result_path, deferrable=False).execute(ctx)

        assert len(fake_adapter.submit_calls) == 1
        assert second["batch_id"] == first["batch_id"]
        assert second["counts"] == first["counts"]

    def test_fail_on_partial_error_raises_after_landing_results_and_a_retry_reattaches(
        self, fake_adapter, result_path
    ):
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
        with _running(fake_adapter):
            op = _make_operator(
                requests=["a"], result_path=result_path, deferrable=False, fail_on_partial_error=True
            )
            with pytest.raises(LLMBatchPartialFailureError, match="errored=1"):
                op.execute(ctx)

            assert len(list(ObjectStoragePath(result_path).glob("*.jsonl"))) == 1
            assert _state_files(result_path), "state is kept so a retry re-attaches"

            op_retry = _make_operator(
                requests=["a"], result_path=result_path, deferrable=False, fail_on_partial_error=True
            )
            with pytest.raises(LLMBatchPartialFailureError):
                op_retry.execute(ctx)
        assert len(fake_adapter.submit_calls) == 1

    def test_fail_on_partial_error_triggers_on_invalid_output_alone(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "completed"
        fake_adapter.results = [_success_item(0, '{"age": "not-a-number"}')]
        ctx = _context()
        with _running(fake_adapter):
            op = _make_operator(
                requests=["a"],
                result_path=result_path,
                deferrable=False,
                fail_on_partial_error=True,
                output_type=Diagnosis,
            )
            with pytest.raises(LLMBatchPartialFailureError, match="invalid_output=1"):
                op.execute(ctx)

    def test_missing_alone_triggers_fail_on_partial_error(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "completed"
        fake_adapter.results = []
        ctx = _context()
        with _running(fake_adapter):
            op = _make_operator(
                requests=["a", "b"], result_path=result_path, deferrable=False, fail_on_partial_error=True
            )
            with pytest.raises(LLMBatchPartialFailureError, match="missing=2"):
                op.execute(ctx)


class TestStateReadFailurePropagates:
    def test_corrupt_state_file_raises_instead_of_resubmitting(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)
            assert len(fake_adapter.submit_calls) == 1

            next(iter(_state_files(result_path))).write_text("{not valid json")

            with pytest.raises(LLMBatchStateReadError):
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)
        assert len(fake_adapter.submit_calls) == 1


class TestOrphanRecovery:
    def test_recovered_orphan_reattaches_without_resubmitting(self, fake_adapter, result_path):
        ctx = _context()
        fingerprint = _expected_fingerprint(["hello"])
        _write_intent(result_path, ctx, fingerprint=fingerprint)
        fake_adapter.orphan_recovery_batch_id = "recovered_batch_123"
        fake_adapter.orphan_recovery_fingerprint = fingerprint

        with _running(fake_adapter):
            with pytest.raises(TaskDeferred) as exc_info:
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)

        assert fake_adapter.submit_calls == []
        assert exc_info.value.trigger.batch_id == "recovered_batch_123"
        assert fake_adapter.find_orphaned_batch_calls[0]["input_fingerprint"] == fingerprint

    def test_recovered_orphan_anchors_end_time_to_intent_at_not_now(self, fake_adapter, result_path):
        ctx = _context()
        fingerprint = _expected_fingerprint(["hello"])
        intent_at = "2020-01-01T00:00:00+00:00"
        _write_intent(result_path, ctx, fingerprint=fingerprint, intent_at=intent_at)
        fake_adapter.orphan_recovery_batch_id = "recovered_batch_123"
        fake_adapter.orphan_recovery_fingerprint = fingerprint
        intent_epoch = datetime.fromisoformat(intent_at).timestamp()

        with (
            _running(fake_adapter),
            mock.patch.object(llm_batch_module.time, "time", autospec=True, return_value=intent_epoch + 5),
        ):
            with pytest.raises(TaskDeferred) as exc_info:
                _make_operator(result_path=result_path, deferrable=True, timeout=1000).execute(ctx)

        assert exc_info.value.trigger.end_time == pytest.approx(intent_epoch + 1000, abs=2)

    def test_no_orphan_found_submits_a_new_batch(self, fake_adapter, result_path):
        ctx = _context()
        _write_intent(result_path, ctx, fingerprint=_expected_fingerprint(["hello"]))
        fake_adapter.orphan_recovery_batch_id = None

        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)
        assert len(fake_adapter.submit_calls) == 1

    def test_fingerprint_mismatch_falls_back_to_a_fresh_submit(self, fake_adapter, result_path):
        ctx = _context()
        _write_intent(result_path, ctx, fingerprint="whatever-was-recorded-before")
        fake_adapter.orphan_recovery_batch_id = "recovered_batch_123"
        fake_adapter.orphan_recovery_fingerprint = "some-completely-different-fingerprint"

        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)

        assert len(fake_adapter.submit_calls) == 1
        assert fake_adapter.submit_calls[0]["input_fingerprint"] == _expected_fingerprint(["hello"])

    def test_on_orphaned_intent_fail_raises_instead_of_resubmitting(self, fake_adapter, result_path):
        ctx = _context()
        _write_intent(result_path, ctx, fingerprint=_expected_fingerprint(["hello"]))
        fake_adapter.orphan_recovery_batch_id = None

        with _running(fake_adapter):
            op = _make_operator(result_path=result_path, deferrable=True, on_orphaned_intent="fail")
            with pytest.raises(LLMBatchOrphanedIntentError):
                op.execute(ctx)
        assert fake_adapter.submit_calls == []

    def test_a_failed_lookup_is_unknown_not_absent_and_never_resubmits(self, fake_adapter, result_path):
        ctx = _context()
        _write_intent(result_path, ctx, fingerprint=_expected_fingerprint(["hello"]))
        fake_adapter.orphan_lookup_error = ConnectionError("provider unreachable")

        with _running(fake_adapter):
            op = _make_operator(result_path=result_path, deferrable=True)
            with pytest.raises(LLMBatchOrphanLookupError, match="provider unreachable"):
                op.execute(ctx)
        assert fake_adapter.submit_calls == []
        assert _state_files(result_path), "the intent record stays so the retry checks again"


class TestCancelledBatch:
    def test_cancelled_with_lost_requests_lands_partials_raises_and_clears_state(
        self, fake_adapter, result_path
    ):
        fake_adapter.get_batch_status = "cancelled"
        fake_adapter.get_batch_counts = {"succeeded": 1, "errored": 0, "expired": 0, "cancelled": 1}
        fake_adapter.results = [_success_item(0, "hello", prefix="k")]
        ctx = _context()
        with _running(fake_adapter):
            op = _make_operator(requests=["a", "b"], result_path=result_path, deferrable=False)
            with pytest.raises(
                LLMBatchCancelledError, match="cancelled before 1 of 2 requests completed"
            ) as exc_info:
                op.execute(ctx)

        _, key16 = LLMBatchOperator._identity(ctx)
        result_file = ObjectStoragePath(result_path) / f"{key16}.jsonl"
        assert str(result_file) in str(exc_info.value)
        rows = [json.loads(line) for line in result_file.read_text().splitlines()]
        # The provider wrote no line for the cancelled request, so its row is "missing"; the
        # manifest relabels it from the job-level counts, which is what the error message reports.
        assert sorted(row["status"] for row in rows) == ["missing", "success"]
        assert _state_files(result_path) == [], "a cancelled batch is not something to re-attach to"

    def test_retry_after_our_own_timeout_cancel_submits_a_fresh_batch(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter), mock.patch.object(llm_batch_module.time, "sleep", autospec=True):
            # Attempt 1: still in progress at the deadline -> cancel_on_timeout cancels it.
            with mock.patch.object(
                llm_batch_module.time, "time", autospec=True, return_value=time.time() + 10_000
            ):
                op1 = _make_operator(result_path=result_path, deferrable=False, timeout=60)
                with pytest.raises(LLMBatchTimeoutError, match="timeout=60s.*was cancelled") as exc_info:
                    op1.execute(ctx)
            assert fake_adapter.cancel_calls == ["batch_1"]
            assert "cancel_on_timeout=True" in str(exc_info.value)
            assert _state_files(result_path), "the timeout itself keeps the state"

            # Attempt 2: re-attaches, finds the cancelled batch, lands nothing, clears the state.
            fake_adapter.get_batch_status = "cancelled"
            fake_adapter.get_batch_counts = {"succeeded": 0, "errored": 0, "expired": 0, "cancelled": 1}
            with pytest.raises(LLMBatchCancelledError):
                _make_operator(result_path=result_path, deferrable=False, timeout=60).execute(ctx)
            assert _state_files(result_path) == []
            assert len(fake_adapter.submit_calls) == 1

            # Attempt 3: submits fresh instead of re-landing the cancelled batch as a success.
            fake_adapter.get_batch_status = "in_progress"
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True, timeout=60).execute(ctx)
            assert len(fake_adapter.submit_calls) == 2

    def test_cancelled_after_everything_completed_is_a_success(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "cancelled"
        fake_adapter.get_batch_counts = {"succeeded": 1, "errored": 0, "expired": 0, "cancelled": 0}
        fake_adapter.results = [_success_item(0, "hello", prefix="k")]
        ctx = _context()
        with _running(fake_adapter):
            manifest = _make_operator(requests=["a"], result_path=result_path, deferrable=False).execute(ctx)

        assert manifest["counts"]["succeeded"] == 1
        assert manifest["terminal_reason"] == "succeeded"
        assert _state_files(result_path)


class TestFailedBatch:
    def test_failed_clears_state_so_a_retry_resubmits(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "failed"
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(LLMBatchJobError, match="recorded state was cleared"):
                _make_operator(result_path=result_path, deferrable=False).execute(ctx)
            assert _state_files(result_path) == []

            with pytest.raises(LLMBatchJobError):
                _make_operator(result_path=result_path, deferrable=False).execute(ctx)
        assert len(fake_adapter.submit_calls) == 2


class TestPollingGivesUp:
    def test_persistent_poll_failures_inside_the_budget_keep_the_state(self, fake_adapter, result_path):
        fake_adapter.get_batch_error = RuntimeError("Connection reset by peer")
        ctx = _context()
        with (
            _running(fake_adapter),
            mock.patch.object(llm_batch_module.time, "sleep", autospec=True) as sleep,
        ):
            # The first get_batch on a fresh submit is the poll; re-attach never happens here.
            op = _make_operator(result_path=result_path, deferrable=False)
            with pytest.raises(
                LLMBatchJobError, match="Gave up polling batch batch_1 after 5 consecutive failures"
            ):
                op.execute(ctx)

        assert sleep.call_count == 4
        assert fake_adapter.cancel_calls == []
        assert _state_files(result_path), "the batch's fate is unknown, so a retry re-attaches"

    def test_timeout_without_cancel_says_the_batch_is_still_running(self, fake_adapter, result_path):
        ctx = _context()
        with (
            _running(fake_adapter),
            mock.patch.object(llm_batch_module.time, "sleep", autospec=True),
            mock.patch.object(
                llm_batch_module.time, "time", autospec=True, return_value=time.time() + 10_000
            ),
        ):
            op = _make_operator(
                result_path=result_path, deferrable=False, timeout=60, cancel_on_timeout=False
            )
            with pytest.raises(LLMBatchTimeoutError, match="left running \\(cancel_on_timeout=False\\)"):
                op.execute(ctx)
        assert fake_adapter.cancel_calls == []


class TestReattachTimeoutBudget:
    def test_retry_inside_the_budget_keeps_the_original_deadline(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True, timeout=1000).execute(ctx)

            key, _ = LLMBatchOperator._identity(ctx)
            record = state_module.read_state(ObjectStoragePath(result_path), key)
            submitted_epoch = datetime.fromisoformat(record.submitted_at).timestamp()

            with mock.patch.object(
                llm_batch_module.time, "time", autospec=True, return_value=submitted_epoch + 100
            ):
                with pytest.raises(TaskDeferred) as exc_info:
                    _make_operator(result_path=result_path, deferrable=True, timeout=1000).execute(ctx)

        assert exc_info.value.trigger.end_time == pytest.approx(submitted_epoch + 1000, abs=2)

    def test_retry_after_the_budget_elapsed_gets_a_fresh_one(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True, timeout=1000).execute(ctx)

            later = time.time() + 50_000
            with mock.patch.object(llm_batch_module.time, "time", autospec=True, return_value=later):
                with pytest.raises(TaskDeferred) as exc_info:
                    _make_operator(
                        result_path=result_path, deferrable=True, timeout=1000, cancel_on_timeout=False
                    ).execute(ctx)

        assert exc_info.value.trigger.end_time == pytest.approx(later + 1000, abs=2)
        assert len(fake_adapter.submit_calls) == 1


class TestExpiredRoutesThroughFinalize:
    def test_expired_status_produces_a_manifest_with_expired_counts(self, fake_adapter, result_path):
        fake_adapter.get_batch_status = "expired"
        fake_adapter.get_batch_counts = {"succeeded": 1, "errored": 0, "expired": 1, "cancelled": 0}
        fake_adapter.results = [_success_item(0, "hello", prefix="k")]
        ctx = _context()
        with _running(fake_adapter):
            manifest = _make_operator(requests=["a", "b"], result_path=result_path, deferrable=False).execute(
                ctx
            )

        assert manifest["counts"]["succeeded"] == 1
        assert manifest["counts"]["expired"] == 1
        assert manifest["terminal_reason"] == "expired"
        assert manifest["request_count"] == sum(manifest["counts"].values())
        assert ObjectStoragePath(manifest["result_uri"]).read_text().strip() != ""


class TestBatchForgottenByProvider:
    def test_not_found_on_reattach_submits_a_new_batch(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)

            fake_adapter.get_batch_error = _NotFound("no such batch")
            real_submit = fake_adapter.submit

            def _submit_and_recover(*args, **kwargs):
                fake_adapter.get_batch_error = None
                return real_submit(*args, **kwargs)

            fake_adapter.submit = _submit_and_recover
            with pytest.raises(TaskDeferred) as exc_info:
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)

        assert len(fake_adapter.submit_calls) == 2
        assert exc_info.value.trigger.batch_id == "batch_2"


class TestExecuteComplete:
    def test_trigger_event_round_trips_into_a_landed_manifest(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred) as exc_info:
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)
            trigger = exc_info.value.trigger

            fake_adapter.results = [_success_item(0, "hello", prefix="k")]
            event = {"status": "success", "batch_id": trigger.batch_id, "counts": None, "message": "done"}
            manifest = _make_operator(result_path=result_path, deferrable=True).execute_complete(ctx, event)

        assert manifest["batch_id"] == "batch_1"
        assert manifest["counts"]["succeeded"] == 1

    def test_timeout_event_raises_the_trigger_message(self, fake_adapter, result_path):
        ctx = _context()
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True).execute(ctx)
            event = {"status": "timeout", "batch_id": "batch_1", "counts": None, "message": "deadline passed"}
            with pytest.raises(LLMBatchTimeoutError, match="deadline passed"):
                _make_operator(result_path=result_path, deferrable=True).execute_complete(ctx, event)
        assert _state_files(result_path)


class TestOnKill:
    def test_cancels_the_batch_when_one_was_submitted(self, fake_adapter, result_path):
        with _running(fake_adapter):
            op = _make_operator(result_path=result_path)
            op.batch_id = "batch_1"
            op.on_kill()
        assert fake_adapter.cancel_calls == ["batch_1"]
        assert fake_adapter.close_calls == 1

    def test_is_a_no_op_without_a_batch_or_with_cancel_on_kill_false(self, fake_adapter, result_path):
        with _running(fake_adapter):
            _make_operator(result_path=result_path).on_kill()
            op = _make_operator(result_path=result_path, cancel_on_kill=False)
            op.batch_id = "batch_1"
            op.on_kill()
        assert fake_adapter.cancel_calls == []


class TestBatchIdXcom:
    def test_batch_id_is_pushed_for_observability(self, fake_adapter, result_path):
        pushed: list = []
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True).execute(_context(pushed=pushed))
        assert pushed == [{"key": "batch_id", "value": "batch_1"}]

    def test_do_xcom_push_false_skips_the_push(self, fake_adapter, result_path):
        pushed: list = []
        with _running(fake_adapter):
            with pytest.raises(TaskDeferred):
                _make_operator(result_path=result_path, deferrable=True, do_xcom_push=False).execute(
                    _context(pushed=pushed)
                )
        assert pushed == []
