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
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.common.ai.batch import dispatch
from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchState,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.decorators.llm_batch import _LLMBatchDecoratedOperator
from airflow.providers.common.ai.exceptions import LLMBatchInputError
from airflow.providers.common.ai.operators import llm_batch as llm_batch_module
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.sdk import DAG, Connection


class _FakeAdapter(BatchAdapter):
    name = "openai"
    max_requests = 100_000
    max_payload_bytes = 100_000_000
    allows_per_request_model = False

    def __init__(self):
        self.submit_calls: list[dict] = []

    def validate_requests(self, requests, *, model, output_spec, **kwargs):
        pass

    def submit(self, requests, *, model, idempotency_key, input_fingerprint, output_spec, **kwargs):
        self.submit_calls.append({"requests": list(requests)})
        return SubmitResult(batch_id="batch_1", provider_input_ref=None)

    def get_batch(self, batch_id):
        return BatchState(status="in_progress", counts=None, error_message=None)

    def cancel_batch(self, batch_id):
        pass

    def iter_results(self, batch_id) -> Iterator[RawResultItem]:
        return iter(())

    def build_output_directive(self, spec):
        return {}

    def extract_output(self, raw, spec):
        return ExtractedOutput(kind="text", text=raw.raw)

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        return None


def _context(**extra):
    ti = SimpleNamespace(dag_id="dag", task_id="t", run_id="run_1", map_index=-1, xcom_push=lambda **kw: None)
    return {"task_instance": ti, "ti": ti, "run_id": "run_1", **extra}


def _patches(fake_adapter):
    conn = Connection(conn_id="c", conn_type="pydanticai", password=None, host=None)
    return (
        mock.patch.object(llm_batch_module.BaseHook, "get_connection", autospec=True, return_value=conn),
        mock.patch.object(
            dispatch, "build_adapter_from_connection", autospec=True, return_value=fake_adapter
        ),
    )


def _make(python_callable, tmp_path, **kwargs):
    kwargs.setdefault("result_path", f"file://{tmp_path.as_posix()}")
    return _LLMBatchDecoratedOperator(
        task_id="batch_task",
        python_callable=python_callable,
        op_args=(),
        op_kwargs={},
        llm_conn_id="my_openai",
        model_id="openai:gpt-5",
        deferrable=True,
        **kwargs,
    )


class TestLLMBatchDecoratedOperator:
    def test_execute_calls_callable_and_normalizes_requests(self, tmp_path):
        fake_adapter = _FakeAdapter()

        def build_prompts():
            return ["Summarize A", "Summarize B"]

        op = _make(build_prompts, tmp_path)
        conn_patch, adapter_patch = _patches(fake_adapter)
        with conn_patch, adapter_patch, pytest.raises(TaskDeferred):
            op.execute(_context())

        assert fake_adapter.submit_calls[0]["requests"] == [
            {"prompt": "Summarize A"},
            {"prompt": "Summarize B"},
        ]

    def test_returned_prompts_are_not_rendered_as_jinja(self, tmp_path):
        """Bulk third-party text may contain ``{{``/``{%``; it must reach the provider verbatim."""
        fake_adapter = _FakeAdapter()

        def build_prompts():
            return ["Ticket: see {{ var.value.secret }}", "Template snippet: {% if x %}"]

        op = _make(build_prompts, tmp_path)
        conn_patch, adapter_patch = _patches(fake_adapter)
        with conn_patch, adapter_patch, pytest.raises(TaskDeferred):
            op.execute(_context())

        assert [r["prompt"] for r in fake_adapter.submit_calls[0]["requests"]] == [
            "Ticket: see {{ var.value.secret }}",
            "Template snippet: {% if x %}",
        ]

    def test_other_template_fields_are_still_rendered(self, tmp_path):
        fake_adapter = _FakeAdapter()
        with DAG(dag_id="dag"):
            op = _make(lambda: ["a"], tmp_path, result_path=f"file://{tmp_path.as_posix()}/{{{{ run_id }}}}")
        op.render_template_fields(_context())
        assert op.result_path.endswith("/run_1")

        conn_patch, adapter_patch = _patches(fake_adapter)
        with conn_patch, adapter_patch, pytest.raises(TaskDeferred):
            op.execute(_context())
        assert (tmp_path / "run_1" / "_airflow_batch_state").exists()

    def test_requests_is_not_a_template_field(self):
        assert "requests" not in _LLMBatchDecoratedOperator.template_fields
        assert "result_path" in _LLMBatchDecoratedOperator.template_fields

    def test_a_callable_returning_a_single_string_is_rejected(self, tmp_path):
        fake_adapter = _FakeAdapter()
        op = _make(lambda: "just one prompt", tmp_path)
        conn_patch, adapter_patch = _patches(fake_adapter)
        with conn_patch, adapter_patch, pytest.raises(LLMBatchInputError, match="must return the whole list"):
            op.execute(_context())
        assert fake_adapter.submit_calls == []
