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

from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchState,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.decorators.llm_batch import _LLMBatchDecoratedOperator
from airflow.providers.common.compat.sdk import TaskDeferred


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


def _context():
    ti = SimpleNamespace(dag_id="dag", task_id="t", run_id="run_1", map_index=-1, xcom_push=lambda **kw: None)
    return {"task_instance": ti, "ti": ti}


class TestLLMBatchDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMBatchDecoratedOperator.custom_operator_name == "@task.llm_batch"

    def test_execute_calls_callable_and_normalizes_requests(self, tmp_path):
        """The callable's return value becomes ``requests``; a bare ``list[str]`` is accepted."""
        fake_adapter = _FakeAdapter()

        def build_prompts():
            return ["Summarize A", "Summarize B"]

        op = _LLMBatchDecoratedOperator(
            task_id="batch_task",
            python_callable=build_prompts,
            op_args=(),
            op_kwargs={},
            result_path=f"file://{tmp_path.as_posix()}",
            llm_conn_id="my_openai",
            model_id="openai:gpt-5",
            deferrable=True,
        )

        with (
            mock.patch(
                "airflow.providers.common.ai.operators.llm_batch.BaseHook.get_connection",
                return_value=SimpleNamespace(conn_type="pydanticai", password=None, host=None),
            ),
            mock.patch("airflow.providers.common.ai.batch.dispatch.build_adapter", return_value=fake_adapter),
            pytest.raises(TaskDeferred),
        ):
            op.execute(_context())

        assert fake_adapter.submit_calls[0]["requests"] == [
            {"prompt": "Summarize A"},
            {"prompt": "Summarize B"},
        ]
