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
from unittest import mock

import pytest

from airflow.providers.common.ai.batch import state as state_module
from airflow.providers.common.ai.batch.state import (
    BatchStateRecord,
    compute_fingerprint,
    compute_identity_key,
    compute_output_schema_digest,
    delete_state,
    read_state,
    write_intent,
    write_submitted,
)
from airflow.providers.common.ai.exceptions import LLMBatchStateReadError
from airflow.sdk import ObjectStoragePath


@pytest.fixture
def result_path(tmp_path):
    return ObjectStoragePath(f"file://{tmp_path.as_posix()}")


class TestComputeIdentityKey:
    def test_separator_prevents_dag_task_boundary_collision(self):
        """A naive ``_``-joined key would collide: dag `etl`+task `load_data` vs. dag `etl_load`+task `data`."""
        a = compute_identity_key(dag_id="etl", task_id="load_data", run_id="r", map_index=-1)
        b = compute_identity_key(dag_id="etl_load", task_id="data", run_id="r", map_index=-1)
        assert a != b

    def test_map_index_distinguishes_mapped_instances(self):
        a = compute_identity_key(dag_id="d", task_id="t", run_id="r", map_index=0)
        b = compute_identity_key(dag_id="d", task_id="t", run_id="r", map_index=1)
        assert a != b


class TestComputeFingerprint:
    def _base_kwargs(self, **overrides):
        kwargs = dict(
            requests=[{"prompt": "hello"}],
            llm_conn_id="my_openai",
            model_id="openai:gpt-5",
            system_prompt="",
            max_tokens=1024,
            request_params=None,
            output_schema="str",
        )
        kwargs.update(overrides)
        return kwargs

    def test_prompt_change_changes_fingerprint(self):
        a = compute_fingerprint(**self._base_kwargs())
        b = compute_fingerprint(**self._base_kwargs(requests=[{"prompt": "goodbye"}]))
        assert a != b

    def test_output_schema_change_changes_fingerprint_even_if_prompt_is_identical(self):
        """
        Schema is sent to the provider as part of the request body, so it must be part of
        the fingerprint -- otherwise editing a Pydantic ``output_type`` and clearing the task
        would silently re-attach to a batch produced under the old schema.
        """
        schema_v1 = {"title": "Diagnosis", "properties": {"age": {"type": "integer"}}}
        schema_v2 = {
            "title": "Diagnosis",
            "properties": {"age": {"type": "integer"}, "notes": {"type": "string"}},
        }
        a = compute_fingerprint(**self._base_kwargs(output_schema=schema_v1))
        b = compute_fingerprint(**self._base_kwargs(output_schema=schema_v2))
        assert a != b

    def test_llm_conn_id_change_changes_fingerprint_even_if_everything_else_is_identical(self):
        """
        Two ``pydanticai`` connections can point at two different accounts. Without
        ``llm_conn_id`` in the fingerprint, switching connections and rerunning would silently
        re-attach to (and return the results of) a batch billed to a different account.
        """
        a = compute_fingerprint(**self._base_kwargs(llm_conn_id="my_openai"))
        b = compute_fingerprint(**self._base_kwargs(llm_conn_id="someone_elses_openai"))
        assert a != b

    def test_fingerprint_is_unaffected_by_the_state_record_schema_version(self):
        """
        The on-disk record version is deliberately not part of the fingerprint material: bumping
        it must not make every in-flight batch look stale and get cancelled and resubmitted.
        """
        before = compute_fingerprint(**self._base_kwargs())
        with mock.patch.object(state_module, "SCHEMA_VERSION", 99):
            during = compute_fingerprint(**self._base_kwargs())
        after = compute_fingerprint(**self._base_kwargs())
        assert before == during == after


class TestComputeOutputSchemaDigest:
    def test_differs_for_different_material(self):
        assert compute_output_schema_digest("str") != compute_output_schema_digest({"type": "object"})


class TestPhaseAAndPhaseBWrites:
    def test_intent_record_has_null_batch_id(self, result_path):
        write_intent(
            result_path,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
        )
        record = read_state(result_path, "k1")
        assert record is not None
        assert record.batch_id is None
        assert record.input_fingerprint == "fp1"
        assert record.output_schema_digest == "sd1"
        assert record.intent_at == "2026-09-11T03:00:00+00:00"

    def test_phase_b_overwrites_phase_a_with_full_record(self, result_path):
        write_intent(
            result_path,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
        )
        write_submitted(
            result_path,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
            adapter="openai",
            llm_conn_id="my_openai",
            model_id="openai:gpt-5",
            output_type_ref=None,
            batch_id="batch_abc123",
            provider_input_ref="file-xyz",
            request_count=12345,
            submitted_at="2026-09-11T04:00:00+00:00",
        )
        record = read_state(result_path, "k1")
        assert record == BatchStateRecord(
            schema_version=1,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
            adapter="openai",
            llm_conn_id="my_openai",
            model_id="openai:gpt-5",
            output_type_ref=None,
            batch_id="batch_abc123",
            provider_input_ref="file-xyz",
            request_count=12345,
            submitted_at="2026-09-11T04:00:00+00:00",
        )

    def test_write_submitted_allows_null_intent_at(self, result_path):
        """A fresh (non-orphan-recovered) submit still has a real intent_at; this covers the
        defensive None case (e.g. backfilling an older record) without requiring one everywhere."""
        write_submitted(
            result_path,
            key="k2",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at=None,
            adapter="openai",
            llm_conn_id="my_openai",
            model_id="openai:gpt-5",
            output_type_ref=None,
            batch_id="batch_abc123",
            provider_input_ref="file-xyz",
            request_count=1,
            submitted_at="2026-09-11T04:00:00+00:00",
        )
        record = read_state(result_path, "k2")
        assert record is not None
        assert record.intent_at is None

    def test_missing_state_reads_as_none(self, result_path):
        assert read_state(result_path, "does-not-exist") is None

    def test_delete_state_removes_the_record(self, result_path):
        write_intent(
            result_path,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
        )
        delete_state(result_path, "k1")
        assert read_state(result_path, "k1") is None

    def test_delete_state_is_idempotent(self, result_path):
        """Deleting an already-absent (or never-written) state file must not raise."""
        delete_state(result_path, "never-written")


class TestReadStateErrorHandling:
    """
    Only a genuine not-found reads as "no recorded batch". Everything else -- a transient
    I/O failure, or a state file that exists but is corrupt -- must raise, not degrade to
    ``None``, since ``None`` is exactly the signal that says "safe to submit a new batch".
    """

    def test_corrupt_json_raises_state_read_error_not_none(self, result_path):
        write_intent(
            result_path,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
        )
        path = result_path / "_airflow_batch_state" / "k1.json"
        path.write_text("{not valid json")
        with pytest.raises(LLMBatchStateReadError, match="corrupt|malformed"):
            read_state(result_path, "k1")

    def test_missing_required_key_raises_state_read_error_not_none(self, result_path):
        path = result_path / "_airflow_batch_state" / "k1.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"schema_version": 1}))  # missing input_fingerprint etc.
        with pytest.raises(LLMBatchStateReadError):
            read_state(result_path, "k1")

    @pytest.mark.parametrize("raw_json", ["[]", "null"])
    def test_non_object_json_raises_state_read_error_not_type_error(self, result_path, raw_json):
        """A state file that parses to a JSON array or ``null`` is not a valid record -- this
        must surface as the same read-error signal as any other corruption, not as a bare
        ``TypeError`` leaking out of ``BatchStateRecord.from_dict``."""
        path = result_path / "_airflow_batch_state" / "k1.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(raw_json)
        with pytest.raises(LLMBatchStateReadError):
            read_state(result_path, "k1")

    def test_mismatched_schema_version_raises_state_read_error(self, result_path):
        """A record written by a different schema version must not be silently accepted --
        the error message should say what's wrong instead of just "corrupt"."""
        write_intent(
            result_path,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
        )
        path = result_path / "_airflow_batch_state" / "k1.json"
        data = json.loads(path.read_text())
        data["schema_version"] = 99
        path.write_text(json.dumps(data))
        with pytest.raises(LLMBatchStateReadError, match="schema version"):
            read_state(result_path, "k1")

    def test_generic_os_error_on_read_raises_state_read_error_not_none(self, result_path):
        write_intent(
            result_path,
            key="k1",
            input_fingerprint="fp1",
            output_schema_digest="sd1",
            intent_at="2026-09-11T03:00:00+00:00",
        )
        with mock.patch.object(
            ObjectStoragePath, "read_text", autospec=True, side_effect=OSError("connection reset")
        ):
            with pytest.raises(LLMBatchStateReadError, match="connection reset"):
                read_state(result_path, "k1")
