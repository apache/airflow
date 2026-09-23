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
from unittest.mock import patch

import pytest
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelResponse,
    TextPart,
)
from pydantic_ai.usage import RequestUsage

from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.sdk import ObjectStoragePath


@pytest.fixture
def tmp_cache_path(tmp_path):
    """Return a file:// path to a temporary directory for cache files."""
    return f"file://{tmp_path.as_posix()}"


@pytest.fixture
def storage(tmp_cache_path):
    with patch("airflow.providers.common.ai.durable.storage._get_base_path") as mock_base:
        mock_base.return_value = ObjectStoragePath(tmp_cache_path)
        yield DurableStorage(dag_id="test_dag", task_id="my_task", run_id="run_1", map_index=-1)


@pytest.fixture
def sample_response():
    return ModelResponse(parts=[TextPart(content="Hello!")])


class TestDurableStorageInit:
    def test_cache_id_is_deterministic(self):
        """The same task identity always maps to the same cache file (so retries resume)."""
        a = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)
        b = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)
        assert a._cache_id == b._cache_id

    def test_cache_id_differs_by_map_index(self):
        base = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)
        mapped = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=3)
        assert base._cache_id != mapped._cache_id

    def test_cache_id_no_collision_across_tasks(self):
        """Distinct (dag, task) pairs that concatenate to the same string must not
        share a cache file -- e.g. dag ``etl`` + task ``load_data`` vs dag
        ``etl_load`` + task ``data``. A plain ``_``-join aliased them, letting one
        task read, overwrite, or delete another task's durable cache."""
        a = DurableStorage(dag_id="etl", task_id="load_data", run_id="r")
        b = DurableStorage(dag_id="etl_load", task_id="data", run_id="r")
        assert a._cache_id != b._cache_id


class TestSaveLoadModelResponse:
    def test_save_and_load_roundtrips(self, storage, sample_response):
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp_abc")

        # Reset in-memory cache to force read from file
        storage._cache = None
        loaded, fingerprint = storage.load_model_response("model_step_0")

        assert loaded is not None
        assert loaded.parts[0].content == "Hello!"
        assert fingerprint == "fp_abc"

    def test_load_returns_none_when_no_cache(self, storage):
        assert storage.load_model_response("model_step_0") == (None, None)

    def test_metadata_carrying_response_roundtrips_byte_identical(self, storage):
        """Multi-step replay relies on cached responses round-tripping byte-identically:
        a later step's fingerprint includes earlier responses in history, metadata
        (usage, provider_response_id, finish_reason) and all. If a store/load cycle
        altered any of it, every multi-step replay would mismatch and re-run. Pin it."""
        resp = ModelResponse(
            parts=[TextPart(content="answer")],
            usage=RequestUsage(input_tokens=11, output_tokens=22),
            model_name="gpt-x",
            provider_response_id="resp_xyz",
            finish_reason="stop",
        )
        before = ModelMessagesTypeAdapter.dump_python([resp], mode="json")

        storage.save_model_response("model_step_0", resp, fingerprint="fp")
        storage._cache = None
        loaded, _ = storage.load_model_response("model_step_0")

        after = ModelMessagesTypeAdapter.dump_python([loaded], mode="json")
        assert after == before

    def test_stored_entry_is_single_encoded(self, storage, sample_response):
        """The response payload is stored as native JSON objects, not a nested
        JSON string -- the whole cache is encoded exactly once by ``_save_cache``."""
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")

        on_disk = json.loads(storage._get_path().read_text())
        entry = on_disk["model_step_0"]

        assert isinstance(entry, dict)  # not a re-encoded JSON string
        assert isinstance(entry["data"], list)  # not a nested JSON string
        assert entry["fingerprint"] == "fp"

    def test_legacy_entry_without_fingerprint_loads(self, storage, sample_response):
        """Entries written before fingerprinting (raw adapter JSON) load with a None fingerprint."""
        cache = storage._load_cache()
        cache["model_step_0"] = ModelMessagesTypeAdapter.dump_json([sample_response]).decode()
        storage._save_cache()
        storage._cache = None

        loaded, fingerprint = storage.load_model_response("model_step_0")

        assert loaded is not None
        assert loaded.parts[0].content == "Hello!"
        assert fingerprint is None


class TestSaveLoadToolResult:
    def test_save_and_load_roundtrips(self, storage):
        storage.save_tool_result("tool_step_0", {"rows": [1, 2, 3]}, fingerprint="fp")

        storage._cache = None
        found, value, fingerprint = storage.load_tool_result("tool_step_0")

        assert found is True
        assert value == {"rows": [1, 2, 3]}

    def test_fingerprint_roundtrips(self, storage):
        storage.save_tool_result("tool_step_0", "result", fingerprint="fp_tool")

        storage._cache = None
        found, value, fingerprint = storage.load_tool_result("tool_step_0")

        assert found is True
        assert fingerprint == "fp_tool"

    def test_legacy_entry_without_fingerprint_loads(self, storage):
        """Entries written before fingerprinting load with a None fingerprint."""
        cache = storage._load_cache()
        cache["tool_step_0"] = json.dumps({"__durable_cached__": True, "value": "old"})
        storage._save_cache()
        storage._cache = None

        found, value, fingerprint = storage.load_tool_result("tool_step_0")

        assert found is True
        assert value == "old"
        assert fingerprint is None

    def test_load_returns_false_when_no_cache(self, storage):
        found, value, fingerprint = storage.load_tool_result("tool_step_0")
        assert found is False
        assert value is None
        assert fingerprint is None

    def test_none_result_roundtrips(self, storage):
        storage.save_tool_result("tool_step_0", None, fingerprint="fp")

        storage._cache = None
        found, value, fingerprint = storage.load_tool_result("tool_step_0")

        assert found is True
        assert value is None

    def test_circular_reference_result_is_skipped_not_raised(self, storage):
        """A circular reference raises ValueError in json.dumps; it must skip the
        entry with a warning, not crash the (otherwise successful) tool step."""
        circular: dict = {}
        circular["self"] = circular

        storage.save_tool_result("tool_step_0", circular, fingerprint="fp")  # must not raise

        found, _, _ = storage.load_tool_result("tool_step_0")
        assert found is False


class TestMalformedEntries:
    def test_empty_data_list_degrades_to_miss(self, storage):
        """A torn entry whose data list is empty loads as a miss, not an IndexError."""
        cache = storage._load_cache()
        cache["model_step_0"] = {"fingerprint": "fp", "data": []}
        storage._save_cache()
        storage._cache = None

        assert storage.load_model_response("model_step_0") == (None, None)

    def test_entry_missing_data_key_degrades_to_miss(self, storage):
        cache = storage._load_cache()
        cache["model_step_0"] = {"fingerprint": "fp"}
        storage._save_cache()
        storage._cache = None

        assert storage.load_model_response("model_step_0") == (None, None)


class TestCleanup:
    def test_cleanup_deletes_file(self, storage, sample_response):
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")
        path = storage._get_path()
        assert path.exists()

        storage.cleanup()
        assert not path.exists()

    def test_cleanup_on_nonexistent_file(self, storage):
        storage.cleanup()  # Should not raise


class TestInMemoryCaching:
    def test_multiple_saves_write_single_file(self, storage, sample_response):
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")
        storage.save_tool_result("tool_step_1", "result", fingerprint="fp")
        storage.save_model_response("model_step_2", sample_response, fingerprint="fp")

        assert "model_step_0" in storage._cache
        assert "tool_step_1" in storage._cache
        assert "model_step_2" in storage._cache

    def test_cache_survives_reload(self, storage, sample_response):
        """Simulate retry: save cache, reset in-memory, reload from file."""
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")
        storage.save_tool_result("tool_step_1", "tool result", fingerprint="fp")

        # Simulate new DurableStorage instance (as on retry)
        storage._cache = None

        loaded_response, _ = storage.load_model_response("model_step_0")
        assert loaded_response is not None
        assert loaded_response.parts[0].content == "Hello!"

        found, value, _ = storage.load_tool_result("tool_step_1")
        assert found is True
        assert value == "tool result"
