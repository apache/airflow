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

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    # ``airflow.sdk.execution_time.context`` exists on older cores, but ``NEVER_EXPIRE``
    # (imported transitively via ``task_state_store``) only lands in 3.3, so an
    # ``importorskip`` on the module is not enough -- gate on the version instead.
    pytest.skip("task state store needs Airflow >= 3.3", allow_module_level=True)

from pydantic import JsonValue, TypeAdapter
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelResponse,
    TextPart,
)
from pydantic_ai.usage import RequestUsage

from airflow.providers.common.ai.durable.base import TOOL_RESULT_SENTINEL
from airflow.providers.common.ai.durable.task_state_store import TaskStateStoreDurableStorage
from airflow.sdk.execution_time.context import NEVER_EXPIRE

# The real accessor validates the value against pydantic ``JsonValue`` before persisting.
_JSON_VALUE: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


class FakeTaskStateStore:
    """In-memory stand-in for the ``context['task_state_store']`` accessor."""

    def __init__(self) -> None:
        self.store: dict = {}
        self.set_retentions: dict = {}
        self.deleted: list[str] = []

    def get(self, key, default=None):
        return self.store.get(key, default)

    def set(self, key, value, *, retention=None):
        # Mirror the real accessor: reject ``None`` and reject values that are not valid
        # ``JsonValue`` (tuples, non-string dict keys), then persist the JSON round-trip
        # -- so tests see the same rejections and Text-column round-trip as production.
        if value is None:
            raise ValueError("Cannot set value as None")
        _JSON_VALUE.validate_python(value)
        self.store[key] = json.loads(json.dumps(value))
        self.set_retentions[key] = retention

    def delete(self, key):
        self.deleted.append(key)
        self.store.pop(key, None)


@pytest.fixture
def accessor():
    return FakeTaskStateStore()


@pytest.fixture
def storage(accessor):
    return TaskStateStoreDurableStorage(accessor)


@pytest.fixture
def sample_response():
    return ModelResponse(parts=[TextPart(content="Hello!")])


class TestSaveLoadModelResponse:
    def test_save_and_load_roundtrips(self, storage, sample_response):
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp_abc")

        loaded, fingerprint = storage.load_model_response("model_step_0")

        assert loaded is not None
        assert loaded.parts[0].content == "Hello!"
        assert fingerprint == "fp_abc"

    def test_stored_with_never_expire(self, storage, accessor, sample_response):
        """Cache entries must survive every retry regardless of retry_delay or retention config."""
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")

        assert accessor.set_retentions["model_step_0"] is NEVER_EXPIRE

    def test_stored_entry_is_native_json_not_a_string(self, storage, accessor, sample_response):
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")

        entry = accessor.store["model_step_0"]
        assert isinstance(entry, dict)
        assert isinstance(entry["data"], list)
        assert entry["fingerprint"] == "fp"

    def test_metadata_carrying_response_roundtrips_byte_identical(self, storage):
        """A later step fingerprints earlier responses in history, metadata and all;
        a store/load cycle that altered any of it would mismatch and re-run."""
        resp = ModelResponse(
            parts=[TextPart(content="answer")],
            usage=RequestUsage(input_tokens=11, output_tokens=22),
            model_name="gpt-x",
            provider_response_id="resp_xyz",
            finish_reason="stop",
        )
        before = ModelMessagesTypeAdapter.dump_python([resp], mode="json")

        storage.save_model_response("model_step_0", resp, fingerprint="fp")
        loaded, _ = storage.load_model_response("model_step_0")

        after = ModelMessagesTypeAdapter.dump_python([loaded], mode="json")
        assert after == before

    def test_load_returns_none_when_missing(self, storage):
        assert storage.load_model_response("model_step_0") == (None, None)

    def test_empty_data_list_degrades_to_miss(self, storage, accessor):
        accessor.store["model_step_0"] = {"fingerprint": "fp", "data": []}
        assert storage.load_model_response("model_step_0") == (None, None)

    def test_entry_missing_data_key_degrades_to_miss(self, storage, accessor):
        accessor.store["model_step_0"] = {"fingerprint": "fp"}
        assert storage.load_model_response("model_step_0") == (None, None)


class TestSaveLoadToolResult:
    def test_save_and_load_roundtrips(self, storage):
        storage.save_tool_result("tool_step_0", {"rows": [1, 2, 3]}, fingerprint="fp_tool")

        found, value, fingerprint = storage.load_tool_result("tool_step_0")

        assert found is True
        assert value == {"rows": [1, 2, 3]}
        assert fingerprint == "fp_tool"

    def test_stored_with_never_expire(self, storage, accessor):
        storage.save_tool_result("tool_step_0", "result", fingerprint="fp")

        assert accessor.set_retentions["tool_step_0"] is NEVER_EXPIRE

    def test_none_result_roundtrips(self, storage):
        """A cached ``None`` is a hit, distinguished from a missing entry by the sentinel."""
        storage.save_tool_result("tool_step_0", None, fingerprint="fp")

        found, value, _ = storage.load_tool_result("tool_step_0")
        assert found is True
        assert value is None

    def test_load_returns_false_when_missing(self, storage):
        assert storage.load_tool_result("tool_step_0") == (False, None, None)

    def test_non_dict_entry_is_a_miss(self, storage, accessor):
        accessor.store["tool_step_0"] = "not a dict"
        assert storage.load_tool_result("tool_step_0") == (False, None, None)

    def test_entry_without_sentinel_is_a_miss(self, storage, accessor):
        accessor.store["tool_step_0"] = {"value": "x"}
        assert storage.load_tool_result("tool_step_0") == (False, None, None)
        assert TOOL_RESULT_SENTINEL not in accessor.store["tool_step_0"]

    def test_non_serializable_result_is_skipped_not_raised(self, storage, accessor):
        """A non-serializable tool result skips caching with a warning; the tool step still succeeds."""
        storage.save_tool_result("tool_step_0", object(), fingerprint="fp")  # must not raise

        assert "tool_step_0" not in accessor.store
        assert storage.load_tool_result("tool_step_0") == (False, None, None)

    def test_circular_reference_result_is_skipped_not_raised(self, storage, accessor):
        circular: dict = {}
        circular["self"] = circular

        storage.save_tool_result("tool_step_0", circular, fingerprint="fp")  # must not raise

        assert "tool_step_0" not in accessor.store

    def test_tuple_result_is_normalized_and_cached(self, storage):
        """A tuple result (a common ``return (value, meta)``) is coerced to a list and cached.

        The store validates against ``JsonValue`` and rejects tuples, so an un-normalized
        write would fail a tool step that already succeeded -- normalization keeps it durable.
        """
        storage.save_tool_result("tool_step_0", (1, 2, 3), fingerprint="fp")  # must not raise

        found, value, _ = storage.load_tool_result("tool_step_0")
        assert found is True
        assert value == [1, 2, 3]

    def test_non_string_keyed_dict_result_is_normalized_and_cached(self, storage):
        """A dict with non-string keys (e.g. ``DataFrame.to_dict()``) is coerced to string keys."""
        storage.save_tool_result("tool_step_0", {1: "a", 2: "b"}, fingerprint="fp")  # must not raise

        found, value, _ = storage.load_tool_result("tool_step_0")
        assert found is True
        assert value == {"1": "a", "2": "b"}


class TestCleanup:
    def test_cleanup_deletes_keys_written_this_run(self, storage, accessor, sample_response):
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")
        storage.save_tool_result("tool_step_1", "result", fingerprint="fp")

        storage.cleanup()

        assert "model_step_0" not in accessor.store
        assert "tool_step_1" not in accessor.store

    def test_cleanup_deletes_keys_only_replayed_this_run(self, accessor, sample_response):
        """A retry that replays an earlier attempt's keys (cache hits) must still clean them up."""
        TaskStateStoreDurableStorage(accessor).save_model_response(
            "model_step_0", sample_response, fingerprint="fp"
        )

        retry = TaskStateStoreDurableStorage(accessor)
        loaded, _ = retry.load_model_response("model_step_0")  # cache hit, no re-write
        assert loaded is not None

        retry.cleanup()
        assert "model_step_0" not in accessor.store

    def test_cleanup_leaves_untracked_keys_untouched(self, storage, accessor, sample_response):
        """Cleanup deletes only the durable keys it touched, never the whole task instance namespace."""
        accessor.store["user_key"] = "kept"
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")

        storage.cleanup()

        assert "model_step_0" not in accessor.store
        assert accessor.store["user_key"] == "kept"

    def test_cleanup_is_best_effort_on_delete_failure(self, accessor, sample_response):
        """A failing delete must not propagate out of cleanup."""
        storage = TaskStateStoreDurableStorage(accessor)
        storage.save_model_response("model_step_0", sample_response, fingerprint="fp")
        accessor.delete = lambda key: (_ for _ in ()).throw(RuntimeError("boom"))

        storage.cleanup()  # must not raise
