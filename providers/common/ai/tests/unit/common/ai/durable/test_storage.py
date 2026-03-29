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

from unittest.mock import patch

import pytest
from pydantic_ai.messages import (
    ModelResponse,
    TextPart,
)

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
    def test_cache_id_format(self, storage):
        assert storage._cache_id == "test_dag_my_task_run_1"

    def test_cache_id_with_map_index(self):
        s = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=3)
        assert s._cache_id == "d_t_r_3"

    def test_cache_id_without_map_index(self):
        s = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)
        assert "_-1" not in s._cache_id


class TestSaveLoadModelResponse:
    def test_save_and_load_roundtrips(self, storage, sample_response):
        storage.save_model_response("model_step_0", sample_response)

        # Reset in-memory cache to force read from file
        storage._cache = None
        loaded = storage.load_model_response("model_step_0")

        assert loaded is not None
        assert loaded.parts[0].content == "Hello!"

    def test_load_returns_none_when_no_cache(self, storage):
        assert storage.load_model_response("model_step_0") is None


class TestSaveLoadToolResult:
    def test_save_and_load_roundtrips(self, storage):
        storage.save_tool_result("tool_step_0", {"rows": [1, 2, 3]})

        storage._cache = None
        found, value = storage.load_tool_result("tool_step_0")

        assert found is True
        assert value == {"rows": [1, 2, 3]}

    def test_load_returns_false_when_no_cache(self, storage):
        found, value = storage.load_tool_result("tool_step_0")
        assert found is False
        assert value is None

    def test_none_result_roundtrips(self, storage):
        storage.save_tool_result("tool_step_0", None)

        storage._cache = None
        found, value = storage.load_tool_result("tool_step_0")

        assert found is True
        assert value is None


class TestCleanup:
    def test_cleanup_deletes_file(self, storage, sample_response):
        storage.save_model_response("model_step_0", sample_response)
        path = storage._get_path()
        assert path.exists()

        storage.cleanup()
        assert not path.exists()

    def test_cleanup_on_nonexistent_file(self, storage):
        storage.cleanup()  # Should not raise


class TestInMemoryCaching:
    def test_multiple_saves_write_single_file(self, storage, sample_response):
        storage.save_model_response("model_step_0", sample_response)
        storage.save_tool_result("tool_step_1", "result")
        storage.save_model_response("model_step_2", sample_response)

        assert "model_step_0" in storage._cache
        assert "tool_step_1" in storage._cache
        assert "model_step_2" in storage._cache

    def test_cache_survives_reload(self, storage, sample_response):
        """Simulate retry: save cache, reset in-memory, reload from file."""
        storage.save_model_response("model_step_0", sample_response)
        storage.save_tool_result("tool_step_1", "tool result")

        # Simulate new DurableStorage instance (as on retry)
        storage._cache = None

        loaded_response = storage.load_model_response("model_step_0")
        assert loaded_response is not None
        assert loaded_response.parts[0].content == "Hello!"

        found, value = storage.load_tool_result("tool_step_1")
        assert found is True
        assert value == "tool result"
