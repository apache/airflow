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

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Store backend requires Airflow >= 3.3", allow_module_level=True)

from airflow.providers.common.io.state_store import backend
from airflow.providers.common.io.state_store.backend import (
    StateStoreObjectStorageBackend,
    _build_asset_path,
    _build_task_path,
    _read_from_object_storage,
    _write_to_object_storage,
)
from airflow.sdk import ObjectStoragePath
from airflow.sdk.state import AssetScope, TaskScope

from tests_common.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def clear_caches():
    backend._get_base_path.cache_clear()
    backend._get_compression.cache_clear()
    backend._get_threshold.cache_clear()
    yield
    backend._get_base_path.cache_clear()
    backend._get_compression.cache_clear()
    backend._get_threshold.cache_clear()


@pytest.fixture
def base_path(tmp_path):
    store_path = tmp_path / "store"
    store_path.mkdir()
    return f"file://{store_path}"


@pytest.fixture
def conf_overrides(base_path):
    from tests_common.test_utils.config import conf_vars

    with conf_vars(
        {
            ("common.io", "state_store_objectstorage_path"): base_path,
            ("common.io", "state_store_objectstorage_compression"): "",
        }
    ):
        yield base_path


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task state store requires Airflow >= 3.3")
class TestPathBuilders:
    def test_build_task_path_segments(self, conf_overrides):
        scope = TaskScope(dag_id="my_dag", run_id="run_1", task_id="my_task", map_index=-1)
        path = _build_task_path(scope, "job_id")
        assert str(path).endswith("my_dag/run_1/my_task/-1/job_id")

    def test_build_task_path_encodes_special_chars(self, conf_overrides):
        scope = TaskScope(dag_id="a/b", run_id="r/1", task_id="t/x", map_index=0)
        path = _build_task_path(scope, "key/name")
        assert str(path).endswith("a%2Fb/r%2F1/t%2Fx/0/key%2Fname")
        assert "a/b" not in str(path)
        assert "key/name" not in str(path)

    def test_build_task_path_distinct_keys_dont_collide(self, conf_overrides):
        scope = TaskScope(dag_id="d", run_id="r", task_id="t", map_index=0)
        path_slash = _build_task_path(scope, "connector/offset")
        path_under = _build_task_path(scope, "connector_offset")
        assert str(path_slash) != str(path_under)

    def test_build_task_path_rejects_empty_and_dots(self, conf_overrides):
        scope = TaskScope(dag_id="d", run_id="r", task_id="t", map_index=0)
        with pytest.raises(ValueError, match="Invalid path segment"):
            _build_task_path(scope, "")
        with pytest.raises(ValueError, match="Invalid path segment"):
            _build_task_path(scope, "..")

    def test_build_asset_path_segments(self, conf_overrides):
        scope = AssetScope(name="my_asset")
        path = _build_asset_path(scope, "status")
        assert str(path).endswith("assets/my_asset/status")

    def test_build_asset_path_uses_uri_when_no_name(self, conf_overrides):
        scope = AssetScope(uri="s3://bucket/path")
        path_slash = _build_asset_path(scope, "key")
        scope2 = AssetScope(uri="s3://bucket_path")
        path_under = _build_asset_path(scope2, "key")
        assert str(path_slash) != str(path_under)

    def test_compression_suffix_appended(self, tmp_path):
        store_path = tmp_path / "store"
        store_path.mkdir()

        with conf_vars(
            {
                ("common.io", "state_store_objectstorage_path"): f"file://{store_path}",
                ("common.io", "state_store_objectstorage_compression"): "gzip",
            }
        ):
            backend._get_base_path.cache_clear()
            backend._get_compression.cache_clear()
            scope = TaskScope(dag_id="d", run_id="r", task_id="t", map_index=-1)
            path = _build_task_path(scope, "k")
            assert str(path).endswith(".gz")


class TestIOPrimitives:
    def test_write_and_read_roundtrip(self, conf_overrides):
        path = ObjectStoragePath(f"{conf_overrides}/test_key")
        _write_to_object_storage(path, '{"value": 42}')
        result = _read_from_object_storage(path)
        assert result == '{"value": 42}'

    def test_read_missing_returns_none(self, conf_overrides):
        path = ObjectStoragePath(f"{conf_overrides}/nonexistent")
        assert _read_from_object_storage(path) is None

    def test_write_creates_parent_dirs(self, conf_overrides):
        path = ObjectStoragePath(f"{conf_overrides}/a/b/c/key")
        _write_to_object_storage(path, "hello")
        assert path.exists()


class TestStateStoreObjectStorageBackend:
    @pytest.fixture
    def store(self, conf_overrides):
        return StateStoreObjectStorageBackend()

    @pytest.fixture
    def task_scope(self):
        return TaskScope(dag_id="my_dag", run_id="run_1", task_id="my_task", map_index=-1)

    @pytest.fixture
    def asset_scope(self):
        return AssetScope(name="my_asset")

    def test_set_and_get_task(self, store, task_scope):
        store.set(task_scope, "k", "hello")
        assert store.get(task_scope, "k") == "hello"

    def test_get_missing_returns_none(self, store, task_scope):
        assert store.get(task_scope, "missing") is None

    def test_delete_task(self, store, task_scope):
        store.set(task_scope, "k", "v")
        store.delete(task_scope, "k")
        assert store.get(task_scope, "k") is None

    def test_delete_missing_is_noop(self, store, task_scope):
        store.delete(task_scope, "does_not_exist")

    def test_clear_task_single_map_index(self, store, task_scope):
        store.set(task_scope, "k1", "v1")
        store.set(task_scope, "k2", "v2")
        store.clear(task_scope)
        assert store.get(task_scope, "k1") is None
        assert store.get(task_scope, "k2") is None

    def test_clear_task_all_map_indices(self, store):
        scope0 = TaskScope(dag_id="d", run_id="r", task_id="t", map_index=0)
        scope1 = TaskScope(dag_id="d", run_id="r", task_id="t", map_index=1)
        store.set(scope0, "k", "v0")
        store.set(scope1, "k", "v1")
        store.clear(scope0, all_map_indices=True)
        assert store.get(scope0, "k") is None
        assert store.get(scope1, "k") is None

    def test_set_and_get_asset(self, store, asset_scope):
        store.set(asset_scope, "status", "ok")
        assert store.get(asset_scope, "status") == "ok"

    def test_clear_asset(self, store, asset_scope):
        store.set(asset_scope, "k1", "v1")
        store.set(asset_scope, "k2", "v2")
        store.clear(asset_scope)
        assert store.get(asset_scope, "k1") is None
        assert store.get(asset_scope, "k2") is None

    def test_serialize_and_deserialize_task(self, store, task_scope):
        ref = store.serialize_task_state_store_to_ref(value={"x": 1}, key="job_id", scope=task_scope)
        assert ref.startswith("file://")
        result = store.deserialize_task_state_store_from_ref(ref)
        assert result == {"x": 1}

    def test_serialize_and_deserialize_asset(self, store, asset_scope):
        ref = store.serialize_asset_state_store_to_ref(value=[1, 2, 3], key="result", scope=asset_scope)
        assert ref.startswith("file://")
        result = store.deserialize_asset_state_store_from_ref(ref)
        assert result == [1, 2, 3]

    def test_deserialize_missing_ref_returns_none(self, store, conf_overrides):
        result = store.deserialize_task_state_store_from_ref(f"{conf_overrides}/no/such/path")
        assert result is None

    def test_task_serialize_offloads_to_storage(self, task_scope, base_path):
        with conf_vars(
            {
                ("common.io", "state_store_objectstorage_path"): base_path,
                ("common.io", "state_store_objectstorage_threshold"): "0",
            }
        ):
            backend._get_threshold.cache_clear()
            store = StateStoreObjectStorageBackend()
            ref = store.serialize_task_state_store_to_ref(value={"x": 1}, key="k", scope=task_scope)
            assert ref.startswith("file://")

    def test_asset_serialize_offloads_to_storage(self, asset_scope, base_path):
        with conf_vars(
            {
                ("common.io", "state_store_objectstorage_path"): base_path,
                ("common.io", "state_store_objectstorage_threshold"): "0",
            }
        ):
            backend._get_threshold.cache_clear()
            store = StateStoreObjectStorageBackend()
            ref = store.serialize_asset_state_store_to_ref(value={"x": 1}, key="k", scope=asset_scope)
            assert ref.startswith("file://")

    def test_task_serialize_to_db_when_below_threshold(self, task_scope, base_path):
        with conf_vars(
            {
                ("common.io", "state_store_objectstorage_path"): base_path,
                ("common.io", "state_store_objectstorage_threshold"): "10000",
            }
        ):
            backend._get_threshold.cache_clear()
            store = StateStoreObjectStorageBackend()
            ref = store.serialize_task_state_store_to_ref(value={"x": 1}, key="k", scope=task_scope)
            assert not ref.startswith("file://")
            assert store.deserialize_task_state_store_from_ref(ref) == {"x": 1}

    def test_asset_serialize_to_db_when_below_threshold(self, asset_scope, base_path):
        with conf_vars(
            {
                ("common.io", "state_store_objectstorage_path"): base_path,
                ("common.io", "state_store_objectstorage_threshold"): "10000",
            }
        ):
            backend._get_threshold.cache_clear()
            store = StateStoreObjectStorageBackend()
            ref = store.serialize_asset_state_store_to_ref(value={"x": 1}, key="k", scope=asset_scope)
            assert not ref.startswith("file://")
            assert store.deserialize_asset_state_store_from_ref(ref) == {"x": 1}

    def test_negative_threshold_raises(self, base_path):
        with conf_vars(
            {
                ("common.io", "state_store_objectstorage_path"): base_path,
                ("common.io", "state_store_objectstorage_threshold"): "-1",
            }
        ):
            backend._get_threshold.cache_clear()
            with pytest.raises(ValueError, match="must be non-negative"):
                backend._get_threshold()
