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

from airflow_shared.state import AssetScope, BaseStoreBackend, StoreScope, TaskScope


class TestAssetScope:
    def test_requires_at_least_one_identifier(self):
        with pytest.raises(ValueError, match="at least one of"):
            AssetScope()

    def test_asset_id_alone_is_valid(self):
        AssetScope(asset_id=1)

    def test_name_alone_is_valid(self):
        AssetScope(name="my_asset")

    def test_uri_alone_is_valid(self):
        AssetScope(uri="s3://bucket/key")


class TestBaseStoreBackend:
    @pytest.fixture
    def backend(self) -> BaseStoreBackend:
        class ConcreteBackend(BaseStoreBackend):
            def __init__(self):
                self.calls: list[str] = []

            def get(self, scope: StoreScope, key: str) -> str | None:
                self.calls.append("get")
                return "value"

            def set(self, scope: StoreScope, key: str, value: str) -> None:
                self.calls.append("set")

            def delete(self, scope: StoreScope, key: str) -> None:
                self.calls.append("delete")

            def clear(self, scope: StoreScope, *, all_map_indices: bool = False) -> None:
                self.calls.append("clear")

            async def aget(self, scope: StoreScope, key: str) -> str | None:
                self.calls.append("aget")
                return "value"

            async def aset(self, scope: StoreScope, key: str, value: str) -> None:
                self.calls.append("aset")

            async def adelete(self, scope: StoreScope, key: str) -> None:
                self.calls.append("adelete")

            async def aclear(self, scope: StoreScope, *, all_map_indices: bool = False) -> None:
                self.calls.append("aclear")

        return ConcreteBackend()

    def test_incomplete_subclass_raises_type_error(self):
        """A subclass that omits any abstract method cannot be instantiated."""

        class IncompleteBackend(BaseStoreBackend):
            def get(self, scope: StoreScope, key: str) -> str | None:
                return None

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteBackend()

    def test_abstract_methods_cover_full_interface(self):
        """BaseStoreBackend enforces all 8 sync+async methods as abstract."""
        expected = {"get", "set", "delete", "clear", "aget", "aset", "adelete", "aclear"}
        assert BaseStoreBackend.__abstractmethods__ == expected

    def test_task_state_store_serialize_deserialize_round_trip(self, backend):
        original = "app_1234"
        scope = TaskScope(dag_id="d", run_id="r", task_id="t", map_index=-1)
        serialized = backend.serialize_task_state_store_to_ref(value=original, key="job_id", scope=scope)
        deserialized = backend.deserialize_task_state_store_from_ref(serialized)
        assert deserialized == original

    def test_task_state_store_serialize_deserialize_typed_values(self, backend):
        """Default backend passes typed values through unchanged (custom backends handle storage)."""
        scope = TaskScope(dag_id="d", run_id="r", task_id="t", map_index=-1)
        assert (
            backend.deserialize_task_state_store_from_ref(
                backend.serialize_task_state_store_to_ref(value=42, key="count", scope=scope)
            )
            == 42
        )
        assert backend.deserialize_task_state_store_from_ref(
            backend.serialize_task_state_store_to_ref(value={"status": "ok"}, key="result", scope=scope)
        ) == {"status": "ok"}

    def test_custom_backend_overrides_task_state_store_ser_deser(self):
        class MyBackend(BaseStoreBackend):
            def get(self, scope, key): ...
            def set(self, scope, key, value): ...
            def delete(self, scope, key): ...
            def clear(self, scope, *, all_map_indices=False): ...
            async def aget(self, scope, key): ...
            async def aset(self, scope, key, value): ...
            async def adelete(self, scope, key): ...
            async def aclear(self, scope, *, all_map_indices=False): ...

            def serialize_task_state_store_to_ref(self, *, value, key, scope: TaskScope):
                return f"s3://bucket/{scope.dag_id}/{scope.task_id}/{key}"

            def deserialize_task_state_store_from_ref(self, stored):
                return f"fetched:{stored}"

        b = MyBackend()
        scope = TaskScope(dag_id="my_dag", run_id="r", task_id="my_task", map_index=-1)
        assert b.serialize_task_state_store_to_ref(value="app_1234", key="job_id", scope=scope) == (
            "s3://bucket/my_dag/my_task/job_id"
        )
        assert (
            b.deserialize_task_state_store_from_ref("s3://bucket/my_dag/my_task/job_id")
            == "fetched:s3://bucket/my_dag/my_task/job_id"
        )

    def test_asset_state_store_serialize_deserialize_round_trip(self, backend):
        original = "2026-05-01"
        scope = AssetScope(name="my_asset")
        serialized = backend.serialize_asset_state_store_to_ref(
            value="2026-05-01", key="watermark", scope=scope
        )
        deserialized = backend.deserialize_asset_state_store_from_ref(serialized)
        assert deserialized == original

    def test_asset_state_store_serialize_deserialize_typed_values(self, backend):
        scope = AssetScope(name="my_asset")
        assert (
            backend.deserialize_asset_state_store_from_ref(
                backend.serialize_asset_state_store_to_ref(value=5, key="total_runs", scope=scope)
            )
            == 5
        )
        assert backend.deserialize_asset_state_store_from_ref(
            backend.serialize_asset_state_store_to_ref(value={"rows": 1234}, key="last_run", scope=scope)
        ) == {"rows": 1234}

    def test_custom_backend_overrides_asset_state_store_ser_deser(self):
        class MyBackend(BaseStoreBackend):
            def get(self, scope, key): ...
            def set(self, scope, key, value): ...
            def delete(self, scope, key): ...
            def clear(self, scope, *, all_map_indices=False): ...
            async def aget(self, scope, key): ...
            async def aset(self, scope, key, value): ...
            async def adelete(self, scope, key): ...
            async def aclear(self, scope, *, all_map_indices=False): ...

            def serialize_asset_state_store_to_ref(self, *, value, key, scope: AssetScope):
                return f"s3://bucket/assets/{scope.name}/{key}"

            def deserialize_asset_state_store_from_ref(self, stored):
                return f"resolved:{stored}"

        b = MyBackend()
        scope = AssetScope(name="my_asset")
        assert b.serialize_asset_state_store_to_ref(value="2026-05-01", key="watermark", scope=scope) == (
            "s3://bucket/assets/my_asset/watermark"
        )
        assert (
            b.deserialize_asset_state_store_from_ref("s3://bucket/assets/my_asset/watermark")
            == "resolved:s3://bucket/assets/my_asset/watermark"
        )
