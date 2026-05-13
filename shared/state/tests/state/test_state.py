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

from airflow_shared.state import BaseStateBackend, StateScope


class TestBaseStateBackend:
    @pytest.fixture
    def backend(self) -> BaseStateBackend:
        class ConcreteBackend(BaseStateBackend):
            def __init__(self):
                self.calls: list[str] = []

            def get(self, scope: StateScope, key: str) -> str | None:
                self.calls.append("get")
                return "value"

            def set(self, scope: StateScope, key: str, value: str) -> None:
                self.calls.append("set")

            def delete(self, scope: StateScope, key: str) -> None:
                self.calls.append("delete")

            def clear(self, scope: StateScope, *, all_map_indices: bool = False) -> None:
                self.calls.append("clear")

            async def aget(self, scope: StateScope, key: str) -> str | None:
                self.calls.append("aget")
                return "value"

            async def aset(self, scope: StateScope, key: str, value: str) -> None:
                self.calls.append("aset")

            async def adelete(self, scope: StateScope, key: str) -> None:
                self.calls.append("adelete")

            async def aclear(self, scope: StateScope, *, all_map_indices: bool = False) -> None:
                self.calls.append("aclear")

        return ConcreteBackend()

    def test_incomplete_subclass_raises_type_error(self):
        """A subclass that omits any abstract method cannot be instantiated."""

        class IncompleteBackend(BaseStateBackend):
            def get(self, scope: StateScope, key: str) -> str | None:
                return None

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteBackend()

    def test_abstract_methods_cover_full_interface(self):
        """BaseStateBackend enforces all 8 sync+async methods as abstract."""
        expected = {"get", "set", "delete", "clear", "aget", "aset", "adelete", "aclear"}
        assert BaseStateBackend.__abstractmethods__ == expected

    def test_task_state_serialize_deserialize_round_trip(self, backend):
        original = "app_1234"
        serialized = backend.serialize_task_state_value(value=original, key="job_id", ti_id="abc-123")
        deserialized = backend.deserialize_task_state_value(serialized)
        assert deserialized == original

    def test_custom_backend_overrides_task_state_ser_deser(self):
        class MyBackend(BaseStateBackend):
            def get(self, scope, key): ...
            def set(self, scope, key, value): ...
            def delete(self, scope, key): ...
            def clear(self, scope, *, all_map_indices=False): ...
            async def aget(self, scope, key): ...
            async def aset(self, scope, key, value): ...
            async def adelete(self, scope, key): ...
            async def aclear(self, scope, *, all_map_indices=False): ...

            def serialize_task_state_value(self, *, value, key, ti_id):
                return f"s3://bucket/{ti_id}/{key}"

            def deserialize_task_state_value(self, stored):
                return f"fetched:{stored}"

        b = MyBackend()
        assert (
            b.serialize_task_state_value(value="app_1234", key="job_id", ti_id="abc-123")
            == "s3://bucket/abc-123/job_id"
        )
        assert (
            b.deserialize_task_state_value("s3://bucket/abc-123/job_id")
            == "fetched:s3://bucket/abc-123/job_id"
        )

    def test_asset_state_serialize_deserialize_round_trip(self, backend):
        original = "2026-05-01"
        serialized = backend.serialize_asset_state_value(
            value="2026-05-01", key="watermark", asset_name="my_asset"
        )
        deserialized = backend.deserialize_asset_state_value(serialized)
        assert deserialized == original

    def test_custom_backend_overrides_asset_state_ser_deser(self):
        class MyBackend(BaseStateBackend):
            def get(self, scope, key): ...
            def set(self, scope, key, value): ...
            def delete(self, scope, key): ...
            def clear(self, scope, *, all_map_indices=False): ...
            async def aget(self, scope, key): ...
            async def aset(self, scope, key, value): ...
            async def adelete(self, scope, key): ...
            async def aclear(self, scope, *, all_map_indices=False): ...

            def serialize_asset_state_value(self, *, value, key, asset_name):
                return f"s3://bucket/assets/{asset_name}/{key}"

            def deserialize_asset_state_value(self, stored):
                return f"resolved:{stored}"

        b = MyBackend()
        assert (
            b.serialize_asset_state_value(value="2026-05-01", key="watermark", asset_name="my_asset")
            == "s3://bucket/assets/my_asset/watermark"
        )
        assert (
            b.deserialize_asset_state_value("s3://bucket/assets/my_asset/watermark")
            == "resolved:s3://bucket/assets/my_asset/watermark"
        )
