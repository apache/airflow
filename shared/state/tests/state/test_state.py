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
