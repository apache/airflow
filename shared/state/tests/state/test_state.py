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

import asyncio

import pytest

from airflow_shared.state import BaseStateBackend, StateScope, TaskScope


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

            def clear(self, scope: StateScope) -> None:
                self.calls.append("clear")

            async def aget(self, scope: StateScope, key: str) -> str | None:
                self.calls.append("aget")
                return "value"

            async def aset(self, scope: StateScope, key: str, value: str) -> None:
                self.calls.append("aset")

            async def adelete(self, scope: StateScope, key: str) -> None:
                self.calls.append("adelete")

            async def aclear(self, scope: StateScope) -> None:
                self.calls.append("aclear")

        return ConcreteBackend()

    def test_aget_is_callable(self, backend):
        """aget can be awaited and returns a value."""
        scope = TaskScope("d", "r", "t")
        result = asyncio.run(backend.aget(scope, "k"))
        assert result == "value"
        assert "aget" in backend.calls

    def test_aset_is_callable(self, backend):
        """aset can be awaited."""
        scope = TaskScope("d", "r", "t")
        asyncio.run(backend.aset(scope, "k", "v"))
        assert "aset" in backend.calls

    def test_adelete_is_callable(self, backend):
        """adelete can be awaited."""
        scope = TaskScope("d", "r", "t")
        asyncio.run(backend.adelete(scope, "k"))
        assert "adelete" in backend.calls

    def test_aclear_is_callable(self, backend):
        """aclear can be awaited."""
        scope = TaskScope("d", "r", "t")
        asyncio.run(backend.aclear(scope))
        assert "aclear" in backend.calls
