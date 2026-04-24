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
        class SyncBackend(BaseStateBackend):
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

        return SyncBackend()

    def test_aget_delegates_to_get(self, backend):
        """aget calls get and returns its value."""
        scope = TaskScope("d", "r", "t")
        result = asyncio.run(backend.aget(scope, "k"))
        assert result == "value"
        assert "get" in backend.calls

    def test_aset_delegates_to_set(self, backend):
        """aset calls set."""
        scope = TaskScope("d", "r", "t")
        asyncio.run(backend.aset(scope, "k", "v"))
        assert "set" in backend.calls

    def test_adelete_delegates_to_delete(self, backend):
        """adelete calls delete."""
        scope = TaskScope("d", "r", "t")
        asyncio.run(backend.adelete(scope, "k"))
        assert "delete" in backend.calls

    def test_aclear_delegates_to_clear(self, backend):
        """aclear calls clear."""
        scope = TaskScope("d", "r", "t")
        asyncio.run(backend.aclear(scope))
        assert "clear" in backend.calls
