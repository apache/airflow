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
import json
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.toolsets.dataquality.base import BaseDQToolset
from airflow.providers.common.ai.utils.dataquality.models import DQCheckInput


class _ConcreteDQToolset(BaseDQToolset):
    """Minimal concrete subclass for testing BaseDQToolset."""

    @property
    def id(self) -> str:
        return "test-dq"

    @property
    def output_mode(self):
        return "execute"


class TestBaseDQToolsetSetChecks:
    def test_set_checks_stores_checks(self):
        ts = _ConcreteDQToolset()
        checks = [DQCheckInput(name="c1", description="desc1")]
        ts.set_checks(checks)
        assert ts._checks == checks

    def test_set_checks_replaces_previous(self):
        ts = _ConcreteDQToolset()
        ts.set_checks([DQCheckInput(name="old", description="old desc")])
        new = [DQCheckInput(name="new", description="new desc")]
        ts.set_checks(new)
        assert ts._checks == new


class TestBaseDQToolsetListChecks:
    def test_list_checks_returns_all_checks(self):
        ts = _ConcreteDQToolset()
        ts.set_checks(
            [
                DQCheckInput(name="null_ids", description="Check for null IDs"),
                DQCheckInput(name="row_count", description="Check minimum row count"),
            ]
        )
        result = json.loads(ts._list_checks())
        assert len(result) == 2
        names = [c["name"] for c in result]
        assert "null_ids" in names
        assert "row_count" in names

    def test_list_checks_empty_when_no_checks(self):
        ts = _ConcreteDQToolset()
        result = json.loads(ts._list_checks())
        assert result == []

    def test_list_checks_has_fixed_validator_field(self):
        ts = _ConcreteDQToolset()
        ts.set_checks(
            [
                DQCheckInput(name="c1", description="d1", validator=lambda v: True),
                DQCheckInput(name="c2", description="d2"),
            ]
        )
        result = {c["name"]: c for c in json.loads(ts._list_checks())}
        assert result["c1"]["has_fixed_validator"] is True
        assert result["c2"]["has_fixed_validator"] is False


class TestBaseDQToolsetGetTools:
    def test_get_tools_includes_list_checks(self):
        ts = _ConcreteDQToolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert "list_checks" in tools

    def test_call_tool_list_checks(self):
        ts = _ConcreteDQToolset()
        ts.set_checks([DQCheckInput(name="x", description="y")])
        result = asyncio.run(ts.call_tool("list_checks", {}, ctx=MagicMock(), tool=MagicMock()))
        data = json.loads(result)
        assert data[0]["name"] == "x"

    def test_call_tool_unknown_raises(self):
        ts = _ConcreteDQToolset()
        with pytest.raises(ValueError, match="Unknown tool"):
            asyncio.run(ts.call_tool("nonexistent", {}, ctx=MagicMock(), tool=MagicMock()))


class TestBaseDQToolsetOutputMode:
    def test_output_mode_not_implemented_on_base(self):
        class _IncompleteToolset(BaseDQToolset):
            @property
            def id(self):
                return "incomplete"

        ts = _IncompleteToolset()
        with pytest.raises(NotImplementedError):
            _ = ts.output_mode
