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
import copy
from typing import Any

import pytest

pytest.importorskip("strands")

from strands.tools.registry import ToolRegistry

from airflow.providers.common.ai.tools import AirflowTool, ToolResult
from airflow.providers.common.ai.tools.strands import as_strands_tools
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from unit.common.ai.toolsets.test_sql import _make_mock_db_hook


def _run(strands_tool, arguments: dict[str, Any]) -> dict[str, Any]:
    """Invoke a Strands tool the way the Strands executor does and return its ToolResult."""
    tool_use = {"toolUseId": "tu-1", "name": strands_tool.tool_name, "input": arguments}

    async def collect():
        return [event async for event in strands_tool.stream(tool_use, {})]

    return asyncio.run(collect())[-1].tool_result


def _tool(content, *, is_error: bool = False) -> AirflowTool:
    async def function(arguments: dict[str, Any]) -> ToolResult:
        return ToolResult(content=content, is_error=is_error)

    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]},
        function=function,
    )


class TestAsStrandsTools:
    def test_converts_every_tool_of_a_toolset(self):
        tools = as_strands_tools(SQLToolset("pg_default"))

        assert [t.tool_name for t in tools] == ["list_tables", "get_schema", "query", "check_query"]
        query = next(t for t in tools if t.tool_name == "query")
        assert query.tool_spec["inputSchema"]["json"]["required"] == ["sql"]

    def test_accepts_individual_tools_alongside_toolsets(self):
        tools = as_strands_tools(_tool("ok"), SQLToolset("pg_default"))

        assert [t.tool_name for t in tools][:2] == ["lookup", "list_tables"]

    def test_registering_does_not_mutate_the_source_schema(self):
        """Strands fills in schema gaps in place; the toolset's own schema must stay untouched."""
        source = _tool("ok")
        before = copy.deepcopy(source.parameters)

        registry = ToolRegistry()
        for tool in as_strands_tools(source):
            registry.register_tool(tool)
        registry.get_all_tool_specs()

        assert source.parameters == before

    def test_text_result_is_a_success(self):
        result = _run(as_strands_tools(_tool("42 rows"))[0], {"key": "a"})

        assert result == {"toolUseId": "tu-1", "status": "success", "content": [{"text": "42 rows"}]}

    def test_json_result_is_passed_as_json(self):
        result = _run(as_strands_tools(_tool({"rows": [[1]]}))[0], {"key": "a"})

        assert result["content"] == [{"json": {"rows": [[1]]}}]

    def test_error_result_maps_to_error_status(self):
        result = _run(as_strands_tools(_tool("Unknown column", is_error=True))[0], {"key": "a"})

        assert result["status"] == "error"
        assert result["content"] == [{"text": "Unknown column"}]

    def test_runs_the_toolset_through_the_strands_executor_path(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])
        query = next(t for t in as_strands_tools(ts) if t.tool_name == "query")

        result = _run(query, {"sql": "SELECT id, name FROM users"})

        assert result["status"] == "success"
        assert '"rows":[[1,"Ada"]]' in result["content"][0]["text"].replace(" ", "")
