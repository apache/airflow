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
import inspect
from unittest.mock import AsyncMock, MagicMock

from airflow.providers.common.ai.utils.toolset_bridge import (
    _build_docstring,
    _create_adk_wrapper,
    toolsets_to_adk_tools,
)


def _make_toolset(tools_dict):
    """Create a mock toolset with the given tools dictionary."""
    toolset = MagicMock()
    toolset.get_tools = AsyncMock(return_value=tools_dict)
    toolset.call_tool = AsyncMock(return_value={"status": "ok"})
    return toolset


def _make_ts_tool(name, description="", params=None, required=None):
    """Create a mock ToolsetTool with a definition."""
    ts_tool = MagicMock()
    schema = {}
    if params:
        schema["properties"] = params
    if required:
        schema["required"] = required
    ts_tool.definition.parameters_json_schema = schema
    ts_tool.definition.description = description
    return ts_tool


class TestBuildDocstring:
    def test_no_properties(self):
        result = _build_docstring("List all tables.", {})
        assert result == "List all tables."

    def test_with_properties(self):
        props = {
            "table_name": {"description": "Name of the table"},
            "limit": {"description": "Max rows to return"},
        }
        result = _build_docstring("Query a table.", props)
        assert "Query a table." in result
        assert "Args:" in result
        assert "    table_name: Name of the table" in result
        assert "    limit: Max rows to return" in result

    def test_property_without_description(self):
        props = {"column": {}}
        result = _build_docstring("Get column info.", props)
        assert "    column: " in result


class TestCreateAdkWrapper:
    def test_wrapper_has_correct_name(self):
        ts_tool = _make_ts_tool("list_tables", description="Lists tables.")
        toolset = _make_toolset({})
        wrapper = _create_adk_wrapper(toolset, "list_tables", ts_tool)

        assert wrapper.__name__ == "list_tables"
        assert wrapper.__qualname__ == "list_tables"

    def test_wrapper_has_docstring(self):
        ts_tool = _make_ts_tool("list_tables", description="Lists all tables in the database.")
        toolset = _make_toolset({})
        wrapper = _create_adk_wrapper(toolset, "list_tables", ts_tool)

        assert "Lists all tables in the database." in wrapper.__doc__

    def test_wrapper_has_dynamic_signature(self):
        ts_tool = _make_ts_tool(
            "query",
            description="Execute SQL.",
            params={
                "sql": {"description": "The SQL query"},
                "limit": {"description": "Max rows"},
            },
            required=["sql"],
        )
        toolset = _make_toolset({})
        wrapper = _create_adk_wrapper(toolset, "query", ts_tool)

        sig = inspect.signature(wrapper)
        assert "sql" in sig.parameters
        assert "limit" in sig.parameters
        assert sig.parameters["sql"].default is inspect.Parameter.empty
        assert sig.parameters["limit"].default is None

    def test_wrapper_is_async(self):
        ts_tool = _make_ts_tool("list_tables")
        toolset = _make_toolset({})
        wrapper = _create_adk_wrapper(toolset, "list_tables", ts_tool)

        assert asyncio.iscoroutinefunction(wrapper)

    def test_wrapper_calls_toolset(self):
        ts_tool = _make_ts_tool(
            "query",
            params={"sql": {"description": "SQL query"}},
            required=["sql"],
        )
        toolset = _make_toolset({})
        wrapper = _create_adk_wrapper(toolset, "query", ts_tool)

        result = asyncio.run(wrapper(sql="SELECT 1"))
        toolset.call_tool.assert_called_once_with("query", {"sql": "SELECT 1"}, None, ts_tool)
        assert result == {"status": "ok"}


class TestToolsetsToAdkTools:
    def test_converts_single_toolset(self):
        ts_tool = _make_ts_tool("list_tables", description="List tables.")
        toolset = _make_toolset({"list_tables": ts_tool})
        tools = toolsets_to_adk_tools([toolset])

        assert len(tools) == 1
        assert tools[0].__name__ == "list_tables"

    def test_converts_multiple_tools(self):
        tool1 = _make_ts_tool("list_tables", description="List tables.")
        tool2 = _make_ts_tool("query", description="Run a query.")
        toolset = _make_toolset({"list_tables": tool1, "query": tool2})
        tools = toolsets_to_adk_tools([toolset])

        assert len(tools) == 2
        names = {t.__name__ for t in tools}
        assert names == {"list_tables", "query"}

    def test_multiple_toolsets(self):
        tool1 = _make_ts_tool("list_tables")
        toolset1 = _make_toolset({"list_tables": tool1})
        tool2 = _make_ts_tool("get_hook_info")
        toolset2 = _make_toolset({"get_hook_info": tool2})

        tools = toolsets_to_adk_tools([toolset1, toolset2])
        assert len(tools) == 2

    def test_skips_toolset_on_error(self):
        """If a toolset's get_tools() fails, it is skipped gracefully."""
        bad_toolset = MagicMock()
        bad_toolset.get_tools = AsyncMock(side_effect=RuntimeError("fail"))

        good_tool = _make_ts_tool("list_tables")
        good_toolset = _make_toolset({"list_tables": good_tool})

        tools = toolsets_to_adk_tools([bad_toolset, good_toolset])
        assert len(tools) == 1
        assert tools[0].__name__ == "list_tables"

    def test_empty_toolsets(self):
        tools = toolsets_to_adk_tools([])
        assert tools == []
