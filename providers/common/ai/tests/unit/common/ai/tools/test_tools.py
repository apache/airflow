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
import traceback
from functools import partial
from typing import TYPE_CHECKING, Any

import pytest
from pydantic_ai.exceptions import UserError
from pydantic_ai.toolsets.filtered import FilteredToolset
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset

from airflow.providers.common.ai.exceptions import MCPToolsetNotSupportedError
from airflow.providers.common.ai.tools import (
    AirflowTool,
    ToolCallError,
    ToolResult,
    _collect_tools,
    collect_tools,
)
from airflow.providers.common.ai.toolsets.mcp import MCPToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from unit.common.ai.toolsets.test_sql import _make_mock_db_hook

if TYPE_CHECKING:
    from pydantic_ai import RunContext
    from pydantic_ai.tools import ToolDefinition


def _tool(function) -> AirflowTool:
    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}},
        function=function,
    )


class TestAirflowToolCall:
    def test_passes_arguments_and_returns_the_result(self):
        seen: list[dict[str, Any]] = []

        async def function(arguments: dict[str, Any]) -> ToolResult:
            seen.append(arguments)
            return ToolResult(content={"rows": [[1, "Ada"]]})

        result = asyncio.run(_tool(function).call({"key": "a"}))

        assert seen == [{"key": "a"}]
        assert result == ToolResult(content={"rows": [[1, "Ada"]]})

    def test_an_error_result_keeps_its_status(self):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            return ToolResult(content="Unknown column 'nme'", is_error=True)

        result = asyncio.run(_tool(function).call({}))

        assert result == ToolResult(content="Unknown column 'nme'", is_error=True)

    def test_an_exception_fails_the_call(self):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            raise PermissionError("role cannot read orders")

        with pytest.raises(ToolCallError, match="lookup failed: PermissionError: role cannot read orders"):
            asyncio.run(_tool(function).call({}))

    def test_a_tool_call_error_keeps_its_message(self):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            raise ToolCallError("query kept failing after 1 correction(s)")

        with pytest.raises(ToolCallError) as caught:
            asyncio.run(_tool(function).call({}))

        assert str(caught.value) == "query kept failing after 1 correction(s)"

    def test_the_failure_is_raised_without_the_original_attached(self):
        """Frameworks record a failed call's exception, context included, in their traces."""

        async def function(arguments: dict[str, Any]) -> ToolResult:
            raise PermissionError("role cannot read orders")

        with pytest.raises(ToolCallError) as caught:
            asyncio.run(_tool(function).call({}))

        assert caught.value.__context__ is None


class _Provider:
    def __init__(self, *tools: AirflowTool) -> None:
        self._tools = list(tools)

    def airflow_tools(self) -> list[AirflowTool]:
        return self._tools


def _named(name: str) -> AirflowTool:
    async def function(arguments: dict[str, Any]) -> ToolResult:
        return ToolResult(content=name)

    return AirflowTool(name=name, description=name, parameters={"type": "object"}, function=function)


class TestCollectTools:
    def test_flattens_toolsets_and_tools_in_order(self):
        tools = collect_tools([_Provider(_named("a"), _named("b")), _named("c")])

        assert [tool.name for tool in tools] == ["a", "b", "c"]

    def test_refuses_two_tools_with_the_same_name(self):
        """Frameworks route a call by name, so one of the two would never run."""
        with pytest.raises(
            ValueError,
            match="More than one tool is named query. .*where they take one, or wrap them with .prefixed\\(\\), so",
        ):
            collect_tools([_Provider(_named("query")), _Provider(_named("query"))])

    @pytest.mark.parametrize(
        ("collect", "advice"),
        [
            pytest.param(
                collect_tools, "Connect your framework's own MCP client to the server instead.", id="public"
            ),
            pytest.param(
                partial(_collect_tools, mcp_hint="Connect Acme's McpClient instead."),
                "Connect Acme's McpClient instead.",
                id="adapter-hint",
            ),
        ],
    )
    def test_an_mcp_toolset_error_says_what_to_connect_instead(self, collect, advice):
        with pytest.raises(MCPToolsetNotSupportedError) as exc_info:
            collect([MCPToolset("mcp_default")])

        assert str(exc_info.value) == (
            "MCPToolset works in Pydantic AI agents and through the LangChain bridge, not through "
            f"AirflowTools. {advice}"
        )

    def test_other_not_implemented_errors_pass_through_unchanged(self):
        """The hint is MCP-specific; another provider's NotImplementedError keeps its own message."""

        class _Unfinished:
            def airflow_tools(self) -> list[AirflowTool]:
                raise NotImplementedError("not yet")

        with pytest.raises(NotImplementedError, match="^not yet$"):
            _collect_tools([_Unfinished()], mcp_hint="Connect Acme's McpClient instead.")


def _build_sql_toolset(name: str = "Ada") -> SQLToolset:
    ts = SQLToolset("pg_default")
    ts._hook = _make_mock_db_hook(records=[(1, name)], last_description=[("id",), ("name",)])
    return ts


def _extract_names(tools: list[AirflowTool]) -> list[str]:
    return [tool.name for tool in tools]


def _tool_name_is_not_query(ctx: RunContext[Any], tool: ToolDefinition) -> bool:
    return tool.name != "query"


async def _tool_name_is_not_query_async(ctx: RunContext[Any], tool: ToolDefinition) -> bool:
    return tool.name != "query"


class TestCollectToolsThroughWrappers:
    @pytest.mark.parametrize(
        "keep", [_tool_name_is_not_query, _tool_name_is_not_query_async], ids=["sync", "async"]
    )
    def test_a_filter_drops_the_tools_it_rejects(self, keep):
        tools = collect_tools([_build_sql_toolset().filtered(keep)])

        assert _extract_names(tools) == ["list_tables", "get_schema", "check_query"]

    def test_the_filter_runs_once_with_each_tools_definition(self):
        seen: list[tuple[RunContext[Any], ToolDefinition]] = []

        def keep_all(ctx: RunContext[Any], tool: ToolDefinition) -> bool:
            seen.append((ctx, tool))
            return True

        tools = collect_tools([_build_sql_toolset().filtered(keep_all)])
        asyncio.run({tool.name: tool for tool in tools}["query"].call({"sql": "SELECT 1"}))

        assert [tool.name for _, tool in seen] == ["list_tables", "get_schema", "query", "check_query"]
        # The toolset's own definitions, not ones rebuilt from the AirflowTool.
        assert all(tool.sequential for _, tool in seen)
        assert all(ctx.deps is None for ctx, _ in seen)

    @pytest.mark.parametrize(
        ("wrap", "offered", "query_tool"),
        [
            pytest.param(
                lambda ts: ts.prefixed("wh"),
                ["wh_list_tables", "wh_get_schema", "wh_query", "wh_check_query"],
                "wh_query",
                id="prefixed",
            ),
            pytest.param(
                lambda ts: ts.renamed({"run_sql": "query"}),
                ["list_tables", "get_schema", "run_sql", "check_query"],
                "run_sql",
                id="renamed",
            ),
        ],
    )
    def test_a_renamed_tool_runs_the_inner_tool(self, wrap, offered, query_tool):
        tools = {tool.name: tool for tool in collect_tools([wrap(_build_sql_toolset())])}

        result = asyncio.run(tools[query_tool].call({"sql": "SELECT id, name FROM users"}))

        assert list(tools) == offered
        assert json.loads(result.content)["rows"] == [[1, "Ada"]]

    def test_nested_wrappers_apply_from_the_inside_out(self):
        toolset = (
            _build_sql_toolset()
            .prefixed("wh")
            .filtered(lambda ctx, tool: tool.name != "wh_get_schema")
            .renamed({"run_sql": "wh_query"})
            .filtered(lambda ctx, tool: tool.name != "wh_check_query")
        )

        tools = {tool.name: tool for tool in collect_tools([toolset])}
        result = asyncio.run(tools["run_sql"].call({"sql": "SELECT id, name FROM users"}))

        assert list(tools) == ["wh_list_tables", "run_sql"]
        assert json.loads(result.content)["rows"] == [[1, "Ada"]]

    @pytest.mark.parametrize(
        ("wrap", "offered", "query_tool"),
        [
            pytest.param(
                lambda ts: ts.renamed({"run_sql": "query"}).prefixed("wh"),
                ["wh_list_tables", "wh_get_schema", "wh_run_sql", "wh_check_query"],
                "wh_run_sql",
                id="renamed-inside-prefixed",
            ),
            pytest.param(
                lambda ts: ts.renamed({"wh_query": "query"}).prefixed("wh"),
                ["wh_list_tables", "wh_get_schema", "wh_wh_query", "wh_check_query"],
                "wh_wh_query",
                id="name-already-carries-the-prefix",
            ),
        ],
    )
    def test_other_nesting_orders_run_the_inner_tool(self, wrap, offered, query_tool):
        tools = {tool.name: tool for tool in collect_tools([wrap(_build_sql_toolset())])}

        result = asyncio.run(tools[query_tool].call({"sql": "SELECT id, name FROM users"}))

        assert list(tools) == offered
        assert json.loads(result.content)["rows"] == [[1, "Ada"]]

    def test_a_filter_that_rejects_every_tool_leaves_none(self):
        assert collect_tools([_build_sql_toolset().filtered(lambda ctx, tool: False)]) == []

    def test_a_rename_onto_an_existing_tool_name_is_refused(self):
        """pydantic-ai's RenamedToolset raises before any tool would be shadowed."""
        with pytest.raises(UserError, match="conflicts with"):
            collect_tools([_build_sql_toolset().renamed({"check_query": "query"})])

    def test_two_prefixes_keep_two_toolsets_on_the_same_tools_apart(self):
        tools = collect_tools(
            [_build_sql_toolset("Ada").prefixed("a"), _build_sql_toolset("Grace").prefixed("b")]
        )
        by_name = {tool.name: tool for tool in tools}

        rows = {
            name: json.loads(asyncio.run(by_name[name].call({"sql": "SELECT id, name FROM users"})).content)[
                "rows"
            ]
            for name in ("a_query", "b_query")
        }

        assert _extract_names(tools) == [
            *(f"a_{name}" for name in ("list_tables", "get_schema", "query", "check_query")),
            *(f"b_{name}" for name in ("list_tables", "get_schema", "query", "check_query")),
        ]
        assert rows == {"a_query": [[1, "Ada"]], "b_query": [[1, "Grace"]]}

    def test_a_rename_map_written_backwards_says_which_way_it_goes(self):
        with pytest.raises(
            ValueError,
            match=r"The tool offered as 'query' maps back to 'run_sql'.*\.renamed\(\{'run_sql': 'query'\}\)",
        ):
            collect_tools([_build_sql_toolset().renamed({"query": "run_sql"})])

    def test_a_wrapper_that_supplies_its_own_tools_is_asked_for_them(self):
        """A guard, not a regression test: it passes before this change too, and must keep passing."""

        class _OwnTools(WrapperToolset[Any]):
            def airflow_tools(self) -> list[AirflowTool]:
                return [_named("own")]

        assert _extract_names(collect_tools([_OwnTools(_build_sql_toolset())])) == ["own"]

    def test_a_subclass_of_a_naming_wrapper_is_refused(self):
        """Its call_tool could add behaviour that tools reached around it would skip."""

        class _AuditedFilter(FilteredToolset[Any]):
            pass

        with pytest.raises(TypeError, match="cannot read through _AuditedFilter"):
            collect_tools([_AuditedFilter(_build_sql_toolset(), lambda ctx, tool: True)])

    def test_an_mcp_toolset_behind_a_filter_says_what_to_connect_instead(self):
        with pytest.raises(MCPToolsetNotSupportedError, match="Connect Acme's McpClient instead."):
            _collect_tools(
                [MCPToolset("mcp_default").filtered(lambda ctx, tool: True)],
                mcp_hint="Connect Acme's McpClient instead.",
            )

    @pytest.mark.parametrize(
        "wrap",
        [
            pytest.param(lambda ts: ts.approval_required(), id="outermost"),
            pytest.param(lambda ts: ts.approval_required().filtered(lambda ctx, tool: True), id="nested"),
        ],
    )
    def test_approval_is_refused_and_points_at_the_frameworks_own(self, wrap):
        with pytest.raises(TypeError) as exc_info:
            collect_tools([wrap(_build_sql_toolset())])

        assert str(exc_info.value) == (
            "AirflowTools cannot carry ApprovalRequiredToolset (.approval_required()) to another "
            "framework's agent: only a Pydantic AI run pauses for the approval it asks for. Pass the "
            "toolset without it and use the framework's own tool approval, such as needs_approval "
            "in the OpenAI Agents SDK, whose guide says how it behaves in a task, or run the agent "
            "with AgentOperator."
        )

    def test_another_wrapper_is_refused_by_name(self):
        async def unchanged(ctx: RunContext[Any], tools: list[ToolDefinition]) -> list[ToolDefinition]:
            return tools

        with pytest.raises(TypeError) as exc_info:
            collect_tools([_build_sql_toolset().prepared(unchanged)])

        assert str(exc_info.value) == (
            "AirflowTools cannot read through PreparedToolset. Only .filtered(), .prefixed() and "
            ".renamed() carry over to another framework's agent; pass the toolset without it."
        )

    @pytest.mark.parametrize(
        "wrap",
        [
            pytest.param(lambda ts: ts, id="bare"),
            pytest.param(lambda ts: ts.filtered(lambda ctx, tool: True), id="filtered"),
        ],
    )
    def test_a_toolset_without_airflow_tools_is_refused(self, wrap):
        with pytest.raises(TypeError, match="cannot take a FunctionToolset: it has no airflow_tools"):
            collect_tools([wrap(FunctionToolset())])


@pytest.mark.enable_redact
class TestAirflowToolMasking:
    def test_masks_a_secret_in_a_text_result(self, registered_secret):
        async def function(arguments: dict[str, Any]) -> ToolResult:
            return ToolResult(content=f"token={registered_secret}")

        result = asyncio.run(_tool(function).call({}))

        assert result.content == "token=***"

    def test_masks_a_secret_nested_deep_in_a_json_result(self, registered_secret):
        payload = {"items": [{"spec": {"containers": [{"env": [{"value": f"pw={registered_secret}"}]}]}}]}

        async def function(arguments: dict[str, Any]) -> ToolResult:
            return ToolResult(content=payload)

        result = asyncio.run(_tool(function).call({}))

        assert result.content == {"items": [{"spec": {"containers": [{"env": [{"value": "pw=***"}]}]}}]}

    def test_a_failed_call_carries_only_the_masked_message(self, registered_secret):
        """Frameworks record a failed call's exception, cause included, in their traces."""

        async def function(arguments: dict[str, Any]) -> ToolResult:
            raise RuntimeError(f"GET https://svc:{registered_secret}@crm.example.com returned 401")

        with pytest.raises(ToolCallError) as caught:
            asyncio.run(_tool(function).call({}))

        assert str(caught.value) == (
            "lookup failed: RuntimeError: GET https://svc:***@crm.example.com returned 401"
        )
        assert registered_secret not in "".join(traceback.format_exception(caught.value))
