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
import traceback
from typing import Any

import pytest

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult, collect_tools


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
        with pytest.raises(ValueError, match="More than one tool is named query"):
            collect_tools([_Provider(_named("query")), _Provider(_named("query"))])


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
