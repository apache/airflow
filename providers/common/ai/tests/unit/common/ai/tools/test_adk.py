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
import json
from typing import Any

import pytest

pytest.importorskip("google.adk")

from google.adk.agents import LlmAgent
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.adk.runners import InMemoryRunner
from google.genai import types

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.tools.adk import AirflowTools
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from unit.common.ai.toolsets.test_sql import _make_mock_db_hook


class _OneToolCallLlm(BaseLlm):
    """Asks for one call of ``tool_name``, then answers with the JSON of the response it got back."""

    model: str = "scripted"
    tool_name: str
    tool_args: dict[str, Any]

    async def generate_content_async(self, llm_request, stream: bool = False):
        responses = [
            part.function_response
            for content in llm_request.contents
            for part in content.parts or ()
            if part.function_response
        ]
        if responses:
            part = types.Part(text=json.dumps(responses[-1].response))
        else:
            part = types.Part(
                function_call=types.FunctionCall(id="call-1", name=self.tool_name, args=self.tool_args)
            )
        yield LlmResponse(content=types.Content(role="model", parts=[part]))


def _tool(result: ToolResult | Exception) -> AirflowTool:
    async def function(arguments: dict[str, Any]) -> ToolResult:
        if isinstance(result, Exception):
            raise result
        return result

    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]},
        function=function,
    )


def _run_agent(toolset: AirflowTools, tool_name: str, tool_args: dict[str, Any]) -> dict[str, Any]:
    agent = LlmAgent(
        name="analyst", model=_OneToolCallLlm(tool_name=tool_name, tool_args=tool_args), tools=[toolset]
    )

    async def run() -> str:
        runner = InMemoryRunner(agent=agent, app_name="test")
        session = await runner.session_service.create_session(app_name="test", user_id="user")
        message = types.Content(role="user", parts=[types.Part(text="go")])
        texts = [
            part.text
            async for event in runner.run_async(user_id="user", session_id=session.id, new_message=message)
            if event.content
            for part in event.content.parts or ()
            if part.text
        ]
        return texts[-1]

    return json.loads(asyncio.run(run()))


class TestAirflowTools:
    def test_adds_every_tool_of_a_toolset(self):
        tools = asyncio.run(AirflowTools(SQLToolset("pg_default")).get_tools())

        assert [t.name for t in tools] == ["list_tables", "get_schema", "query", "check_query"]
        declaration = next(t for t in tools if t.name == "query")._get_declaration()
        assert declaration.parameters_json_schema["required"] == ["sql"]

    def test_the_declaration_does_not_share_the_source_schema(self):
        source = _tool(ToolResult("ok"))
        before = copy.deepcopy(source.parameters)

        declaration = asyncio.run(AirflowTools(source).get_tools())[0]._get_declaration()
        declaration.parameters_json_schema["properties"]["key"]["description"] = "changed"

        assert source.parameters == before

    def test_marks_an_error_response_for_adk_telemetry(self):
        tool = asyncio.run(AirflowTools(_tool(ToolResult("ok"))).get_tools())[0]

        assert tool._detect_error_in_response({"error": "Unknown column"}) == "TOOL_ERROR"
        assert tool._detect_error_in_response({"result": "ok"}) is None


class TestAgentRun:
    def test_the_agent_reads_a_toolset_result(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])

        response = _run_agent(AirflowTools(ts), "query", {"sql": "SELECT id, name FROM users"})

        assert json.loads(response["result"])["rows"] == [[1, "Ada"]]

    def test_a_correctable_failure_reaches_the_model_as_an_error(self):
        toolset = AirflowTools(_tool(ToolResult("Unknown column 'nme'", is_error=True)))

        assert _run_agent(toolset, "lookup", {"key": "a"}) == {"error": "Unknown column 'nme'"}

    def test_any_other_failure_fails_the_run(self):
        toolset = AirflowTools(_tool(PermissionError("role cannot read orders")))

        with pytest.raises(ToolCallError, match="PermissionError: role cannot read orders"):
            _run_agent(toolset, "lookup", {"key": "a"})

    @pytest.mark.enable_redact
    def test_a_secret_in_a_result_reaches_the_model_masked(self, registered_secret):
        toolset = AirflowTools(_tool(ToolResult(f"key={registered_secret}")))

        assert _run_agent(toolset, "lookup", {"key": "a"}) == {"result": "key=***"}
