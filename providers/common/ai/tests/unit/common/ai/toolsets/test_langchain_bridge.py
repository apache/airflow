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
import sys
from typing import Any

import pytest

pytest.importorskip("langchain_core")

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

from airflow.providers.common.ai.toolsets.langchain_bridge import airflow_toolset_to_langchain_tools

_PASSTHROUGH = SchemaValidator(core_schema.any_schema())
# Coerces the ``n`` field to int so we can assert the args_validator runs.
_INT_VALIDATOR = SchemaValidator(
    core_schema.typed_dict_schema({"n": core_schema.typed_dict_field(core_schema.int_schema())})
)

_ECHO_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"text": {"type": "string", "description": "Text to echo."}},
    "required": ["text"],
}
_ADD_ONE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"n": {"type": "integer", "description": "A number."}},
    "required": ["n"],
}
_BOOM_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}


class FakeToolset(AbstractToolset[None]):
    """Minimal toolset with two tools, recording the args each tool is called with."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    @property
    def id(self) -> str:
        return "fake"

    async def get_tools(self, ctx) -> dict[str, ToolsetTool[None]]:
        return {
            "echo": ToolsetTool(
                toolset=self,
                tool_def=ToolDefinition(
                    name="echo", description="Echo the text back.", parameters_json_schema=_ECHO_SCHEMA
                ),
                max_retries=1,
                args_validator=_PASSTHROUGH,
            ),
            "add_one": ToolsetTool(
                toolset=self,
                tool_def=ToolDefinition(
                    name="add_one", description="Add one to n.", parameters_json_schema=_ADD_ONE_SCHEMA
                ),
                max_retries=1,
                args_validator=_INT_VALIDATOR,
            ),
            "boom": ToolsetTool(
                toolset=self,
                tool_def=ToolDefinition(
                    name="boom",
                    description="Always asks the model to retry.",
                    parameters_json_schema=_BOOM_SCHEMA,
                ),
                max_retries=1,
                args_validator=_PASSTHROUGH,
            ),
        }

    async def call_tool(self, name, tool_args, ctx, tool) -> Any:
        self.calls.append((name, tool_args))
        if name == "echo":
            return f"echo: {tool_args['text']}"
        if name == "add_one":
            return tool_args["n"] + 1
        if name == "boom":
            raise ModelRetry("fix your input and try again")
        raise ValueError(name)


class TestAirflowToolsetToLangChainTools:
    def test_returns_one_tool_per_toolset_tool(self):
        tools = airflow_toolset_to_langchain_tools(FakeToolset())

        assert {t.name for t in tools} == {"echo", "add_one", "boom"}

    def test_carries_description_and_args_schema(self):
        tools = {t.name: t for t in airflow_toolset_to_langchain_tools(FakeToolset())}

        echo = tools["echo"]
        assert echo.description == "Echo the text back."
        # ``args`` is derived from the tool's parameters_json_schema.
        assert "text" in echo.args
        assert echo.args["text"]["type"] == "string"

    def test_sync_invoke_calls_through_to_toolset(self):
        toolset = FakeToolset()
        echo = {t.name: t for t in airflow_toolset_to_langchain_tools(toolset)}["echo"]

        result = echo.invoke({"text": "hi"})

        assert result == "echo: hi"
        assert toolset.calls == [("echo", {"text": "hi"})]

    def test_args_validator_coerces_before_call(self):
        toolset = FakeToolset()
        add_one = {t.name: t for t in airflow_toolset_to_langchain_tools(toolset)}["add_one"]

        # LangChain passes the raw value through; the toolset's validator coerces
        # the string "5" to the int 5 before call_tool sees it.
        result = add_one.invoke({"n": "5"})

        assert result == 6
        assert toolset.calls == [("add_one", {"n": 5})]

    def test_async_invoke_calls_through_to_toolset(self):
        toolset = FakeToolset()
        echo = {t.name: t for t in airflow_toolset_to_langchain_tools(toolset)}["echo"]

        result = asyncio.run(echo.ainvoke({"text": "yo"}))

        assert result == "echo: yo"
        assert toolset.calls == [("echo", {"text": "yo"})]

    def test_model_retry_returned_as_tool_output_sync(self):
        # ModelRetry is a "retry with this guidance" signal, not a failure: the
        # bridge returns the message as the tool output so the model self-corrects
        # rather than aborting the agent run.
        boom = {t.name: t for t in airflow_toolset_to_langchain_tools(FakeToolset())}["boom"]

        assert boom.invoke({}) == "fix your input and try again"

    def test_repeated_model_retry_propagates_then_resets(self):
        # A tool that keeps raising ModelRetry must not loop forever: once the tool's
        # max_retries (1 here) is exhausted, the error propagates so the run fails
        # instead of the bridge feeding the message back indefinitely. The budget then
        # resets so a reused tool is not poisoned for the next run.
        boom = {t.name: t for t in airflow_toolset_to_langchain_tools(FakeToolset())}["boom"]

        assert boom.invoke({}) == "fix your input and try again"  # fed back
        with pytest.raises(ModelRetry, match="fix your input"):  # budget exhausted -> propagates
            boom.invoke({})
        assert boom.invoke({}) == "fix your input and try again"  # reset -> fed back again

    def test_model_retry_count_resets_after_success(self):
        # The retry budget resets on a successful call: fail (fed back), succeed
        # (reset), fail again (fed back rather than immediately propagating).
        class FlakyToolset(FakeToolset):
            def __init__(self) -> None:
                super().__init__()
                self._outcomes = iter([ModelRetry("retry 1"), "ok", ModelRetry("retry 2")])

            async def call_tool(self, name, tool_args, ctx, tool) -> Any:
                if name == "boom":
                    outcome = next(self._outcomes)
                    if isinstance(outcome, ModelRetry):
                        raise outcome
                    return outcome
                return await super().call_tool(name, tool_args, ctx, tool)

        boom = {t.name: t for t in airflow_toolset_to_langchain_tools(FlakyToolset())}["boom"]

        assert boom.invoke({}) == "retry 1"  # count 0 -> 1, returned
        assert boom.invoke({}) == "ok"  # success, count reset to 0
        assert boom.invoke({}) == "retry 2"  # count 0 -> 1 again, returned (not propagated)

    def test_model_retry_returned_as_tool_output_async(self):
        boom = {t.name: t for t in airflow_toolset_to_langchain_tools(FakeToolset())}["boom"]

        assert asyncio.run(boom.ainvoke({})) == "fix your input and try again"

    def test_deps_are_exposed_on_the_run_context(self):
        sentinel = object()
        captured: dict[str, Any] = {}

        class DepsToolset(FakeToolset):
            async def get_tools(self, ctx) -> dict[str, ToolsetTool[None]]:
                captured["deps"] = ctx.deps
                return await super().get_tools(ctx)

        airflow_toolset_to_langchain_tools(DepsToolset(), deps=sentinel)

        assert captured["deps"] is sentinel

    def test_missing_langchain_raises_optional_feature_exception(self, monkeypatch):
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        # Setting the module to None in sys.modules makes the lazy import inside
        # the bridge raise ImportError, exercising the optional-dependency path.
        monkeypatch.setitem(sys.modules, "langchain_core.tools", None)

        with pytest.raises(AirflowOptionalProviderFeatureException):
            airflow_toolset_to_langchain_tools(FakeToolset())


class TestSQLToolsetConversion:
    def test_sql_toolset_exposes_its_four_tools(self):
        # get_tools / construction do not touch the database, so no connection
        # is needed to enumerate the tools.
        sql = pytest.importorskip("airflow.providers.common.ai.toolsets.sql")

        tools = airflow_toolset_to_langchain_tools(sql.SQLToolset(db_conn_id="db"))

        assert {t.name for t in tools} == {"list_tables", "get_schema", "query", "check_query"}
