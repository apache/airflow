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
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ApprovalRequired, CallDeferred, ModelRetry
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets.combined import CombinedToolset
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.tool_metrics import (
    calling_framework,
    record_tool_call,
)
from airflow.providers.common.ai.utils.toolset_base import MaskingToolset, with_masking

from unit.common.ai.operators.test_agent import _InMemoryDurableStorage
from unit.common.ai.toolsets.test_sql import _make_mock_db_hook
from unit.common.ai.utils.test_toolset_base import _call as _call_scripted, _ScriptedToolset


@pytest.fixture
def stats():
    with patch("airflow.providers.common.ai.utils.tool_metrics.Stats", MagicMock(spec=["incr"])) as mock:
        yield mock


def _tags(outcome: str, framework: str = "pydantic_ai", toolset: str = "SQLToolset") -> dict[str, str]:
    return {"toolset": toolset, "framework": framework, "outcome": outcome}


def _sql_toolset(**hook_kwargs) -> SQLToolset:
    ts = SQLToolset("pg_default")
    ts._hook = _make_mock_db_hook(**hook_kwargs)
    return ts


def _call(toolset, name: str, args: dict):
    async def run():
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
        tools = await toolset.get_tools(ctx)
        return await toolset.call_tool(name, args, ctx, tools[name])

    return asyncio.run(run())


class TestRecordToolCall:
    def test_a_call_outside_any_adapter_is_pydantic_ais(self, stats):
        record_tool_call("SQLToolset", "executed")

        stats.incr.assert_called_once_with("common_ai.tool_calls", tags=_tags("executed"))

    def test_an_adapter_names_its_framework_for_the_calls_inside(self, stats):
        with calling_framework("strands"):
            record_tool_call("SQLToolset", "executed")
        record_tool_call("SQLToolset", "executed")

        assert stats.incr.call_args_list == [
            call("common_ai.tool_calls", tags=_tags("executed", framework="strands")),
            call("common_ai.tool_calls", tags=_tags("executed")),
        ]


class TestToolsetsCountTheirCalls:
    def test_a_call_that_returns_is_executed(self, stats):
        _call(_sql_toolset(), "list_tables", {})

        stats.incr.assert_called_once_with("common_ai.tool_calls", tags=_tags("executed"))

    def test_a_call_that_raises_is_failed(self, stats):
        ts = _sql_toolset()
        ts._hook.run.side_effect = ConnectionError("down")

        with pytest.raises(ModelRetry):
            _call(ts, "query", {"sql": "SELECT 1"})

        stats.incr.assert_called_once_with("common_ai.tool_calls", tags=_tags("failed"))

    def test_a_call_through_the_neutral_interface_without_an_adapter_is_none(self, stats):
        tool = {t.name: t for t in _sql_toolset().airflow_tools()}["list_tables"]

        asyncio.run(tool.call({}))

        stats.incr.assert_called_once_with("common_ai.tool_calls", tags=_tags("executed", framework="none"))

    def test_a_toolset_the_dag_author_wrote_is_not_counted(self, stats):
        """The metric measures this provider's toolsets; the masking wrapper does not count."""

        def ping() -> str:
            return "pong"

        _call(MaskingToolset(wrapped=FunctionToolset([ping])), "ping", {})

        stats.incr.assert_not_called()

    def test_a_durable_replay_is_counted_as_replayed_not_executed(self, stats):
        storage = _InMemoryDurableStorage()
        for _ in range(2):
            cached = CachingToolset(
                wrapped=with_masking(_sql_toolset()), storage=storage, counter=DurableStepCounter()
            )
            ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage(), tool_call_id="c1")

            async def run(toolset=cached, ctx=ctx):
                tools = await toolset.get_tools(ctx)
                return await toolset.call_tool("list_tables", {}, ctx, tools["list_tables"])

            asyncio.run(run())

        assert [c.kwargs["tags"]["outcome"] for c in stats.incr.call_args_list] == ["executed", "replayed"]


class TestOutcomes:
    @pytest.mark.parametrize(
        ("raised", "outcomes"),
        [
            pytest.param(ApprovalRequired(), [], id="paused_for_approval"),
            pytest.param(CallDeferred(), [], id="deferred"),
            pytest.param(ModelRetry("fix it"), ["failed"], id="model_retry"),
            pytest.param(RuntimeError("boom"), ["failed"], id="error"),
        ],
    )
    def test_a_paused_call_is_not_counted_and_a_failed_one_is(self, stats, raised, outcomes):
        with pytest.raises(type(raised)):
            _call_scripted(_ScriptedToolset(raised))

        assert [c.kwargs["tags"]["outcome"] for c in stats.incr.call_args_list] == outcomes

    def test_a_replay_through_a_wrapper_counts_the_toolset_underneath(self, stats):
        storage = _InMemoryDurableStorage()
        for _ in range(2):
            wrapped = with_masking(_sql_toolset().prefixed("wh"))
            cached = CachingToolset(wrapped=wrapped, storage=storage, counter=DurableStepCounter())
            ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage(), tool_call_id="c1")

            async def run(toolset=cached, ctx=ctx):
                tools = await toolset.get_tools(ctx)
                return await toolset.call_tool("wh_list_tables", {}, ctx, tools["wh_list_tables"])

            asyncio.run(run())

        assert [c.kwargs["tags"]["outcome"] for c in stats.incr.call_args_list] == ["executed", "replayed"]

    def test_a_replay_inside_a_combined_toolset_counts_the_toolset_it_came_from(self, stats):
        storage = _InMemoryDurableStorage()
        for _ in range(2):
            combined = CombinedToolset([_sql_toolset(), FunctionToolset([])])
            cached = CachingToolset(wrapped=combined, storage=storage, counter=DurableStepCounter())
            ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage(), tool_call_id="c1")

            async def run(toolset=cached, ctx=ctx):
                tools = await toolset.get_tools(ctx)
                return await toolset.call_tool("list_tables", {}, ctx, tools["list_tables"])

            asyncio.run(run())

        assert [c.kwargs["tags"]["outcome"] for c in stats.incr.call_args_list] == ["executed", "replayed"]
