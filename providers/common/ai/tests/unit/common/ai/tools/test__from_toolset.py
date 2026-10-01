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
"""``airflow_tools()``: the bundled toolsets behave as they do inside a pydantic-ai run."""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import threading
import time

import pytest
from pydantic import ValidationError
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ModelRetry, ToolFailed
from pydantic_ai.models.test import TestModel
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.tools import ToolCallError, ToolResult
from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset, tool_call_scope
from airflow.providers.common.ai.toolsets.hook import HookToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from unit.common.ai.toolsets.test_sql import _make_mock_db_hook


def _by_name(tools) -> dict:
    return {tool.name: tool for tool in tools}


def _scripted(*outcomes, max_retries: int | None = 1):
    """A toolset with one tool, ``step``, that returns or raises each outcome in turn."""
    remaining = iter(outcomes)

    def step() -> str:
        """Take the next step."""
        outcome = next(remaining)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    return _by_name(airflow_tools_from_toolset(FunctionToolset([step], max_retries=max_retries)))["step"]


class TestSQLToolsetAirflowTools:
    def test_exposes_the_same_tools_as_the_pydantic_ai_path(self):
        ts = SQLToolset("pg_default")
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
        pydantic_tools = asyncio.run(ts.get_tools(ctx))

        tools = _by_name(ts.airflow_tools())

        assert tools.keys() == pydantic_tools.keys()
        for name, tool in tools.items():
            assert tool.parameters == pydantic_tools[name].tool_def.parameters_json_schema
            assert tool.description == pydantic_tools[name].tool_def.description

    def test_query_returns_rows(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])

        result = asyncio.run(
            _by_name(ts.airflow_tools())["query"].call({"sql": "SELECT id, name FROM users"})
        )

        assert not result.is_error
        assert json.loads(result.content)["rows"] == [[1, "Ada"]]

    def test_a_blocked_statement_is_an_error_the_model_can_read(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(_by_name(ts.airflow_tools())["query"].call({"sql": "DROP TABLE users"}))

        assert result.is_error
        assert "The query tool failed" in result.content
        ts._hook.run.assert_not_called()

    def test_a_missing_argument_is_an_error_the_model_can_read(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(_by_name(ts.airflow_tools())["query"].call({}))

        assert result.is_error
        assert "sql" in result.content
        ts._hook.run.assert_not_called()

    def test_a_database_that_keeps_failing_fails_the_call(self):
        """Bad credentials are not the model's to fix; the tool's retry budget ends the loop."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.run.side_effect = ConnectionError("authentication failed")
        query = _by_name(ts.airflow_tools())["query"]

        first = asyncio.run(query.call({"sql": "SELECT 1"}))
        assert first.is_error

        with pytest.raises(ToolCallError, match="query kept failing after 1 correction"):
            asyncio.run(query.call({"sql": "SELECT 1"}))

    @pytest.mark.enable_redact
    def test_a_database_error_carrying_a_secret_is_masked(self, registered_secret):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.run.side_effect = RuntimeError(
            f"could not connect to postgresql://svc:{registered_secret}@db"
        )

        result = asyncio.run(_by_name(ts.airflow_tools())["query"].call({"sql": "SELECT 1"}))

        assert result.is_error
        assert registered_secret not in result.content
        assert "postgresql://svc:***@db" in result.content

    def test_calls_through_separate_tool_lists_never_overlap(self):
        """Two agents sharing a toolset must still take turns on its one hook."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])
        run = ts._hook.run.side_effect
        active, peak = 0, 0
        counter = threading.Lock()

        def slow_run(*args, **kwargs):
            nonlocal active, peak
            with counter:
                active += 1
                peak = max(peak, active)
            time.sleep(0.05)
            with counter:
                active -= 1
            return run(*args, **kwargs)

        ts._hook.run.side_effect = slow_run
        first, second = _by_name(ts.airflow_tools())["query"], _by_name(ts.airflow_tools())["query"]

        async def both():
            return await asyncio.gather(
                first.call({"sql": "SELECT id, name FROM users"}),
                second.call({"sql": "SELECT id, name FROM users"}),
            )

        results = asyncio.run(both())

        assert [r.is_error for r in results] == [False, False]
        assert peak == 1


class TestRetryBudget:
    def test_a_success_resets_the_budget(self):
        step = _scripted(ModelRetry("retry 1"), "ok", ModelRetry("retry 2"))

        results = [asyncio.run(step.call({})) for _ in range(3)]

        assert [r.is_error for r in results] == [True, False, True]

    def test_failures_of_calls_made_together_count_once(self):
        """Frameworks run a turn's tool calls concurrently; pydantic-ai counts one failure per turn."""
        gate = threading.Event()

        def step() -> str:
            """Fail slowly, so the two calls overlap."""
            gate.wait(1)
            raise ModelRetry("bad input")

        tool = airflow_tools_from_toolset(FunctionToolset([step], max_retries=1))[0]

        async def two_at_once():
            calls = [asyncio.create_task(tool.call({})) for _ in range(2)]
            await asyncio.sleep(0.05)
            gate.set()
            return await asyncio.gather(*calls)

        results = asyncio.run(two_at_once())

        assert [r.is_error for r in results] == [True, True]

    def test_invalid_arguments_in_calls_made_together_count_once(self):
        """Validation fails without awaiting, so these calls do not overlap in time on their own."""

        def lookup(key: str) -> str:
            """Look a key up."""
            return key

        tool = airflow_tools_from_toolset(FunctionToolset([lookup], max_retries=1))[0]

        async def turn():
            return await asyncio.gather(tool.call({}), tool.call({}))

        results = asyncio.run(turn())
        assert [r.is_error for r in results] == [True, True]
        with pytest.raises(ToolCallError, match="kept failing"):
            asyncio.run(turn())

    def test_a_toolset_without_its_own_budget_gets_one_correction(self):
        """With no agent to take a budget from, a tool whose toolset follows the run gets
        pydantic-ai's default of one correction."""
        step = _scripted(ModelRetry("bad"), ModelRetry("bad"), max_retries=None)

        async def call_in(turn: str) -> ToolResult:
            with tool_call_scope(run="run", turn=turn):
                return await step.call({})

        assert asyncio.run(call_in("turn-1")).is_error
        with pytest.raises(ToolCallError, match="after 1 correction"):
            asyncio.run(call_in("turn-2"))

    def test_last_attempt_means_what_it_does_in_a_pydantic_ai_run(self):
        """Each call sees its own retry count, so a tool can fall back on its last attempt."""

        def lookup(ctx: RunContext[None]) -> str:
            """Look the answer up."""
            if ctx.last_attempt:
                return "fallback"
            raise ModelRetry("try again")

        tool = airflow_tools_from_toolset(FunctionToolset([lookup], max_retries=1))[0]

        async def call_in(turn: str) -> ToolResult:
            with tool_call_scope(run="run", turn=turn):
                return await tool.call({})

        assert asyncio.run(call_in("turn-1")).is_error
        assert asyncio.run(call_in("turn-2")).content == "fallback"

    def test_a_new_run_starts_with_a_fresh_budget(self):
        """An agent reused for a second run gets its full budget again, as in pydantic-ai."""
        step = _scripted(ModelRetry("bad"), ModelRetry("bad"), max_retries=1)

        async def call_in(run: str) -> ToolResult:
            with tool_call_scope(run=run, turn=f"{run}-turn-1"):
                return await step.call({})

        assert asyncio.run(call_in("run-1")).is_error
        assert asyncio.run(call_in("run-2")).is_error

    def test_failures_in_one_turn_count_once_even_one_after_another(self):
        """A framework that runs a turn's calls in sequence still counts that turn once."""
        step = _scripted(*[ModelRetry("bad")] * 4, max_retries=1)

        async def call_in(turn: str) -> ToolResult:
            with tool_call_scope(run="run", turn=turn):
                return await step.call({})

        for _ in range(3):
            assert asyncio.run(call_in("turn-1")).is_error
        with pytest.raises(ToolCallError, match="kept failing"):
            asyncio.run(call_in("turn-2"))

    def test_a_success_does_not_clear_a_failure_of_the_same_turn(self):
        step = _scripted(ModelRetry("bad"), "ok", ModelRetry("bad"), max_retries=1)

        async def call_in(turn: str) -> ToolResult:
            with tool_call_scope(run="run", turn=turn):
                return await step.call({})

        assert asyncio.run(call_in("turn-1")).is_error
        assert not asyncio.run(call_in("turn-1")).is_error
        with pytest.raises(ToolCallError, match="kept failing"):
            asyncio.run(call_in("turn-2"))

    def test_each_tool_list_starts_with_a_fresh_budget(self):
        toolset = FunctionToolset([_failing_step], max_retries=0)

        for _ in range(2):
            tool = airflow_tools_from_toolset(toolset)[0]
            with pytest.raises(ToolCallError):
                asyncio.run(tool.call({}))

    def test_a_tool_failure_is_reported_without_using_the_budget(self):
        step = _scripted(ToolFailed("no such file"), ToolFailed("no such file"), max_retries=0)

        results = [asyncio.run(step.call({})) for _ in range(2)]

        assert results == [ToolResult(content="no such file", is_error=True)] * 2

    def test_a_validation_error_raised_by_the_tool_itself_propagates(self):
        """The call may already have had a side effect, so the model is not invited to retry it."""
        step = _scripted(ValidationError.from_exception_data("Response", []))

        with pytest.raises(ToolCallError, match="ValidationError"):
            asyncio.run(step.call({}))


class TestSequentialTools:
    def test_run_one_at_a_time_in_the_order_they_were_called(self):
        """A sandbox's write_file must finish before the run_command called after it starts."""
        events: list[str] = []

        def write_file() -> str:
            """Write a file, slowly."""
            events.append("write started")
            time.sleep(0.2)
            events.append("write finished")
            return "written"

        def run_command() -> str:
            """Run a command that reads the file."""
            events.append("command ran")
            return "ran"

        tools = _by_name(
            airflow_tools_from_toolset(FunctionToolset([write_file, run_command], sequential=True))
        )

        async def one_turn():
            return await asyncio.gather(tools["write_file"].call({}), tools["run_command"].call({}))

        asyncio.run(one_turn())

        assert events == ["write started", "write finished", "command ran"]

    def test_more_waiting_calls_than_threads_do_not_starve_the_running_one(self):
        """
        Waiting holds no thread, so the running call can still get one for its blocking work.

        A hook call runs on the event loop's default executor; were each waiting call to hold a
        thread of it, two waiting calls would leave none for the call whose turn it is.
        """
        tool = airflow_tools_from_toolset(HookToolset(_FakeHook(), allowed_methods=["list_keys"]))[0]

        async def many_at_once():
            asyncio.get_running_loop().set_default_executor(
                concurrent.futures.ThreadPoolExecutor(max_workers=2)
            )
            calls = (tool.call({"bucket": "reports"}) for _ in range(6))
            return await asyncio.wait_for(asyncio.gather(*calls), timeout=10)

        results = asyncio.run(many_at_once())

        assert [r.is_error for r in results] == [False] * 6


def _failing_step() -> str:
    """Always ask for a correction."""
    raise ModelRetry("bad input")


class _FakeHook:
    def list_keys(self, bucket: str, prefix: str | None = None) -> list[str]:
        """
        List object keys in a bucket.

        :param bucket: Name of the bucket.
        :param prefix: Key prefix to filter by.
        """
        return [f"{bucket}/{prefix or ''}a.csv"]


class TestHookToolsetAirflowTools:
    def test_calls_the_hook_method(self):
        ts = HookToolset(_FakeHook(), allowed_methods=["list_keys"], tool_name_prefix="s3_")

        tool = _by_name(ts.airflow_tools())["s3_list_keys"]
        result = asyncio.run(tool.call({"bucket": "raw", "prefix": "2026/"}))

        assert result == ToolResult(content='["raw/2026/a.csv"]')
        assert tool.parameters["required"] == ["bucket"]
        assert tool.description == "List object keys in a bucket."
