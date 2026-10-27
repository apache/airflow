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
"""How many times the model may correct a failed call to a Common AI toolset's tools."""

from __future__ import annotations

import asyncio

import pytest
from pydantic_ai import Agent, RunContext
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
from airflow.providers.common.ai.toolsets.hook import HookToolset
from airflow.providers.common.ai.toolsets.object_storage import ObjectStorageToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.compat.sdk import BaseHook

from unit.common.ai.toolsets.test_datafusion import _make_mock_datasource_config
from unit.common.ai.toolsets.test_sql import _make_mock_db_hook


class _ListKeysHook(BaseHook):
    def list_keys(self, bucket: str) -> list[str]:
        """List the keys in a bucket."""
        return []


TOOLSETS = [
    pytest.param(lambda **kw: SQLToolset("pg_default", **kw), id="sql"),
    pytest.param(lambda **kw: HookToolset(_ListKeysHook(), allowed_methods=["list_keys"], **kw), id="hook"),
    pytest.param(lambda **kw: DataFusionToolset([_make_mock_datasource_config()], **kw), id="datafusion"),
    pytest.param(lambda **kw: ObjectStorageToolset("memory://bucket/reports", **kw), id="object-storage"),
]


@pytest.mark.parametrize("make_toolset", TOOLSETS)
class TestToolsetRetryBudget:
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [pytest.param({}, 3, id="agents-budget"), pytest.param({"max_retries": 0}, 0, id="own-budget")],
    )
    def test_tools_get_the_toolsets_budget_else_the_agents(self, make_toolset, kwargs, expected):
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage(), max_retries=3)

        tools = asyncio.run(make_toolset(**kwargs).get_tools(ctx))

        assert {tool.max_retries for tool in tools.values()} == {expected}

    def test_negative_budget_is_rejected(self, make_toolset):
        with pytest.raises(ValueError, match="max_retries must not be negative"):
            make_toolset(max_retries=-1)


class TestSQLToolsetInAnAgent:
    @staticmethod
    def _run(agent_retries: int | None, failures: int) -> str:
        hook = _make_mock_db_hook()
        hook.run.side_effect = [RuntimeError('column "totl" does not exist')] * failures + [[(42,)]]
        toolset = SQLToolset("pg_default")
        toolset._hook = hook

        def model_fn(messages, info):
            if any(isinstance(p, ToolReturnPart) for m in messages for p in m.parts):
                return ModelResponse(parts=[TextPart(content="done")])
            call = ToolCallPart(tool_name="query", args={"sql": "SELECT 1"}, tool_call_id=f"c{len(messages)}")
            return ModelResponse(parts=[call])

        return (
            Agent(FunctionModel(model_fn), toolsets=[toolset], retries=agent_retries)
            .run_sync("total?")
            .output
        )

    def test_the_agents_retries_let_the_model_correct_its_sql_more_than_once(self):
        assert self._run(agent_retries=3, failures=2) == "done"

    def test_the_default_budget_still_ends_the_run(self):
        with pytest.raises(UnexpectedModelBehavior, match="exceeded max retries count of 1"):
            self._run(agent_retries=None, failures=2)
