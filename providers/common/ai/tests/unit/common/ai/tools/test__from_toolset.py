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
"""``airflow_tools()`` on the bundled toolsets: same behaviour as the pydantic-ai path."""

from __future__ import annotations

import asyncio
import json
import threading
import time

import pytest
from pydantic_ai import RunContext
from pydantic_ai.models.test import TestModel
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.tools import ToolResult
from airflow.providers.common.ai.toolsets.hook import HookToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk._shared.secrets_masker import reset_secrets_masker
from airflow.sdk.log import mask_secret

from unit.common.ai.toolsets.test_sql import _make_mock_db_hook


def _by_name(toolset) -> dict:
    return {tool.name: tool for tool in toolset.airflow_tools()}


@pytest.fixture
def db_password():
    reset_secrets_masker()
    mask_secret("db-password-91c3")
    yield "db-password-91c3"
    reset_secrets_masker()


class TestSQLToolsetAirflowTools:
    def test_exposes_the_same_tools_as_the_pydantic_ai_path(self):
        ts = SQLToolset("pg_default")
        ctx = RunContext(deps=None, model=TestModel(), usage=RunUsage())
        pydantic_tools = asyncio.run(ts.get_tools(ctx))

        tools = _by_name(ts)

        assert tools.keys() == pydantic_tools.keys()
        for name, tool in tools.items():
            assert tool.parameters == pydantic_tools[name].tool_def.parameters_json_schema
            assert tool.description == pydantic_tools[name].tool_def.description

    def test_query_returns_rows(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])

        result = asyncio.run(_by_name(ts)["query"].call({"sql": "SELECT id, name FROM users"}))

        assert not result.is_error
        assert json.loads(result.content)["rows"] == [[1, "Ada"]]

    def test_blocked_statement_is_an_error_the_model_can_read(self):
        """The toolset's own SQL validation still applies; its retry message reaches the model."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(_by_name(ts)["query"].call({"sql": "DROP TABLE users"}))

        assert result.is_error
        assert "The query tool failed" in result.content
        ts._hook.run.assert_not_called()

    def test_missing_argument_is_an_error_the_model_can_read(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(_by_name(ts)["query"].call({}))

        assert result.is_error
        assert "sql" in result.content
        ts._hook.run.assert_not_called()

    @pytest.mark.enable_redact
    def test_database_error_carrying_a_secret_is_masked(self, db_password):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.run.side_effect = RuntimeError(f"could not connect to postgresql://svc:{db_password}@db")

        result = asyncio.run(_by_name(ts)["query"].call({"sql": "SELECT 1"}))

        assert result.is_error
        assert db_password not in result.content
        assert "postgresql://svc:***@db" in result.content

    def test_calls_into_one_toolset_never_overlap(self):
        """
        Two agents sharing a toolset get separate tool lists; their calls must still
        take turns on the toolset's one hook, whose ``last_description`` each query reads.
        """
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
        first, second = _by_name(ts)["query"], _by_name(ts)["query"]

        async def both():
            return await asyncio.gather(
                first.call({"sql": "SELECT id, name FROM users"}),
                second.call({"sql": "SELECT id, name FROM users"}),
            )

        results = asyncio.run(both())

        assert [r.is_error for r in results] == [False, False]
        assert peak == 1


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

        tool = _by_name(ts)["s3_list_keys"]
        result = asyncio.run(tool.call({"bucket": "raw", "prefix": "2026/"}))

        assert result == ToolResult(content='["raw/2026/a.csv"]')
        assert tool.parameters["required"] == ["bucket"]
        assert tool.description == "List object keys in a bucket."
