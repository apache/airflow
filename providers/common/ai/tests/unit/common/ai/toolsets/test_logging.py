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

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from airflow.providers.common.ai.toolsets.logging import LoggingToolset


@pytest.fixture
def wrapped_toolset():
    ts = AsyncMock()
    ts.id = "test-toolset"
    ts.get_tools = AsyncMock(return_value={"tool_a": MagicMock()})
    return ts


@pytest.fixture
def logger():
    return logging.getLogger("test.logging_toolset")


@pytest.fixture
def logging_toolset(wrapped_toolset, logger):
    return LoggingToolset(wrapped=wrapped_toolset, logger=logger)


class TestLoggingToolset:
    @pytest.mark.asyncio
    async def test_logs_tool_call_name(self, logging_toolset, wrapped_toolset, logger, caplog):
        wrapped_toolset.call_tool = AsyncMock(return_value="result")
        ctx = MagicMock()
        tool = MagicMock()

        with caplog.at_level(logging.INFO, logger="test.logging_toolset"):
            await logging_toolset.call_tool("list_tables", {"schema": "public"}, ctx, tool)

        assert any("::group::Tool call: list_tables" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_args_at_debug_level(self, logging_toolset, wrapped_toolset, logger, caplog):
        wrapped_toolset.call_tool = AsyncMock(return_value="result")
        ctx = MagicMock()
        tool = MagicMock()

        with caplog.at_level(logging.DEBUG, logger="test.logging_toolset"):
            await logging_toolset.call_tool("list_tables", {"schema": "public"}, ctx, tool)

        debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
        assert any('Tool args: {"schema": "public"}' in r.message for r in debug_records)

    @pytest.mark.asyncio
    async def test_logs_timing(self, logging_toolset, wrapped_toolset, logger, caplog):
        wrapped_toolset.call_tool = AsyncMock(return_value="ok")
        ctx = MagicMock()
        tool = MagicMock()

        with caplog.at_level(logging.INFO, logger="test.logging_toolset"):
            await logging_toolset.call_tool("query", {}, ctx, tool)

        assert any("Tool query returned in" in r.message for r in caplog.records)
        assert any("::endgroup::" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_logs_error_on_exception(self, logging_toolset, wrapped_toolset, logger, caplog):
        wrapped_toolset.call_tool = AsyncMock(side_effect=RuntimeError("boom"))
        ctx = MagicMock()
        tool = MagicMock()

        with caplog.at_level(logging.INFO, logger="test.logging_toolset"):
            with pytest.raises(RuntimeError, match="boom"):
                await logging_toolset.call_tool("bad_tool", {}, ctx, tool)

        assert any("Tool bad_tool failed after" in r.message for r in caplog.records)
        assert any("::endgroup::" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_delegates_get_tools(self, logging_toolset, wrapped_toolset):
        ctx = MagicMock()
        tools = await logging_toolset.get_tools(ctx)

        assert tools == {"tool_a": wrapped_toolset.get_tools.return_value["tool_a"]}
        wrapped_toolset.get_tools.assert_awaited_once_with(ctx)

    @pytest.mark.asyncio
    async def test_empty_args_not_logged(self, logging_toolset, wrapped_toolset, caplog):
        wrapped_toolset.call_tool = AsyncMock(return_value="ok")
        ctx = MagicMock()
        tool = MagicMock()

        with caplog.at_level(logging.DEBUG, logger="test.logging_toolset"):
            await logging_toolset.call_tool("list_tables", {}, ctx, tool)

        assert not any("Tool args:" in r.message for r in caplog.records)
