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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.toolsets.mcp import MCPToolset


class TestMCPToolsetInit:
    def test_id_includes_conn_id(self):
        ts = MCPToolset("my_mcp_server")
        assert ts.id == "mcp-my_mcp_server"

    def test_stores_tool_prefix(self):
        ts = MCPToolset("my_mcp_server", tool_prefix="weather")
        assert ts._tool_prefix == "weather"


class TestMCPToolsetGetServer:
    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStreamableHTTP", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_http_transport(self, mock_base_hook, mock_server_cls):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        server = ts._get_server()

        mock_server_cls.assert_called_once_with("http://localhost:3001/mcp", headers=None, tool_prefix=None)
        assert server is mock_server_cls.return_value

    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStreamableHTTP", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_http_with_auth_token(self, mock_base_hook, mock_server_cls):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
            password="my-token",
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        ts._get_server()

        mock_server_cls.assert_called_once_with(
            "http://localhost:3001/mcp",
            headers={"Authorization": "Bearer my-token"},
            tool_prefix=None,
        )

    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerSSE", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_sse_transport(self, mock_base_hook, mock_server_cls):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            host="http://localhost:3001/sse",
            extra=json.dumps({"transport": "sse"}),
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        server = ts._get_server()

        mock_server_cls.assert_called_once_with("http://localhost:3001/sse", headers=None, tool_prefix=None)
        assert server is mock_server_cls.return_value

    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStdio", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_stdio_transport(self, mock_base_hook, mock_server_cls):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "uvx", "args": ["mcp-run-python"]}),
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        server = ts._get_server()

        mock_server_cls.assert_called_once_with("uvx", args=["mcp-run-python"], timeout=10, tool_prefix=None)
        assert server is mock_server_cls.return_value

    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStreamableHTTP", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_passes_tool_prefix(self, mock_base_hook, mock_server_cls):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn", tool_prefix="weather")
        ts._get_server()

        mock_server_cls.assert_called_once_with(
            "http://localhost:3001/mcp", headers=None, tool_prefix="weather"
        )

    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStdio", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_args_string_converted_to_list(self, mock_base_hook, mock_server_cls):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "uvx", "args": "mcp-run-python"}),
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        ts._get_server()

        mock_server_cls.assert_called_once_with("uvx", args=["mcp-run-python"], timeout=10, tool_prefix=None)

    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_http_without_host_raises(self, mock_base_hook):
        conn = Connection(conn_id="mcp_conn", conn_type="mcp")
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        with pytest.raises(ValueError, match="requires a host URL"):
            ts._get_server()

    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_stdio_without_command_raises(self, mock_base_hook):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio"}),
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        with pytest.raises(ValueError, match="requires 'command'"):
            ts._get_server()

    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_unknown_transport_raises(self, mock_base_hook):
        conn = Connection(
            conn_id="mcp_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "grpc"}),
        )
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        with pytest.raises(ValueError, match="Unknown transport"):
            ts._get_server()

    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStreamableHTTP", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_caches_server(self, mock_base_hook, mock_server_cls):
        conn = Connection(conn_id="mcp_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        mock_base_hook.get_connection.return_value = conn

        ts = MCPToolset("mcp_conn")
        first = ts._get_server()
        second = ts._get_server()

        assert first is second
        mock_base_hook.get_connection.assert_called_once()


class TestMCPToolsetDelegation:
    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStreamableHTTP", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_get_tools_delegates(self, mock_base_hook, mock_server_cls):
        conn = Connection(conn_id="mcp_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        mock_base_hook.get_connection.return_value = conn

        mock_server = mock_server_cls.return_value
        expected_tools = {"tool1": MagicMock()}
        mock_server.get_tools = AsyncMock(return_value=expected_tools)

        ts = MCPToolset("mcp_conn")
        ctx = MagicMock()
        result = asyncio.run(ts.get_tools(ctx))

        assert result is expected_tools
        mock_server.get_tools.assert_awaited_once_with(ctx)

    @patch("airflow.providers.common.ai.toolsets.mcp.MCPServerStreamableHTTP", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.mcp.BaseHook", autospec=True)
    def test_call_tool_delegates(self, mock_base_hook, mock_server_cls):
        conn = Connection(conn_id="mcp_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        mock_base_hook.get_connection.return_value = conn

        mock_server = mock_server_cls.return_value
        mock_server.call_tool = AsyncMock(return_value="tool result")

        ts = MCPToolset("mcp_conn")
        ctx = MagicMock()
        tool = MagicMock()
        result = asyncio.run(ts.call_tool("my_tool", {"arg": "value"}, ctx, tool))

        assert result == "tool result"
        mock_server.call_tool.assert_awaited_once_with("my_tool", {"arg": "value"}, ctx, tool)
