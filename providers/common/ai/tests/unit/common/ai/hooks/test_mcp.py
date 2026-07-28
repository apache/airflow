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

import json
from unittest.mock import MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.mcp import MCPHook

# The hook imports these lazily inside get_conn(), so we patch them at their
# source modules rather than on the hook module. MCPToolset is the unified
# pydantic-ai entrypoint; the three transports come from FastMCP.
_MCP_TOOLSET = "pydantic_ai.mcp.MCPToolset"
_HTTP_TRANSPORT = "fastmcp.client.transports.StreamableHttpTransport"
_SSE_TRANSPORT = "fastmcp.client.transports.SSETransport"
_STDIO_TRANSPORT = "fastmcp.client.transports.StdioTransport"


class TestMCPHookInit:
    def test_default_conn_id(self):
        hook = MCPHook()
        assert hook.mcp_conn_id == "mcp_default"

    def test_custom_conn_id(self):
        hook = MCPHook(mcp_conn_id="my_mcp")
        assert hook.mcp_conn_id == "my_mcp"

    def test_tool_prefix(self):
        hook = MCPHook(mcp_conn_id="my_mcp", tool_prefix="weather")
        assert hook.tool_prefix == "weather"


class TestMCPHookGetConn:
    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_http_transport(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        mock_transport_cls.assert_called_once_with("http://localhost:3001/mcp", headers=None)
        mock_toolset_cls.assert_called_once_with(mock_transport_cls.return_value)
        assert result is mock_toolset_cls.return_value

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_http_is_default_transport(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with("http://localhost:3001/mcp", headers=None)
        mock_toolset_cls.assert_called_once_with(mock_transport_cls.return_value)

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_http_with_auth_token(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
            password="my-secret-token",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(
            "http://localhost:3001/mcp",
            headers={"Authorization": "Bearer my-secret-token"},
        )

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_passes_tool_prefix(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn", tool_prefix="weather")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        # tool_prefix is applied by wrapping the toolset, not via a constructor arg.
        mock_toolset_cls.assert_called_once_with(mock_transport_cls.return_value)
        mock_toolset_cls.return_value.prefixed.assert_called_once_with("weather")
        assert result is mock_toolset_cls.return_value.prefixed.return_value

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_SSE_TRANSPORT, autospec=True)
    def test_sse_transport(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            host="http://localhost:3001/sse",
            extra=json.dumps({"transport": "sse"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        mock_transport_cls.assert_called_once_with("http://localhost:3001/sse", headers=None)
        mock_toolset_cls.assert_called_once_with(mock_transport_cls.return_value)
        assert result is mock_toolset_cls.return_value

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_stdio_transport(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "uvx", "args": ["mcp-run-python"]}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        mock_transport_cls.assert_called_once_with(command="uvx", args=["mcp-run-python"], env=None)
        mock_toolset_cls.assert_called_once_with(mock_transport_cls.return_value, init_timeout=10)
        assert result is mock_toolset_cls.return_value

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_stdio_custom_timeout(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps(
                {"transport": "stdio", "command": "python", "args": ["-m", "server"], "timeout": 30}
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(command="python", args=["-m", "server"], env=None)
        mock_toolset_cls.assert_called_once_with(mock_transport_cls.return_value, init_timeout=30)

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_args_string_converted_to_list(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "uvx", "args": "mcp-run-python"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(command="uvx", args=["mcp-run-python"], env=None)

    def test_http_without_host_raises(self):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="requires a host URL"):
                hook.get_conn()

    def test_sse_without_host_raises(self):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "sse"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="requires a host URL"):
                hook.get_conn()

    def test_stdio_without_command_raises(self):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="requires 'command'"):
                hook.get_conn()

    def test_unknown_transport_raises(self):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "websocket"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="Unknown transport"):
                hook.get_conn()

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_caches_server(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_conn()
            second = hook.get_conn()

        assert first is second
        mock_toolset_cls.assert_called_once()


class TestMCPHookTestConnection:
    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_successful_config(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert "valid" in message.lower()

    def test_failed_config(self):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "host URL" in message


class TestMCPHookUIFieldBehaviour:
    def test_hidden_fields(self):
        behaviour = MCPHook.get_ui_field_behaviour()
        assert "schema" in behaviour["hidden_fields"]
        assert "port" in behaviour["hidden_fields"]
        assert "login" in behaviour["hidden_fields"]

    def test_relabeling(self):
        behaviour = MCPHook.get_ui_field_behaviour()
        assert behaviour["relabeling"]["password"] == "Auth Token"


class TestMCPHookTokenProvider:
    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_http_uses_token_provider(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn", token_provider=lambda: "minted-jwt")
        conn = Connection(conn_id="test_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(
            "http://localhost:3001/mcp",
            headers={"Authorization": "Bearer minted-jwt"},
        )

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_token_provider_overrides_static_password(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn", token_provider=lambda: "fresh")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            host="http://localhost:3001/mcp",
            password="static-pat",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(
            "http://localhost:3001/mcp",
            headers={"Authorization": "Bearer fresh"},
        )

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_token_provider_called_when_establishing_connection(self, mock_transport_cls, mock_toolset_cls):
        provider = MagicMock(return_value="tok")
        hook = MCPHook(mcp_conn_id="test_conn", token_provider=provider)
        conn = Connection(conn_id="test_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        provider.assert_called_once_with()

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_masks_minted_token(self, mock_transport_cls, mock_toolset_cls):
        """The minted token must be registered with secret masking, like conn.password."""
        hook = MCPHook(mcp_conn_id="test_conn", token_provider=lambda: "minted-jwt")
        conn = Connection(conn_id="test_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch("airflow.providers.common.ai.hooks.mcp.mask_secret", autospec=True) as mock_mask,
        ):
            hook.get_conn()

        mock_mask.assert_called_once_with("minted-jwt")

    @pytest.mark.parametrize("bad_token", ["", None], ids=["empty", "non_string"])
    def test_invalid_token_raises(self, bad_token):
        """A token_provider returning a non-string or empty value fails loud."""
        hook = MCPHook(mcp_conn_id="test_conn", token_provider=lambda: bad_token)
        conn = Connection(conn_id="test_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="must return a non-empty string token"):
                hook.get_conn()

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_SSE_TRANSPORT, autospec=True)
    def test_sse_uses_token_provider(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn", token_provider=lambda: "minted")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            host="http://localhost:3001/sse",
            extra=json.dumps({"transport": "sse"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(
            "http://localhost:3001/sse",
            headers={"Authorization": "Bearer minted"},
        )

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_stdio_does_not_invoke_token_provider(self, mock_transport_cls, mock_toolset_cls):
        """stdio has no HTTP headers, so the token provider must not be called."""
        provider = MagicMock(return_value="tok")
        hook = MCPHook(mcp_conn_id="test_conn", token_provider=provider)
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "uvx", "args": ["mcp-run-python"]}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        provider.assert_not_called()
        mock_transport_cls.assert_called_once_with(command="uvx", args=["mcp-run-python"], env=None)
        mock_toolset_cls.assert_called_once_with(mock_transport_cls.return_value, init_timeout=10)


class TestMCPHookEnvProvider:
    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_stdio_uses_env_provider(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=lambda: {"SPLUNK_API_KEY": "minted"})
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "spacefarer-mcp", "args": []}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(
            command="spacefarer-mcp", args=[], env={"SPLUNK_API_KEY": "minted"}
        )

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_static_extra_env_used_when_no_provider(self, mock_transport_cls, mock_toolset_cls):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps(
                {
                    "transport": "stdio",
                    "command": "spacefarer-mcp",
                    "args": [],
                    "env": {"SPACEFARER_MCP_ALLOW_EXECUTION": "0"},
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(
            command="spacefarer-mcp", args=[], env={"SPACEFARER_MCP_ALLOW_EXECUTION": "0"}
        )

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_env_provider_merges_over_static_extra_env(self, mock_transport_cls, mock_toolset_cls):
        """env_provider keys win over Extra.env on conflicts; other static keys survive."""
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=lambda: {"SPLUNK_API_KEY": "fresh"})
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps(
                {
                    "transport": "stdio",
                    "command": "spacefarer-mcp",
                    "args": [],
                    "env": {"SPLUNK_API_KEY": "stale", "SPACEFARER_MCP_ALLOW_EXECUTION": "0"},
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_transport_cls.assert_called_once_with(
            command="spacefarer-mcp",
            args=[],
            env={"SPLUNK_API_KEY": "fresh", "SPACEFARER_MCP_ALLOW_EXECUTION": "0"},
        )

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_env_provider_called_when_establishing_connection(self, mock_transport_cls, mock_toolset_cls):
        provider = MagicMock(return_value={"SPLUNK_API_KEY": "minted"})
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=provider)
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "spacefarer-mcp", "args": []}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        provider.assert_called_once_with()

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_STDIO_TRANSPORT, autospec=True)
    def test_masks_provided_env_values(self, mock_transport_cls, mock_toolset_cls):
        """Minted env values must be registered with secret masking, like the minted token."""
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=lambda: {"SPLUNK_API_KEY": "minted"})
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "spacefarer-mcp", "args": []}),
        )
        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch("airflow.providers.common.ai.hooks.mcp.mask_secret", autospec=True) as mock_mask,
        ):
            hook.get_conn()

        mock_mask.assert_called_once_with("minted")

    @pytest.mark.parametrize("bad_value", ["", None, 123], ids=["empty", "none", "non_string"])
    def test_invalid_env_provider_value_raises(self, bad_value):
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=lambda: {"SPLUNK_API_KEY": bad_value})
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "spacefarer-mcp", "args": []}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="must have non-empty string values"):
                hook.get_conn()

    def test_invalid_env_provider_key_raises(self):
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=lambda: {1: "value"})
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "spacefarer-mcp", "args": []}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="must have non-empty string keys"):
                hook.get_conn()

    def test_invalid_env_provider_return_type_raises(self):
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=lambda: "not-a-dict")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps({"transport": "stdio", "command": "spacefarer-mcp", "args": []}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="must return a dict\\[str, str\\]"):
                hook.get_conn()

    def test_invalid_extra_env_type_raises(self):
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps(
                {"transport": "stdio", "command": "spacefarer-mcp", "args": [], "env": ["not", "a", "dict"]}
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="must be an object of"):
                hook.get_conn()

    @pytest.mark.parametrize("falsy_bad_env", [[], 0, False, ""], ids=["list", "zero", "false", "str"])
    def test_falsy_invalid_extra_env_type_raises(self, falsy_bad_env):
        """A falsy-but-wrong-type ``env`` (``[]``, ``0``, ...) must not silently become ``{}``."""
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps(
                {
                    "transport": "stdio",
                    "command": "spacefarer-mcp",
                    "args": [],
                    "env": falsy_bad_env,
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="must be an object of"):
                hook.get_conn()

    def test_invalid_extra_env_value_raises(self):
        """A non-string value in the static Extra.env must fail with a clear message, not a bare
        TypeError deep inside subprocess.Popen when the transport actually spawns."""
        hook = MCPHook(mcp_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="mcp",
            extra=json.dumps(
                {
                    "transport": "stdio",
                    "command": "spacefarer-mcp",
                    "args": [],
                    "env": {"SPACEFARER_MCP_ALLOW_EXECUTION": 0},
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="must have non-empty string values"):
                hook.get_conn()

    @patch(_MCP_TOOLSET, autospec=True)
    @patch(_HTTP_TRANSPORT, autospec=True)
    def test_http_does_not_invoke_env_provider(self, mock_transport_cls, mock_toolset_cls):
        """HTTP/SSE have no subprocess environment, so the env provider must not be called."""
        provider = MagicMock(return_value={"SPLUNK_API_KEY": "minted"})
        hook = MCPHook(mcp_conn_id="test_conn", env_provider=provider)
        conn = Connection(conn_id="test_conn", conn_type="mcp", host="http://localhost:3001/mcp")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        provider.assert_not_called()
