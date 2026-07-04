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
import os
from collections.abc import Callable
from unittest import mock

import httpx
import pytest
from fastmcp.exceptions import ToolError

from airflow_mcp_server import client as client_module

Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture(autouse=True)
def _reset_state():
    client_module.reset_client_state()
    yield
    client_module.reset_client_state()


def _mock_client(handler: Handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _login_response(token: str = "tok-1") -> httpx.Response:
    return httpx.Response(201, json={"access_token": token})


class TestTokenFlow:
    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_fetches_and_caches_token(self, mock_build):
        login_calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/auth/token":
                login_calls.append(request)
                return _login_response("tok-1")
            assert request.headers["authorization"] == "Bearer tok-1"
            return httpx.Response(200, json={"ok": True})

        mock_build.return_value = _mock_client(handler)

        await client_module.call_api("GET", "/api/v2/version")
        await client_module.call_api("GET", "/api/v2/version")

        assert len(login_calls) == 1

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW_ACCESS_TOKEN": "preset-token"})
    async def test_preset_token_skips_login(self, mock_build):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path != "/auth/token"
            assert request.headers["authorization"] == "Bearer preset-token"
            return httpx.Response(200, json={"ok": True})

        mock_build.return_value = _mock_client(handler)

        result = await client_module.call_api("GET", "/api/v2/version")

        assert result == {"ok": True}

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW_USERNAME": "someone", "AIRFLOW_PASSWORD": "secret"})
    async def test_login_uses_configured_credentials(self, mock_build):
        seen_bodies = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/auth/token":
                seen_bodies.append(json.loads(request.content))
                return _login_response("tok-1")
            return httpx.Response(200, json={"ok": True})

        mock_build.return_value = _mock_client(handler)

        await client_module.call_api("GET", "/api/v2/version")

        assert seen_bodies == [{"username": "someone", "password": "secret"}]

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_retries_once_on_401_with_fresh_token(self, mock_build):
        issued_tokens = []

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/auth/token":
                token = f"tok-{len(issued_tokens) + 1}"
                issued_tokens.append(token)
                return _login_response(token)
            if request.headers["authorization"] == "Bearer tok-1":
                return httpx.Response(401, json={"detail": "expired"})
            return httpx.Response(200, json={"ok": True})

        mock_build.return_value = _mock_client(handler)

        result = await client_module.call_api("GET", "/api/v2/version")

        assert result == {"ok": True}
        assert issued_tokens == ["tok-1", "tok-2"]

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_raises_on_failed_login(self, mock_build):
        mock_build.return_value = _mock_client(lambda request: httpx.Response(401, text="bad credentials"))

        with pytest.raises(ToolError, match="authenticate"):
            await client_module.call_api("GET", "/api/v2/version")

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW_ACCESS_TOKEN": "tok"})
    def test_rebuilds_client_when_event_loop_changes(self, mock_build):
        mock_build.side_effect = lambda: _mock_client(lambda request: httpx.Response(200, json={"ok": True}))

        asyncio.run(client_module.call_api("GET", "/api/v2/version"))
        asyncio.run(client_module.call_api("GET", "/api/v2/version"))

        assert mock_build.call_count == 2


class TestWriteGate:
    async def test_post_blocked_without_flag(self):
        with pytest.raises(ToolError, match="read-only"):
            await client_module.call_api("POST", "/api/v2/dags/foo/dagRuns", json_body={})

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW_MCP_ALLOW_WRITES": "true", "AIRFLOW_ACCESS_TOKEN": "tok"})
    async def test_post_allowed_with_flag(self, mock_build):
        mock_build.return_value = _mock_client(lambda request: httpx.Response(200, json={"ok": True}))

        result = await client_module.call_api("POST", "/api/v2/dags/foo/dagRuns", json_body={})

        assert result == {"ok": True}

    async def test_delete_blocked_without_deletes_flag(self):
        with pytest.raises(ToolError, match="DELETE"):
            await client_module.call_api("DELETE", "/api/v2/dags/foo")

    @mock.patch.dict(os.environ, {"AIRFLOW_MCP_ALLOW_WRITES": "true"})
    async def test_delete_blocked_when_only_writes_enabled(self):
        # DELETE has its own stricter gate; enabling writes must not enable deletions.
        with pytest.raises(ToolError, match="DELETE"):
            await client_module.call_api("DELETE", "/api/v2/dags/foo")

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW_MCP_ALLOW_DELETES": "true", "AIRFLOW_ACCESS_TOKEN": "tok"})
    async def test_delete_allowed_with_deletes_flag(self, mock_build):
        mock_build.return_value = _mock_client(lambda request: httpx.Response(200, json={"ok": True}))

        result = await client_module.call_api("DELETE", "/api/v2/dags/foo")

        assert result == {"ok": True}

    async def test_unsupported_method_rejected(self):
        with pytest.raises(ToolError, match="Unsupported HTTP method"):
            await client_module.call_api("HEAD", "/api/v2/dags")


class TestPathNormalization:
    async def test_rejects_path_outside_api_v2(self):
        with pytest.raises(ToolError, match="/api/v2/"):
            await client_module.call_api("GET", "/auth/token")

    @pytest.mark.parametrize(
        "path",
        [
            "/api/v2/../../auth/token",  # escapes /api/v2 into another mounted sub-app
            "/api/v2/dags/../variables/secret",  # redirects to a different resource
        ],
    )
    async def test_rejects_dot_segments(self, path):
        with pytest.raises(ToolError, match=r"\.\."):
            await client_module.call_api("GET", path)


class TestErrorMapping:
    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW_ACCESS_TOKEN": "tok"})
    async def test_404_raises_tool_error_with_status_and_body(self, mock_build):
        mock_build.return_value = _mock_client(lambda request: httpx.Response(404, text="Dag not found"))

        with pytest.raises(ToolError, match="404") as exc_info:
            await client_module.call_api("GET", "/api/v2/dags/missing")

        assert "Dag not found" in str(exc_info.value)

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW_ACCESS_TOKEN": "tok"})
    async def test_non_json_response_wrapped_as_text(self, mock_build):
        mock_build.return_value = _mock_client(
            lambda request: httpx.Response(200, content=b"plain body", headers={"content-type": "text/plain"})
        )

        result = await client_module.call_api("GET", "/api/v2/dags/foo/dagRuns/bar/taskInstances/baz/logs/1")

        assert result == {"text": "plain body"}
