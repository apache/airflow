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
from collections.abc import Callable
from unittest import mock

import httpx
import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from airflow_mcp_server import client as client_module
from airflow_mcp_server.server import mcp

Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch: pytest.MonkeyPatch):
    # A fixed pre-issued token keeps tool tests focused on tool behavior, not the login flow
    # (which test_client.py already covers in depth).
    monkeypatch.setenv("AIRFLOW_ACCESS_TOKEN", "test-token")
    client_module.reset_client_state()
    yield
    client_module.reset_client_state()


def _install_handler(mock_build: mock.MagicMock, handler: Handler) -> None:
    mock_build.return_value = httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _json_response(payload: object, status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code, json=payload)


class TestListDags:
    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_trims_dag_fields(self, mock_build):
        dags_payload = {
            "dags": [
                {
                    "dag_id": "example_dag",
                    "dag_display_name": "Example",
                    "is_paused": False,
                    "is_stale": False,
                    "has_import_errors": False,
                    "next_dagrun_run_after": "2026-07-04T00:00:00Z",
                    "timetable_summary": "@daily",
                    "owners": ["airflow"],
                    "tags": [{"name": "team-a", "dag_id": "example_dag", "dag_display_name": "Example"}],
                    "fileloc": "/opt/airflow/dags/example_dag.py",
                }
            ],
            "total_entries": 1,
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags"
            return _json_response(dags_payload)

        _install_handler(mock_build, handler)

        async with Client(mcp) as c:
            result = await c.call_tool("list_dags", {"limit": 10})

        assert result.data == {
            "dags": [
                {
                    "dag_id": "example_dag",
                    "is_paused": False,
                    "is_stale": False,
                    "has_import_errors": False,
                    "next_dagrun_run_after": "2026-07-04T00:00:00Z",
                    "timetable_summary": "@daily",
                    "owners": ["airflow"],
                    "tags": ["team-a"],
                }
            ],
            "total_entries": 1,
        }
        # fileloc was not requested and must not leak through.
        assert "fileloc" not in result.data["dags"][0]


class TestGetTaskLog:
    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_tails_long_logs(self, mock_build):
        events = [{"event": f"line {i}", "timestamp": "2026-07-04T00:00:00Z"} for i in range(10)]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == ("/api/v2/dags/d/dagRuns/r/taskInstances/t/logs/1")
            return _json_response({"content": events, "continuation_token": None})

        _install_handler(mock_build, handler)

        async with Client(mcp) as c:
            result = await c.call_tool(
                "get_task_log",
                {"dag_id": "d", "dag_run_id": "r", "task_id": "t", "try_number": 1, "tail_lines": 3},
            )

        assert result.data["total_lines"] == 10
        assert result.data["truncated"] is True
        assert result.data["log"] == "line 7\nline 8\nline 9"

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_returns_full_log_when_shorter_than_tail(self, mock_build):
        events = [{"event": "only line"}]
        _install_handler(
            mock_build, lambda request: _json_response({"content": events, "continuation_token": None})
        )

        async with Client(mcp) as c:
            result = await c.call_tool(
                "get_task_log",
                {"dag_id": "d", "dag_run_id": "r", "task_id": "t", "try_number": 1, "tail_lines": 500},
            )

        assert result.data["total_lines"] == 1
        assert result.data["truncated"] is False
        assert result.data["log"] == "only line"

    async def test_rejects_non_positive_tail_lines(self):
        async with Client(mcp) as c:
            with pytest.raises(ToolError):
                await c.call_tool(
                    "get_task_log",
                    {"dag_id": "d", "dag_run_id": "r", "task_id": "t", "tail_lines": 0},
                )


class TestDiagnoseDagRun:
    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_composes_run_task_instances_and_failure_logs(self, mock_build):
        task_instances = [
            {"task_id": "ok_task", "state": "success", "try_number": 1, "map_index": -1},
            {"task_id": "bad_task", "state": "failed", "try_number": 2, "map_index": -1},
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/api/v2/dags/d/dagRuns/r":
                return _json_response({"dag_run_id": "r", "state": "failed"})
            if path == "/api/v2/dags/d/dagRuns/r/taskInstances":
                return _json_response({"task_instances": task_instances, "total_entries": 2})
            if path == "/api/v2/dags/d/dagRuns/r/taskInstances/bad_task/logs/2":
                return _json_response({"content": [{"event": "boom"}], "continuation_token": None})
            raise AssertionError(f"unexpected request path: {path}")

        _install_handler(mock_build, handler)

        async with Client(mcp) as c:
            result = await c.call_tool("diagnose_dag_run", {"dag_id": "d", "dag_run_id": "r"})

        assert result.data["dag_run"] == {"dag_run_id": "r", "state": "failed"}
        assert len(result.data["task_instances"]) == 2
        assert result.data["total_task_instances"] == 2
        assert result.data["task_instances_truncated"] is False
        assert result.data["failures"] == [{"task_id": "bad_task", "state": "failed", "log_tail": "boom"}]


class TestSecretStripping:
    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_list_connections_omits_password_and_extra(self, mock_build):
        payload = {
            "connections": [
                {
                    "connection_id": "my_db",
                    "conn_type": "postgres",
                    "host": "db",
                    "schema": "public",
                    "port": 5432,
                    "login": "user",
                    "description": "primary db",
                    "password": "s3cret",
                    "extra": '{"token": "abc"}',
                }
            ],
            "total_entries": 1,
        }
        _install_handler(mock_build, lambda request: _json_response(payload))

        async with Client(mcp) as c:
            result = await c.call_tool("list_connections", {})

        conn = result.data["connections"][0]
        assert conn["connection_id"] == "my_db"
        assert "password" not in conn
        assert "extra" not in conn
        assert "s3cret" not in json.dumps(result.data)

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_list_variables_omits_values(self, mock_build):
        payload = {
            "variables": [
                {"key": "api_key", "value": "super-secret", "is_encrypted": True, "description": "k"}
            ],
            "total_entries": 1,
        }
        _install_handler(mock_build, lambda request: _json_response(payload))

        async with Client(mcp) as c:
            result = await c.call_tool("list_variables", {})

        var = result.data["variables"][0]
        assert var["key"] == "api_key"
        assert "value" not in var
        assert "super-secret" not in json.dumps(result.data)


class TestTriggerDagRun:
    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_body_shape(self, mock_build, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AIRFLOW_MCP_ALLOW_WRITES", "true")
        seen_bodies = []

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags/my_dag/dagRuns"
            seen_bodies.append(json.loads(request.content))
            return _json_response({"dag_run_id": "generated"}, status_code=200)

        _install_handler(mock_build, handler)

        async with Client(mcp) as c:
            result = await c.call_tool(
                "trigger_dag_run", {"dag_id": "my_dag", "conf": {"key": "value"}, "note": "manual run"}
            )

        assert seen_bodies == [{"logical_date": None, "conf": {"key": "value"}, "note": "manual run"}]
        assert result.data == {"dag_run_id": "generated"}

    async def test_blocked_without_allow_writes(self):
        async with Client(mcp) as c:
            with pytest.raises(ToolError, match="read-only"):
                await c.call_tool("trigger_dag_run", {"dag_id": "my_dag"})


class TestAirflowApiCallDeleteGate:
    async def test_delete_blocked_when_only_writes_enabled(self, monkeypatch: pytest.MonkeyPatch):
        # Enabling writes must not enable deletions -- DELETE has its own stricter gate.
        monkeypatch.setenv("AIRFLOW_MCP_ALLOW_WRITES", "true")
        async with Client(mcp) as c:
            with pytest.raises(ToolError, match="DELETE"):
                await c.call_tool("airflow_api_call", {"method": "DELETE", "path": "/api/v2/dags/my_dag"})

    @mock.patch("airflow_mcp_server.client._build_http_client", autospec=True)
    async def test_delete_allowed_with_deletes_flag(self, mock_build, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("AIRFLOW_MCP_ALLOW_DELETES", "true")

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "DELETE"
            assert request.url.path == "/api/v2/dags/my_dag"
            return _json_response({"ok": True}, status_code=200)

        _install_handler(mock_build, handler)

        async with Client(mcp) as c:
            result = await c.call_tool(
                "airflow_api_call", {"method": "DELETE", "path": "/api/v2/dags/my_dag"}
            )

        assert result.data == {"ok": True}
