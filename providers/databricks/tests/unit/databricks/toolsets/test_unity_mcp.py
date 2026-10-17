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
import importlib
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import pytest

pytest.importorskip("fastmcp")
pytest.importorskip("airflow.providers.common.ai")

from pydantic_ai import ModelRetry, RunContext

from airflow.models import Connection
from airflow.providers.common.ai.utils import toolset_base
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.databricks.exceptions import (
    DatabricksUnityMCPAccessDeniedError,
    DatabricksUnityMCPError,
    DatabricksUnityMCPThrottledError,
    DatabricksUnityMCPTransportError,
)
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.toolsets.unity_mcp import DatabricksUnityMCPToolset

CONN_ID = "databricks_unity"
SERVICE = "main.tools.genie"

# What Unity Gateway answers, at HTTP 403, for a service the caller may not invoke or that does not exist.
NOT_AUTHORIZED = {"code": -32007, "message": "Not authorized to invoke MCP service."}
INVALID_PARAMS = {"code": -32602, "message": "Invalid params: 'text' is required"}


class _StubGateway(BaseHTTPRequestHandler):
    """
    A Unity Gateway stand-in that serves one MCP Service with a single ``echo`` tool.

    A call of ``echo`` whose text is ``"reply <status>"`` gets that HTTP status with an empty body,
    and ``"reply <status> <error>"`` gets it with the JSON-RPC error named by ``<error>``.
    """

    protocol_version = "HTTP/1.1"
    errors = {"not_authorized": NOT_AUTHORIZED, "invalid_params": INVALID_PARAMS}
    # Per-test behaviour, set by the ``gateway`` fixture: a reply for every request to a service,
    # a status for notifications only, and a barrier tool calls wait at, so that they overlap.
    service_reply: dict[str, tuple[int, dict[str, str], str | None]] = {}
    notification_status: int | None = None
    tool_call_barrier: threading.Barrier | None = None
    # Seconds to hold the answer to ``initialize``; ``release`` ends any wait early on teardown.
    initialize_delay: float = 0
    release = threading.Event()
    requests: list[tuple[str, str | None, str | None]] = []

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
        method = body.get("method")
        self.requests.append((self.path, self.headers.get("Authorization"), method))
        service = self.path.rsplit("/", 1)[-1]
        if service in self.service_reply:
            status, headers, error = self.service_reply[service]
            return self._reply_error(status, body.get("id"), error, headers)
        if "id" not in body:
            return self._reply(self.notification_status or 202)
        if method == "initialize":
            self.release.wait(self.initialize_delay)
            result = {
                "protocolVersion": body["params"]["protocolVersion"],
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "stub", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "echo",
                        "description": "Echo the text back.",
                        "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}},
                    }
                ]
            }
        elif method == "tools/call":
            if self.tool_call_barrier:
                self.tool_call_barrier.wait()
            if "text" not in body["params"]["arguments"]:
                return self._reply(200, {"jsonrpc": "2.0", "id": body["id"], "error": INVALID_PARAMS})
            text = body["params"]["arguments"]["text"]
            if text.startswith("reply "):
                status, _, error = text.removeprefix("reply ").partition(" ")
                return self._reply_error(int(status), body["id"], error or None)
            result = {"content": [{"type": "text", "text": f"echo: {text}"}], "isError": False}
        else:
            result = {}
        self._reply(200, {"jsonrpc": "2.0", "id": body["id"], "result": result})

    def do_GET(self):
        self._reply(405)

    def do_DELETE(self):
        self._reply(200)

    def _reply_error(self, status, request_id, error, headers=None):
        payload = {"jsonrpc": "2.0", "id": request_id, "error": self.errors[error]} if error else None
        self._reply(status, payload, headers)

    def _reply(self, status, payload=None, headers=None):
        data = json.dumps(payload).encode() if payload is not None else b""
        self.send_response(status)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        if payload is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def gateway():
    _StubGateway.service_reply = {}
    _StubGateway.notification_status = None
    _StubGateway.tool_call_barrier = None
    _StubGateway.initialize_delay = 0
    _StubGateway.release = threading.Event()
    _StubGateway.requests = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StubGateway)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    _StubGateway.release.set()
    server.shutdown()
    server.server_close()


@pytest.fixture
def gateway_conn(gateway, create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id=CONN_ID,
            conn_type="databricks",
            host="127.0.0.1",
            port=gateway.server_address[1],
            schema="http",
            password="dapi-token",
        )
    )


def _run(coro):
    return asyncio.run(coro)


def _ctx():
    ctx = mock.MagicMock(spec=RunContext)
    ctx.max_retries = 1
    return ctx


async def _list_and_call(toolset, text="hi"):
    async with toolset:
        tools = await toolset.get_tools(_ctx())
        result = await toolset.execute_tool("echo", {"text": text}, ctx=_ctx(), tool=tools["echo"])
    return tools, result


async def _call_concurrently(*texts):
    """Call ``echo`` once per text, all at the same time, and return each call's result or exception."""
    toolset = DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)
    async with toolset:
        tools = await toolset.get_tools(_ctx())
        return await asyncio.gather(
            *(toolset.execute_tool("echo", {"text": t}, ctx=_ctx(), tool=tools["echo"]) for t in texts),
            return_exceptions=True,
        )


class TestServiceName:
    @pytest.mark.parametrize(
        "name",
        [
            "main.tools",
            "main.tools.genie.extra",
            "main.tools.genie/../../api/2.0/jobs",
            "main.tools.genie?x=1",
            "main.tools.genie#frag",
            "main.tools.ge nie",
            "main.tools.my-genie",
            "main..genie",
            "evil.example.com/main.tools.genie",
            "",
        ],
    )
    def test_rejects_names_that_are_not_three_safe_parts(self, name):
        with pytest.raises(ValueError, match="catalog.schema.service"):
            DatabricksUnityMCPToolset(name)

    def test_templated_name_is_validated_once_rendered(self, gateway_conn):
        toolset = DatabricksUnityMCPToolset("{{ params.service }}", databricks_conn_id=CONN_ID)
        toolset._service_name = "main.tools/../../api"

        with pytest.raises(ValueError, match="catalog.schema.service"):
            toolset._get_server()

    def test_id_names_connection_and_service(self):
        assert (
            DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID).id
            == f"databricks-unity-mcp-{CONN_ID}-{SERVICE}"
        )

    def test_connection_and_service_are_agent_template_fields(self):
        assert set(DatabricksUnityMCPToolset.agent_template_fields) == {
            "_databricks_conn_id",
            "_service_name",
        }


class TestServiceUrl:
    @pytest.mark.parametrize(
        ("host", "extra", "expected"),
        [
            ("xx.cloud.databricks.com", None, "https://xx.cloud.databricks.com"),
            ("https://xx.cloud.databricks.com/", None, "https://xx.cloud.databricks.com"),
            (None, {"host": "https://yy.azuredatabricks.net"}, "https://yy.azuredatabricks.net"),
        ],
    )
    def test_url_is_built_on_the_connection_host(self, create_connection_without_db, host, extra, expected):
        create_connection_without_db(
            Connection(conn_id=CONN_ID, conn_type="databricks", host=host, password="t", extra=extra)
        )
        toolset = DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)

        assert (
            toolset.get_service_url(DatabricksHook(CONN_ID))
            == f"{expected}/ai-gateway/mcp-services/{SERVICE}"
        )

    def test_connection_without_host_is_rejected(self, create_connection_without_db):
        create_connection_without_db(Connection(conn_id=CONN_ID, conn_type="databricks", password="t"))
        toolset = DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)

        with pytest.raises(ValueError, match="no workspace host"):
            toolset.get_service_url(DatabricksHook(CONN_ID))

    @pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "[::1]"])
    def test_plain_http_is_allowed_to_loopback_only(self, create_connection_without_db, host):
        create_connection_without_db(
            Connection(conn_id=CONN_ID, conn_type="databricks", host=host, schema="http", password="t")
        )
        toolset = DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)

        assert toolset.get_service_url(DatabricksHook(CONN_ID)).startswith(f"http://{host}/")

    def test_plain_http_to_a_workspace_is_rejected(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID,
                conn_type="databricks",
                host="xx.cloud.databricks.com",
                schema="http",
                password="t",
            )
        )
        toolset = DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)

        with pytest.raises(ValueError, match="only sent over HTTPS"):
            toolset.get_service_url(DatabricksHook(CONN_ID))

    def test_requests_go_through_the_connection_proxy(self, gateway, create_connection_without_db):
        # The stub gateway doubles as the proxy: a proxied request names the full URL in its path.
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID,
                conn_type="databricks",
                host="127.0.0.2",
                port=1,
                schema="http",
                password="dapi-token",
                extra={"proxies": {"http": f"http://127.0.0.1:{gateway.server_address[1]}"}},
            )
        )

        _, result = _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))

        assert result == "echo: hi"
        assert {path for path, _, _ in _StubGateway.requests} == {
            f"http://127.0.0.2:1/ai-gateway/mcp-services/{SERVICE}"
        }


class TestAuthentication:
    def test_tools_are_discovered_and_called_with_the_connection_token(self, gateway, gateway_conn):
        tools, result = _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))

        assert list(tools) == ["echo"]
        assert result == "echo: hi"
        assert _StubGateway.requests
        assert {path for path, _, _ in _StubGateway.requests} == {f"/ai-gateway/mcp-services/{SERVICE}"}
        assert {auth for _, auth, _ in _StubGateway.requests} == {"Bearer dapi-token"}

    @mock.patch.object(DatabricksHook, "_get_token", autospec=True)
    def test_token_is_fetched_for_every_request_so_it_can_refresh(self, get_token, gateway, gateway_conn):
        tokens = (f"oauth-{i}" for i in range(1000))
        get_token.side_effect = lambda *_, **__: next(tokens)
        _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))

        sent = [auth for _, auth, _ in _StubGateway.requests]
        # One token is fetched up front to fail fast, then one per request.
        assert sent == [f"Bearer oauth-{i}" for i in range(1, len(sent) + 1)]

    def test_requests_do_not_wait_for_other_toolsets_blocking_calls(self, gateway, gateway_conn):
        async def call_while_another_toolset_holds_the_lock():
            toolset = DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)
            async with toolset:
                tools = await toolset.get_tools(_ctx())
                with toolset_base._blocking_call_lock:
                    return await asyncio.wait_for(
                        toolset.execute_tool("echo", {"text": "hi"}, ctx=_ctx(), tool=tools["echo"]),
                        timeout=10,
                    )

        assert _run(call_while_another_toolset_holds_the_lock()) == "echo: hi"

    @mock.patch("airflow.providers.databricks.toolsets.unity_mcp.mask_secret", autospec=True)
    def test_minted_tokens_are_masked(self, mask_secret, gateway, gateway_conn):
        _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))

        mask_secret.assert_called_with("dapi-token")

    def test_username_and_password_connection_is_rejected(self, create_connection_without_db):
        create_connection_without_db(
            Connection(conn_id=CONN_ID, conn_type="databricks", host="h", login="user", password="pass")
        )

        with pytest.raises(ValueError, match="sends a bearer token"):
            DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)._get_server()

    @mock.patch.object(DatabricksHook, "_get_token", autospec=True)
    def test_token_failure_during_the_run_is_reported(self, get_token, gateway, gateway_conn):
        # The up-front check and the first request get a token; the next request's fetch fails.
        get_token.side_effect = ["dapi-token", "dapi-token", OSError("token endpoint unreachable")]

        with pytest.raises(DatabricksUnityMCPError, match="Could not get a token") as err:
            _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))

        assert "token endpoint unreachable" in str(err.value)


class TestErrors:
    @pytest.mark.parametrize(
        ("status", "headers", "error", "expected", "retry_after"),
        [
            (401, {}, None, DatabricksUnityMCPAccessDeniedError, None),
            (403, {}, None, DatabricksUnityMCPAccessDeniedError, None),
            (403, {}, "not_authorized", DatabricksUnityMCPAccessDeniedError, None),
            (404, {}, None, DatabricksUnityMCPError, None),
            (429, {"Retry-After": "12"}, None, DatabricksUnityMCPThrottledError, 12.0),
            (
                429,
                {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"},
                None,
                DatabricksUnityMCPThrottledError,
                None,
            ),
        ],
    )
    def test_gateway_errors_while_connecting_are_translated(
        self, gateway, gateway_conn, status, headers, error, expected, retry_after
    ):
        _StubGateway.service_reply[SERVICE] = (status, headers, error)

        with pytest.raises(expected) as err:
            _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))

        assert type(err.value) is expected
        assert err.value.http_status_code == status
        if expected is DatabricksUnityMCPThrottledError:
            assert err.value.retry_after == retry_after

    def test_missing_service_is_reported_as_access_denied(self, gateway, gateway_conn):
        _StubGateway.service_reply[SERVICE] = (403, {}, "not_authorized")

        with pytest.raises(DatabricksUnityMCPAccessDeniedError, match="no service has that name"):
            _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))

    def test_server_error_during_a_tool_call_says_the_call_may_have_run(self, gateway, gateway_conn):
        with pytest.raises(DatabricksUnityMCPError, match="may or may not have run") as err:
            _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID), "reply 503"))

        assert type(err.value) is DatabricksUnityMCPError
        assert err.value.http_status_code == 503
        # The failed call was sent once and not retried.
        assert [m for _, _, m in _StubGateway.requests].count("tools/call") == 1

    def test_denied_tool_call_is_not_left_to_the_model(self, gateway, gateway_conn):
        with pytest.raises(DatabricksUnityMCPAccessDeniedError):
            _run(
                _list_and_call(
                    DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID), "reply 403 not_authorized"
                )
            )

    @pytest.mark.parametrize(
        "text", ["reply 200 invalid_params", "reply 400 invalid_params", "reply 500 invalid_params"]
    )
    def test_json_rpc_error_from_the_server_is_left_to_the_model(self, gateway, gateway_conn, text):
        # A server may answer bad arguments with a JSON-RPC error at an HTTP error status; it is
        # still the server's answer, so the model can correct the call.
        with pytest.raises(ModelRetry, match="'text' is required"):
            _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID), text))

    def test_concurrent_calls_are_each_judged_by_their_own_failure(self, gateway, gateway_conn):
        # Calls run in parallel, so another call's response must not change how a failure is
        # reported: a server error stays a non-retried failure however the other call ends.
        _StubGateway.tool_call_barrier = threading.Barrier(2, timeout=10)
        for _ in range(10):
            failed, succeeded = _run(_call_concurrently("reply 503", "hi"))
            assert type(failed) is DatabricksUnityMCPError
            assert "may or may not have run" in str(failed)
            assert succeeded == "echo: hi"

            failed, throttled = _run(_call_concurrently("reply 503", "reply 429"))
            # Neither status can be attributed to its call, so neither is reported as throttling.
            assert type(failed) is DatabricksUnityMCPError
            assert type(throttled) is DatabricksUnityMCPError
            assert failed.http_status_code is None

            invalid, failed = _run(_call_concurrently("reply 400 invalid_params", "reply 503"))
            assert isinstance(invalid, ModelRetry)
            assert type(failed) is DatabricksUnityMCPError

    def test_tool_errors_after_a_tolerated_gateway_error_are_left_to_the_agent(self, gateway, gateway_conn):
        # The MCP client carries on when the gateway rejects a notification; a later tool error the
        # model can fix must still reach pydantic-ai as a retry, not as that earlier gateway error.
        _StubGateway.notification_status = 400

        async def call_with_missing_argument():
            toolset = DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)
            async with toolset:
                tools = await toolset.get_tools(_ctx())
                await toolset.execute_tool("echo", {}, ctx=_ctx(), tool=tools["echo"])

        with pytest.raises(ModelRetry, match="'text' is required"):
            _run(call_with_missing_argument())

    def test_unreachable_gateway_is_a_transport_error(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID, conn_type="databricks", host="127.0.0.1", port=1, schema="http", password="t"
            )
        )

        with pytest.raises(DatabricksUnityMCPTransportError, match="Could not reach Unity Gateway"):
            _run(_list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID)))


class TestHandshakeTimeout:
    def test_slow_handshake_within_init_timeout_connects(self, gateway, gateway_conn):
        _StubGateway.initialize_delay = 1

        _, result = _run(
            _list_and_call(DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID, init_timeout=10))
        )

        assert result == "echo: hi"

    def test_handshake_beyond_init_timeout_is_a_transport_error(self, gateway, gateway_conn):
        # Shorter than pydantic-ai's own 5 second default, so this only fails if init_timeout is used.
        _StubGateway.initialize_delay = 2

        with pytest.raises(DatabricksUnityMCPTransportError, match="within 0.5 seconds. Raise init_timeout"):
            _run(
                _list_and_call(
                    DatabricksUnityMCPToolset(SERVICE, databricks_conn_id=CONN_ID, init_timeout=0.5)
                )
            )

    @pytest.mark.parametrize("init_timeout", [0, -1])
    def test_init_timeout_must_be_positive(self, init_timeout):
        with pytest.raises(ValueError, match="init_timeout must be a positive number"):
            DatabricksUnityMCPToolset(SERVICE, init_timeout=init_timeout)


class TestFrameworkNeutralTools:
    def test_are_not_supported(self):
        with pytest.raises(NotImplementedError, match="framework-neutral"):
            DatabricksUnityMCPToolset(SERVICE).airflow_tools()


class TestOptionalDependency:
    def test_import_without_common_ai_names_the_extra(self):
        module = "airflow.providers.databricks.toolsets.unity_mcp"
        with mock.patch.dict(sys.modules, {"airflow.providers.common.ai.utils.toolset_base": None}):
            sys.modules.pop(module)
            with pytest.raises(AirflowOptionalProviderFeatureException, match=r"databricks\[common.ai\]"):
                importlib.import_module(module)
