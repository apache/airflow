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

import subprocess
import sys
from unittest import mock

import aiohttp
import pytest
import requests

from airflow.providers.common.compat.sdk import Connection
from airflow.providers.databricks.exceptions import DatabricksAgentInvocationTimeout, DatabricksApiError
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook

try:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents.base import ManagedAgentRef, ManagedAgentRequest

    HAS_COMMON_AI = True
except ImportError:
    HAS_COMMON_AI = False

APP_URL = "https://agent.databricksapps.com"
INVOCATION_ID = "550e8400-e29b-41d4-a716-446655440000"


@pytest.fixture
def hook():
    hook = DatabricksAgentHook(APP_URL, retry_limit=2, retry_delay=0)
    hook.databricks_conn = Connection(
        conn_id="databricks_default",
        conn_type="databricks",
        host="workspace.cloud.databricks.com",
        login="client-id",
        password="client-secret",
        extra='{"service_principal_oauth": true}',
    )
    hook.user_agent_header = {"user-agent": "test"}
    return hook


@pytest.mark.parametrize(
    "url",
    [
        "http://app",
        "https://user:secret@app",
        "https://app/path",
        "https://app?token=x",
        "https://app#fragment",
        "https://",
    ],
)
def test_invalid_app_url(url):
    with pytest.raises(ValueError, match="HTTPS base URL"):
        DatabricksAgentHook(url)


@pytest.mark.parametrize(
    ("extra", "login", "password"),
    [
        ("{}", "", "pat"),
        ('{"token":"pat","service_principal_oauth":true}', "", ""),
        ('{"service_principal_oauth":true}', "client", ""),
    ],
)
def test_requires_oauth(hook, extra, login, password):
    hook.databricks_conn = Connection(
        conn_id="databricks_default", conn_type="databricks", extra=extra, login=login, password=password
    )
    with pytest.raises(ValueError, match="service_principal_oauth"):
        hook.create_invocation(INVOCATION_ID, {})


@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
@pytest.mark.parametrize("session_id", [None, "conversation"])
def test_invoke(request, token, hook, session_id):
    response = mock.Mock(spec=requests.Response)
    response.status_code = 202
    response.json.return_value = {"id": INVOCATION_ID, "status_url": "ignored"}
    request.return_value = response
    assert hook.create_invocation(INVOCATION_ID, {"messages": []}, session_id) == response.json.return_value
    payload = {"id": INVOCATION_ID, "input": {"messages": []}, "background": True}
    headers = {"user-agent": "test", "Authorization": "Bearer oauth"}
    if session_id:
        payload["session_id"] = session_id
        headers["X-Routing-Key"] = session_id
    request.assert_called_once_with(
        "POST",
        f"{APP_URL}/api/invocations",
        json=payload,
        headers=headers,
        timeout=180,
        allow_redirects=False,
    )
    token.assert_called_once_with(hook, "https://workspace.cloud.databricks.com/oidc/v1/token")


@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
def test_get_retries_transient_error(request, token, hook):
    response = mock.Mock(spec=requests.Response)
    response.status_code = 200
    response.json.return_value = {"status": "completed", "output": "answer"}
    request.side_effect = [requests.ConnectionError("connection dropped"), response]
    assert hook.get_invocation(INVOCATION_ID) == response.json.return_value
    assert request.call_count == 2
    assert request.call_args.args == ("GET", f"{APP_URL}/api/invocations/{INVOCATION_ID}")
    assert request.call_args.kwargs["json"] is None


@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
@pytest.mark.parametrize("status", [302, 403, 409, 429, 500])
def test_http_errors(request, token, hook, status):
    response = mock.Mock(spec=requests.Response)
    response.status_code = status
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    request.return_value = response
    with pytest.raises(DatabricksApiError):
        hook.get_invocation(INVOCATION_ID)
    assert request.call_count == (2 if status in (429, 500) else 1)


@pytest.mark.parametrize("method", ["create_invocation", "get_invocation"])
def test_invalid_invocation_id(hook, method):
    with pytest.raises(ValueError, match="badly formed hexadecimal UUID"):
        getattr(hook, method)("../../other", **({"input": {}} if method == "create_invocation" else {}))


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@mock.patch("airflow.providers.databricks.hooks.databricks_base.get_async_connection", autospec=True)
@pytest.mark.parametrize("cached_connection", [True, False])
@pytest.mark.parametrize("status", [200, 302, 403, 429, 500])
@pytest.mark.asyncio
async def test_async_get(get_connection, session, token, hook, status, cached_connection):
    get_connection.return_value = hook.databricks_conn
    if not cached_connection:
        del hook.databricks_conn
    response = mock.Mock(spec=aiohttp.ClientResponse)
    response.status = status
    response.json = mock.AsyncMock(spec=aiohttp.ClientResponse.json, return_value={"status": "completed"})
    if status >= 400:
        response.raise_for_status.side_effect = aiohttp.ClientResponseError(
            mock.Mock(spec=aiohttp.RequestInfo), (), status=status
        )
    session.return_value.get.return_value.__aenter__.return_value = response
    async with hook:
        if status == 200:
            assert await hook.a_get_invocation(INVOCATION_ID, "conversation") == {"status": "completed"}
        else:
            with pytest.raises(DatabricksApiError):
                await hook.a_get_invocation(INVOCATION_ID, "conversation")
    assert session.return_value.get.call_count == (2 if status in (429, 500) else 1)
    assert session.return_value.get.call_args.args == (f"{APP_URL}/api/invocations/{INVOCATION_ID}",)
    assert session.return_value.get.call_args.kwargs["headers"]["X-Routing-Key"] == "conversation"
    session.return_value.close.assert_awaited_once()
    if cached_connection:
        get_connection.assert_not_awaited()
    else:
        get_connection.assert_awaited_once_with("databricks_default", hook=hook)


@pytest.mark.skipif(not HAS_COMMON_AI, reason="requires apache-airflow-providers-common-ai")
class TestManagedAgent:
    def test_resolve_and_capabilities(self, hook):
        agent = hook.agent(APP_URL + "/")
        assert agent.ref == ManagedAgentRef(platform="databricks.agent_runtime", name=APP_URL)
        assert agent.capabilities.sessions
        assert agent.capabilities.structured_output
        assert agent.capabilities.trace
        assert not agent.capabilities.usage

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    @pytest.mark.parametrize("kind", ["prompt", "messages", "input_key"])
    def test_synchronous_request(self, http, token, hook, kind):
        response = mock.Mock(spec=requests.Response)
        response.status_code = 200
        raw = {"id": INVOCATION_ID, "status": "completed", "output": "answer", "session_id": "conversation"}
        response.json.return_value = raw
        http.return_value = response
        options = {"invocation_id": INVOCATION_ID}
        if kind == "input_key":
            options["input_key"] = "question"
        request = ManagedAgentRequest(
            prompt=None if kind == "messages" else "hello",
            messages=[{"role": "user", "content": "hello"}] if kind == "messages" else None,
            session_id="conversation",
            timeout=12.5,
            vendor_options=options,
        )
        result = hook.agent("https://other.databricksapps.com").invoke(request)
        kwargs = http.call_args.kwargs
        assert http.call_args.args == ("POST", "https://other.databricksapps.com/api/invocations")
        assert kwargs["json"] == {
            "id": INVOCATION_ID,
            "input": {"question": "hello"} if kind == "input_key" else {"messages": request.as_messages()},
            "session_id": "conversation",
        }
        assert 0 < kwargs["timeout"] <= 12.5
        assert kwargs["headers"]["X-Routing-Key"] == "conversation"
        assert result.text == "answer"
        assert result.raw == raw
        assert result.structured is None
        assert result.session_id == "conversation"
        assert result.trace_ref == INVOCATION_ID
        assert hook.app_url == APP_URL

    @mock.patch.object(DatabricksAgentHook, "_do_agent_api_call", autospec=True)
    @pytest.mark.parametrize(
        ("output", "text", "structured"),
        [
            (None, "", None),
            ("plain", "plain", None),
            ({"output": "42", "steps": 3}, "42", {"output": "42", "steps": 3}),
            ({"answer": 42}, '{"answer": 42}', {"answer": 42}),
            ([1, 2], "[1, 2]", [1, 2]),
        ],
    )
    def test_response_and_generated_identity(self, call, output, text, structured):
        call.return_value = {"status": "completed", "output": output}
        hook = DatabricksAgentHook()
        first = hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        second = hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        assert first.text == text
        assert first.structured == structured
        assert first.raw == call.return_value
        assert first.trace_ref != second.trace_ref
        assert first.session_id is None
        assert "session_id" not in call.call_args.args[3]

    @mock.patch.object(DatabricksAgentHook, "_do_agent_api_call", autospec=True)
    @pytest.mark.parametrize(
        "raw", [{"status": "failed"}, {"status": "interrupted"}, {"status": "running"}, {}, []]
    )
    def test_requires_completed_answer(self, call, hook, raw):
        call.return_value = raw
        with pytest.raises(ManagedAgentInvocationError, match="did not complete"):
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))

    @mock.patch.object(DatabricksAgentHook, "_do_agent_api_call", autospec=True)
    @pytest.mark.parametrize(
        "options",
        [
            {"app_url": APP_URL},
            {"background": True},
            {"input_key": 3},
            {"input_key": ""},
            {"invocation_id": "bad"},
        ],
    )
    def test_invalid_options(self, call, hook, options):
        with pytest.raises(ValueError, match="vendor_options|UUID"):
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello", vendor_options=options))
        call.assert_not_called()

    @mock.patch.object(DatabricksAgentHook, "_do_agent_api_call", autospec=True)
    def test_input_key_requires_prompt(self, call, hook):
        with pytest.raises(ValueError, match="only applies to a prompt"):
            hook.agent(APP_URL).invoke(
                ManagedAgentRequest(messages=[], vendor_options={"input_key": "question"})
            )
        call.assert_not_called()

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    @pytest.mark.parametrize("status", [302, 400, 401, 403, 404, 409, 429, 500])
    def test_http_error_classification(self, http, token, hook, status):
        response = mock.Mock(spec=requests.Response)
        response.status_code = status
        error = requests.HTTPError(response=response)
        response.raise_for_status.side_effect = error
        http.return_value = response
        expected = requests.HTTPError if status in (429, 500) else ManagedAgentInvocationError
        with pytest.raises(expected) as exc:
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        if status in (429, 500):
            assert exc.value is error
        else:
            assert isinstance(exc.value.__cause__, DatabricksApiError)

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_transient_connection_failure(self, http, token, hook):
        error = requests.ConnectionError("disconnected")
        http.side_effect = error
        with pytest.raises(requests.ConnectionError) as exc:
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        assert exc.value is error

    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests.post", autospec=True)
    @pytest.mark.parametrize("status", [400, 401, 403, 429, 500])
    def test_oauth_http_error_classification(self, token_http, agent_http, hook, status):
        response = mock.Mock(spec=requests.Response)
        response.status_code = status
        response.content = b"OAuth request failed"
        error = requests.HTTPError(response=response)
        response.raise_for_status.side_effect = error
        token_http.return_value = response
        expected = requests.HTTPError if status in (429, 500) else ManagedAgentInvocationError
        with pytest.raises(expected) as exc:
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        if status in (429, 500):
            assert exc.value is error
        else:
            assert exc.value.__cause__.__context__ is error
        agent_http.assert_not_called()

    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests.post", autospec=True)
    @pytest.mark.parametrize("error", [requests.ConnectionError("disconnected"), requests.Timeout("timeout")])
    def test_oauth_transient_transport_failure(self, token_http, agent_http, hook, error):
        token_http.side_effect = error
        with pytest.raises(type(error)) as exc:
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        assert exc.value is error
        agent_http.assert_not_called()

    @mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_rejects_late_synchronous_answer(self, http, token, clock, hook):
        clock.monotonic.return_value = 0
        response = mock.Mock(spec=requests.Response)
        response.status_code = 200
        response.json.return_value = {"status": "completed", "output": "answer"}

        def get_response(*args, **kwargs):
            clock.monotonic.return_value = 1
            return response

        http.side_effect = get_response
        with pytest.raises(DatabricksAgentInvocationTimeout):
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello", timeout=1))
        assert http.call_args.kwargs["timeout"] == 1

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True)
    @pytest.mark.parametrize("timeout", [0, -1])
    def test_invalid_request_timeout(self, token, hook, timeout):
        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello", timeout=timeout))
        token.assert_not_called()


def test_background_requires_app_url():
    with pytest.raises(ValueError, match="app_url is required"):
        DatabricksAgentHook().create_invocation(INVOCATION_ID, {})


def test_without_common_ai():
    script = (
        "import sys\n"
        "from unittest.mock import patch\n"
        "sys.modules['airflow.providers.common.ai.managed_agents.base'] = None\n"
        "from airflow.providers.databricks.hooks.agent import DatabricksAgentHook\n"
        "from airflow.providers.databricks.operators.agent import DatabricksAgentInvokeOperator\n"
        "from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException\n"
        "hook = DatabricksAgentHook('https://agent.databricksapps.com')\n"
        "DatabricksAgentInvokeOperator(task_id='invoke', app_url=hook.app_url, input={})\n"
        "with patch.object(hook, '_do_agent_api_call', autospec=True, return_value={'status': 'completed'}):\n"
        f"    assert hook.create_invocation('{INVOCATION_ID}', {{}}) == {{'status': 'completed'}}\n"
        "try:\n"
        "    hook.agent(hook.app_url)\n"
        "except AirflowOptionalProviderFeatureException as e:\n"
        "    assert 'databricks[common.ai]' in str(e)\n"
        "    assert isinstance(e.__cause__, ImportError)\n"
        "else:\n"
        "    raise SystemExit('agent() did not raise')\n"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


@mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
@pytest.mark.parametrize(("budget", "request_timeout"), [(10, 10), (200, 180)])
def test_poll_request_budget(request, token, clock, hook, budget, request_timeout):
    clock.monotonic.return_value = 0
    response = mock.Mock(spec=requests.Response)
    response.status_code = 200
    response.json.return_value = {"status": "completed"}
    request.return_value = response
    assert hook.get_invocation(INVOCATION_ID, timeout_seconds=budget) == {"status": "completed"}
    assert request.call_args.kwargs["timeout"] == request_timeout


@pytest.mark.parametrize("budget", [0, -1])
def test_invalid_poll_budget(hook, budget):
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        hook.get_invocation(INVOCATION_ID, timeout_seconds=budget)


@mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True)
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
@pytest.mark.parametrize("phase", ["before_token", "after_token", "after_request"])
def test_expired_poll_budget(request, token, clock, hook, phase):
    clock.monotonic.return_value = 0
    response = mock.Mock(spec=requests.Response)
    response.status_code = 200
    response.json.return_value = {"status": "completed"}
    request.return_value = response
    if phase == "before_token":
        clock.monotonic.side_effect = [0, 1]
    elif phase == "after_token":

        def get_token(*args):
            clock.monotonic.return_value = 1
            return "oauth"

        token.side_effect = get_token
    else:

        def get_response(*args, **kwargs):
            clock.monotonic.return_value = 1
            return response

        request.side_effect = get_response
    with pytest.raises(DatabricksAgentInvocationTimeout, match=INVOCATION_ID):
        hook.get_invocation(INVOCATION_ID, timeout_seconds=1)
    if phase == "after_request":
        request.assert_called_once()
    else:
        request.assert_not_called()


@mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
@pytest.mark.parametrize("budget", [None, 0.5, 10])
def test_retry_budget(request, token, clock, hook, budget):
    clock.monotonic.return_value = 0
    request.side_effect = requests.ConnectionError("unavailable")
    hook.retry_args["sleep"] = mock.Mock(spec=["__call__"])
    expected = DatabricksAgentInvocationTimeout if budget == 0.5 else DatabricksApiError
    with pytest.raises(expected):
        hook.get_invocation(INVOCATION_ID, timeout_seconds=budget)
    assert request.call_count == (1 if budget == 0.5 else 2)
    if budget == 0.5:
        hook.retry_args["sleep"].assert_not_called()


@mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
def test_retry_recomputes_request_budget(request, token, clock, hook):
    clock.monotonic.return_value = 0
    hook.retry_args["sleep"] = mock.Mock(spec=["__call__"])
    response = mock.Mock(spec=requests.Response)
    response.status_code = 200
    response.json.return_value = {"status": "completed"}

    def get_response(*args, **kwargs):
        if request.call_count == 1:
            clock.monotonic.return_value = 4
            raise requests.ConnectionError("disconnected")
        return response

    request.side_effect = get_response
    assert hook.get_invocation(INVOCATION_ID, timeout_seconds=10) == {"status": "completed"}
    assert [call.kwargs["timeout"] for call in request.call_args_list] == [10, 6]


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@pytest.mark.parametrize(
    "error", [aiohttp.ServerDisconnectedError(), aiohttp.ClientOSError(104, "reset"), ConnectionResetError()]
)
@pytest.mark.asyncio
async def test_async_retries_disconnect(session, token, hook, error):
    response = mock.Mock(spec=aiohttp.ClientResponse)
    response.status = 200
    response.json = mock.AsyncMock(spec=aiohttp.ClientResponse.json, return_value={"status": "completed"})
    session.return_value.get.return_value.__aenter__.side_effect = [error, response]
    async with hook:
        assert await hook.a_get_invocation(INVOCATION_ID) == {"status": "completed"}
    assert session.return_value.get.call_count == 2
    session.return_value.close.assert_awaited_once()


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@pytest.mark.parametrize(
    "error",
    [
        aiohttp.ServerDisconnectedError(),
        aiohttp.InvalidURL("invalid"),
        aiohttp.ClientPayloadError("malformed"),
    ],
)
@pytest.mark.asyncio
async def test_async_transport_failure(session, token, hook, error):
    session.return_value.get.return_value.__aenter__.side_effect = error
    async with hook:
        with pytest.raises(
            DatabricksApiError if isinstance(error, aiohttp.ServerDisconnectedError) else type(error)
        ):
            await hook.a_get_invocation(INVOCATION_ID)
    assert session.return_value.get.call_count == (
        2 if isinstance(error, aiohttp.ServerDisconnectedError) else 1
    )
    session.return_value.close.assert_awaited_once()
