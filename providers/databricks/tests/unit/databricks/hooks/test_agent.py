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
from uuid import UUID

import aiohttp
import pytest
import requests

from airflow.providers.common.compat.sdk import Connection
from airflow.providers.databricks.exceptions import DatabricksAgentInvocationTimeout, DatabricksApiError
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook

try:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents.base import ManagedAgentRef, ManagedAgentRequest
    from airflow.providers.common.ai.toolsets.managed_agent import ManagedAgentToolset

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
@pytest.mark.asyncio
async def test_async_get_cached_connection(get_connection, session, token, hook):
    response = mock.Mock(spec=aiohttp.ClientResponse)
    response.status = 200
    response.json = mock.AsyncMock(spec=aiohttp.ClientResponse.json, return_value={"status": "completed"})
    session.return_value.get.return_value.__aenter__.return_value = response
    async with hook:
        assert await hook.a_get_invocation(INVOCATION_ID, "conversation") == {"status": "completed"}
    session.return_value.get.assert_called_once()
    assert session.return_value.get.call_args.args == (f"{APP_URL}/api/invocations/{INVOCATION_ID}",)
    assert session.return_value.get.call_args.kwargs["headers"]["X-Routing-Key"] == "conversation"
    session.return_value.close.assert_awaited_once()
    get_connection.assert_not_awaited()


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@mock.patch("airflow.providers.databricks.hooks.databricks_base.get_async_connection", autospec=True)
@pytest.mark.asyncio
async def test_async_get_loads_connection(get_connection, session, token, hook):
    get_connection.return_value = hook.databricks_conn
    del hook.databricks_conn
    response = mock.Mock(spec=aiohttp.ClientResponse)
    response.status = 200
    response.json = mock.AsyncMock(spec=aiohttp.ClientResponse.json, return_value={"status": "completed"})
    session.return_value.get.return_value.__aenter__.return_value = response
    async with hook:
        assert await hook.a_get_invocation(INVOCATION_ID) == {"status": "completed"}
    get_connection.assert_awaited_once_with("databricks_default", hook=hook)
    session.return_value.close.assert_awaited_once()


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@pytest.mark.parametrize("status", [429, 500])
@pytest.mark.asyncio
async def test_async_get_retries_http_error(session, token, hook, status):
    response = mock.Mock(spec=aiohttp.ClientResponse)
    response.status = status
    response.raise_for_status.side_effect = aiohttp.ClientResponseError(
        mock.Mock(spec=aiohttp.RequestInfo), (), status=status
    )
    session.return_value.get.return_value.__aenter__.return_value = response
    async with hook:
        with pytest.raises(DatabricksApiError):
            await hook.a_get_invocation(INVOCATION_ID)
    assert session.return_value.get.call_count == 2
    session.return_value.close.assert_awaited_once()


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@pytest.mark.parametrize("status", [302, 403])
@pytest.mark.asyncio
async def test_async_get_does_not_retry_terminal_http_error(session, token, hook, status):
    response = mock.Mock(spec=aiohttp.ClientResponse)
    response.status = status
    response.raise_for_status.side_effect = aiohttp.ClientResponseError(
        mock.Mock(spec=aiohttp.RequestInfo), (), status=status
    )
    session.return_value.get.return_value.__aenter__.return_value = response
    async with hook:
        with pytest.raises(DatabricksApiError):
            await hook.a_get_invocation(INVOCATION_ID)
    session.return_value.get.assert_called_once()
    session.return_value.close.assert_awaited_once()


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
    def test_response_normalization(self, call, output, text, structured):
        call.return_value = {"status": "completed", "output": output}
        hook = DatabricksAgentHook()
        first = hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        assert first.text == text
        assert first.structured == structured
        assert first.raw == call.return_value

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_toolset_creates_one_shot_sessions(self, http, token, hook):
        response = mock.Mock(spec=requests.Response)
        response.status_code = 200
        response.json.return_value = {"status": "completed", "output": "answer"}
        http.return_value = response
        toolset = ManagedAgentToolset(hook.agent(APP_URL), tool_name="ask_agent", timeout=12)

        assert toolset.invoke_sync("hello") == "answer"
        assert toolset.invoke_sync("another question") == "answer"

        first, second = http.call_args_list
        assert first.kwargs["json"]["id"] != second.kwargs["json"]["id"]
        for call, prompt in zip(http.call_args_list, ["hello", "another question"]):
            payload = call.kwargs["json"]
            assert str(UUID(payload["id"])) == payload["id"]
            assert payload == {
                "id": payload["id"],
                "session_id": payload["id"],
                "input": {"messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}]},
            }
            assert call.args == ("POST", f"{APP_URL}/api/invocations")
            assert call.kwargs["headers"]["X-Routing-Key"] == payload["session_id"]
            assert 0 < call.kwargs["timeout"] <= 12

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_returns_generated_session(self, http, token, hook):
        response = mock.Mock(spec=requests.Response)
        response.status_code = 200
        response.json.return_value = {"id": INVOCATION_ID, "status": "completed", "output": "answer"}
        http.return_value = response
        result = hook.agent(APP_URL).invoke(
            ManagedAgentRequest(prompt="hello", vendor_options={"invocation_id": INVOCATION_ID})
        )
        assert result.session_id == INVOCATION_ID
        assert http.call_args.kwargs["json"]["session_id"] == INVOCATION_ID

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_toolset_rejects_template_interruption(self, http, token, hook):
        response = mock.Mock(spec=requests.Response)
        response.status_code = 200
        # The CLI adapters put their pause status inside the completed runtime envelope.
        response.json.return_value = {
            "id": INVOCATION_ID,
            "status": "completed",
            "output": {"status": "interrupted", "output": [{"type": "interrupt"}]},
        }
        http.return_value = response
        toolset = ManagedAgentToolset(hook.agent(APP_URL), tool_name="ask_agent")
        with pytest.raises(ManagedAgentInvocationError):
            toolset.invoke_sync("hello")

    @mock.patch.object(DatabricksAgentHook, "_do_agent_api_call", autospec=True)
    @pytest.mark.parametrize("raw", [{"status": "queued"}, {"status": "active"}, {}, []])
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
    @pytest.mark.parametrize("status", [302, 400, 401, 403, 404, 409])
    def test_terminal_http_error(self, http, token, hook, status):
        response = mock.Mock(spec=requests.Response)
        response.status_code = status
        error = requests.HTTPError(response=response)
        response.raise_for_status.side_effect = error
        http.return_value = response
        with pytest.raises(ManagedAgentInvocationError) as exc:
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        assert isinstance(exc.value.__cause__, DatabricksApiError)
        http.assert_called_once()

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_rate_limit_remains_transient(self, http, token, hook):
        response = mock.Mock(spec=requests.Response)
        response.status_code = 429
        error = requests.HTTPError(response=response)
        response.raise_for_status.side_effect = error
        http.return_value = response
        with pytest.raises(requests.HTTPError) as exc:
            hook.agent(APP_URL).invoke(ManagedAgentRequest(prompt="hello"))
        assert exc.value is error

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_sync_failure_is_terminal_after_status_lookup(self, http, token, hook):
        failed_post = mock.Mock(spec=requests.Response)
        failed_post.status_code = 500
        failed_post.json.return_value = {"detail": "agent invocation failed"}
        failed_post.raise_for_status.side_effect = requests.HTTPError(response=failed_post)
        status_response = mock.Mock(spec=requests.Response)
        status_response.status_code = 200
        status_response.json.return_value = {
            "id": INVOCATION_ID,
            "status": "failed",
            "error": "agent invocation failed",
        }
        http.side_effect = lambda method, *args, **kwargs: {"POST": failed_post, "GET": status_response}[
            method
        ]

        with pytest.raises(ManagedAgentInvocationError):
            hook.agent("https://other.databricksapps.com").invoke(
                ManagedAgentRequest(
                    prompt="hello",
                    session_id="conversation",
                    timeout=12,
                    vendor_options={"invocation_id": INVOCATION_ID},
                )
            )

        get_calls = [call for call in http.call_args_list if call.args[0] == "GET"]
        assert len(get_calls) == 1
        assert get_calls[0].args == (
            "GET",
            f"https://other.databricksapps.com/api/invocations/{INVOCATION_ID}",
        )
        assert get_calls[0].kwargs["headers"]["X-Routing-Key"] == "conversation"
        assert 0 < get_calls[0].kwargs["timeout"] <= 12

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    @pytest.mark.parametrize("status", ["queued", "active"])
    def test_server_error_with_pending_invocation_remains_transient(self, http, token, hook, status):
        failed_post = mock.Mock(spec=requests.Response)
        failed_post.status_code = 500
        error = requests.HTTPError(response=failed_post)
        failed_post.raise_for_status.side_effect = error
        status_response = mock.Mock(spec=requests.Response)
        status_response.status_code = 200
        status_response.json.return_value = {"id": INVOCATION_ID, "status": status}
        http.side_effect = lambda method, *args, **kwargs: {"POST": failed_post, "GET": status_response}[
            method
        ]

        with pytest.raises(requests.HTTPError) as exc:
            hook.agent(APP_URL).invoke(
                ManagedAgentRequest(prompt="hello", vendor_options={"invocation_id": INVOCATION_ID})
            )
        assert exc.value is error
        assert ("GET", f"{APP_URL}/api/invocations/{INVOCATION_ID}") in [
            call.args for call in http.call_args_list
        ]

    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    @pytest.mark.parametrize("lookup_status", [404, 503])
    def test_failed_status_lookup_preserves_original_error(self, http, token, hook, lookup_status):
        failed_post = mock.Mock(spec=requests.Response)
        failed_post.status_code = 500
        original_error = requests.HTTPError(response=failed_post)
        failed_post.raise_for_status.side_effect = original_error
        failed_get = mock.Mock(spec=requests.Response)
        failed_get.status_code = lookup_status
        failed_get.raise_for_status.side_effect = requests.HTTPError(response=failed_get)
        http.side_effect = lambda method, *args, **kwargs: {"POST": failed_post, "GET": failed_get}[method]

        with pytest.raises(requests.HTTPError) as exc:
            hook.agent(APP_URL).invoke(
                ManagedAgentRequest(prompt="hello", vendor_options={"invocation_id": INVOCATION_ID})
            )
        assert exc.value is original_error
        assert ("GET", f"{APP_URL}/api/invocations/{INVOCATION_ID}") in [
            call.args for call in http.call_args_list
        ]

    @mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_status_lookup_uses_remaining_budget(self, http, token, clock, hook):
        clock.monotonic.return_value = 0
        failed_post = mock.Mock(spec=requests.Response)
        failed_post.status_code = 500
        failed_post.raise_for_status.side_effect = requests.HTTPError(response=failed_post)
        completed_get = mock.Mock(spec=requests.Response)
        completed_get.status_code = 200
        completed_get.json.return_value = {"id": INVOCATION_ID, "status": "failed"}

        def get_response(method, *args, **kwargs):
            clock.monotonic.return_value = 4
            return {"POST": failed_post, "GET": completed_get}[method]

        http.side_effect = get_response
        with pytest.raises(ManagedAgentInvocationError):
            hook.agent(APP_URL).invoke(
                ManagedAgentRequest(
                    prompt="hello", timeout=12, vendor_options={"invocation_id": INVOCATION_ID}
                )
            )
        get_calls = [call for call in http.call_args_list if call.args[0] == "GET"]
        assert len(get_calls) == 1
        assert get_calls[0].kwargs["timeout"] == 8

    @mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
    @mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
    @mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
    def test_status_lookup_rejects_late_result(self, http, token, clock, hook):
        clock.monotonic.return_value = 0
        failed_post = mock.Mock(spec=requests.Response)
        failed_post.status_code = 500
        failed_post.raise_for_status.side_effect = requests.HTTPError(response=failed_post)
        completed_get = mock.Mock(spec=requests.Response)
        completed_get.status_code = 200
        completed_get.json.return_value = {"id": INVOCATION_ID, "status": "failed"}

        def get_response(method, *args, **kwargs):
            clock.monotonic.return_value = {"POST": 4, "GET": 12}[method]
            return {"POST": failed_post, "GET": completed_get}[method]

        http.side_effect = get_response
        with pytest.raises(DatabricksAgentInvocationTimeout):
            hook.agent(APP_URL).invoke(
                ManagedAgentRequest(
                    prompt="hello", timeout=12, vendor_options={"invocation_id": INVOCATION_ID}
                )
            )
        assert http.call_args.args == ("GET", f"{APP_URL}/api/invocations/{INVOCATION_ID}")

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
def test_poll_budget_expires_before_token(request, token, clock, hook):
    clock.monotonic.side_effect = [0, 1]
    with pytest.raises(DatabricksAgentInvocationTimeout, match=INVOCATION_ID):
        hook.get_invocation(INVOCATION_ID, timeout_seconds=1)
    token.assert_not_called()
    request.assert_not_called()


@mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True)
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
def test_poll_budget_expires_after_token(request, token, clock, hook):
    clock.monotonic.return_value = 0

    def get_token(*args):
        clock.monotonic.return_value = 1
        return "oauth"

    token.side_effect = get_token
    with pytest.raises(DatabricksAgentInvocationTimeout, match=INVOCATION_ID):
        hook.get_invocation(INVOCATION_ID, timeout_seconds=1)
    token.assert_called_once()
    request.assert_not_called()


@mock.patch("airflow.providers.databricks.hooks.agent.time", autospec=True)
@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
def test_poll_budget_rejects_late_response(request, token, clock, hook):
    clock.monotonic.return_value = 0
    response = mock.Mock(spec=requests.Response)
    response.status_code = 200
    response.json.return_value = {"status": "completed"}

    def get_response(*args, **kwargs):
        clock.monotonic.return_value = 1
        return response

    request.side_effect = get_response
    with pytest.raises(DatabricksAgentInvocationTimeout, match=INVOCATION_ID):
        hook.get_invocation(INVOCATION_ID, timeout_seconds=1)
    request.assert_called_once()


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
@pytest.mark.parametrize("error", [aiohttp.InvalidURL("invalid"), aiohttp.ClientPayloadError("malformed")])
@pytest.mark.asyncio
async def test_async_non_retryable_transport_failure(session, token, hook, error):
    session.return_value.get.return_value.__aenter__.side_effect = error
    async with hook:
        with pytest.raises(type(error)) as exc:
            await hook.a_get_invocation(INVOCATION_ID)
    assert exc.value is error
    session.return_value.get.assert_called_once()
    session.return_value.close.assert_awaited_once()


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@pytest.mark.asyncio
async def test_async_disconnect_exhausts_retries(session, token, hook):
    session.return_value.get.return_value.__aenter__.side_effect = aiohttp.ServerDisconnectedError()
    async with hook:
        with pytest.raises(DatabricksApiError):
            await hook.a_get_invocation(INVOCATION_ID)
    assert session.return_value.get.call_count == 2
    session.return_value.close.assert_awaited_once()
