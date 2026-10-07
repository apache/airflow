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

from unittest import mock

import aiohttp
import pytest
import requests

from airflow.providers.common.compat.sdk import Connection
from airflow.providers.databricks.exceptions import DatabricksApiError
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook

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
        hook.invoke_agent(INVOCATION_ID, {})


@mock.patch.object(DatabricksAgentHook, "_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.agent.requests.request", autospec=True)
@pytest.mark.parametrize("session_id", [None, "conversation"])
def test_invoke(request, token, hook, session_id):
    response = mock.Mock(spec=requests.Response)
    response.status_code = 202
    response.json.return_value = {"id": INVOCATION_ID, "status_url": "ignored"}
    request.return_value = response
    assert hook.invoke_agent(INVOCATION_ID, {"messages": []}, session_id) == response.json.return_value
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


@pytest.mark.parametrize("method", ["invoke_agent", "get_invocation"])
def test_invalid_invocation_id(hook, method):
    with pytest.raises(ValueError, match="badly formed hexadecimal UUID"):
        getattr(hook, method)("../../other", **({"input": {}} if method == "invoke_agent" else {}))


@mock.patch.object(DatabricksAgentHook, "_a_get_sp_token", autospec=True, return_value="oauth")
@mock.patch("airflow.providers.databricks.hooks.databricks_base.aiohttp.ClientSession", autospec=True)
@pytest.mark.parametrize("status", [200, 302, 403, 429, 500])
@pytest.mark.asyncio
async def test_async_get(session, token, hook, status):
    response = mock.Mock(spec=aiohttp.ClientResponse)
    response.status = status
    response.json = mock.AsyncMock(return_value={"status": "completed"})
    if status >= 400:
        response.raise_for_status.side_effect = aiohttp.ClientResponseError(mock.Mock(), (), status=status)
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
