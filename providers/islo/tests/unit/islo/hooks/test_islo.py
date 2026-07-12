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
from unittest.mock import Mock, patch

import httpx
import pytest

from airflow.providers.islo.hooks.islo import AsyncIsloClient, IsloClientConfig, IsloHook
from airflow.providers.islo.models import (
    IsloExecutionRef,
    IsloExecutionState,
    IsloSandboxSpec,
)


@pytest.mark.asyncio
async def test_client_create_execute_status_and_delete() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(200, json={"session_token": "jwt", "cookie_max_age": 600})
        if request.url.path == "/sandboxes" and request.method == "POST":
            return httpx.Response(200, json={"name": "airflow-id", "id": "sandbox-id"})
        if request.url.path == "/sandboxes/airflow-id/exec" and request.method == "POST":
            return httpx.Response(200, json={"exec_id": "exec-id"})
        if request.url.path == "/sandboxes/airflow-id/exec/exec-id":
            return httpx.Response(200, json={"status": "completed", "exit_code": 0})
        if request.url.path == "/sandboxes/airflow-id" and request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(500)

    async_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncIsloClient(IsloClientConfig(access_key="ak_test"), http_client=async_http)
    spec = IsloSandboxSpec(
        name="airflow-id",
        request_id="00000000-0000-0000-0000-000000000001",
        snapshot_name="airflow-runtime",
    )

    sandbox_name, sandbox_id = await client.create_sandbox(spec)
    execution_id = await client.execute(
        sandbox_name,
        ["python", "-m", "airflow.sdk.execution_time.execute_workload"],
        {},
        workdir=None,
        timeout_seconds=3600,
    )
    ref = IsloExecutionRef(spec.request_id, sandbox_name, sandbox_id, execution_id)

    assert (await client.execution_result(ref)).state is IsloExecutionState.SUCCEEDED
    await client.delete_sandbox(sandbox_name)
    assert sum(request.url.path == "/auth/token" for request in requests) == 1
    assert all(request.headers.get("authorization") == "Bearer jwt" for request in requests[1:])
    create_payload = json.loads(
        next(
            request.content
            for request in requests
            if request.method == "POST" and request.url.path == "/sandboxes"
        )
    )
    assert create_payload["request_id"] == spec.request_id
    assert create_payload["snapshot_name"] == "airflow-runtime"
    assert create_payload["lifecycle"] == {"delete_after": 86400}
    exec_payload = json.loads(
        next(request.content for request in requests if request.url.path.endswith("/exec"))
    )
    assert exec_payload["timeout_secs"] == 3600

    await async_http.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["failed", "timeout"])
async def test_terminal_failure_without_exit_code_is_not_treated_as_running(status: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/token":
            return httpx.Response(200, json={"session_token": "jwt"})
        return httpx.Response(200, json={"status": status, "exit_code": None})

    async_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncIsloClient(IsloClientConfig(access_key="ak_test"), http_client=async_http)
    ref = IsloExecutionRef("request", "sandbox", "sandbox-id", "exec-id")

    result = await client.execution_result(ref)

    assert result.state is IsloExecutionState.FAILED
    await async_http.aclose()


@pytest.mark.asyncio
async def test_only_404_is_confirmed_gone() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/auth/token":
            return httpx.Response(200, json={"session_token": "jwt"})
        return httpx.Response(404)

    async_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncIsloClient(IsloClientConfig(access_key="ak_test"), http_client=async_http)
    ref = IsloExecutionRef("request", "sandbox", "sandbox-id", "exec-id")

    assert (await client.execution_result(ref)).state is IsloExecutionState.GONE
    await async_http.aclose()


@pytest.mark.asyncio
async def test_exec_submission_is_never_retried() -> None:
    exec_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal exec_calls
        if request.url.path == "/auth/token":
            return httpx.Response(200, json={"session_token": "jwt"})
        exec_calls += 1
        return httpx.Response(503)

    async_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncIsloClient(
        IsloClientConfig(access_key="ak_test", max_retries=5),
        http_client=async_http,
    )

    with pytest.raises(httpx.HTTPStatusError):
        await client.execute("sandbox", ["true"], {}, workdir=None, timeout_seconds=10)

    assert exec_calls == 1
    await async_http.aclose()


@pytest.mark.asyncio
async def test_unauthorized_response_refreshes_token_only_once() -> None:
    token_calls = 0
    compute_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal compute_calls, token_calls
        if request.url.path == "/auth/token":
            token_calls += 1
            return httpx.Response(200, json={"session_token": f"jwt-{token_calls}"})
        compute_calls += 1
        if compute_calls == 1:
            return httpx.Response(401)
        if compute_calls == 2:
            return httpx.Response(503)
        return httpx.Response(200, json={"status": "running", "exit_code": None})

    async_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncIsloClient(IsloClientConfig(access_key="ak_test"), http_client=async_http)
    ref = IsloExecutionRef("request", "sandbox", "sandbox-id", "exec-id")

    assert (await client.execution_result(ref)).state is IsloExecutionState.RUNNING
    assert token_calls == 2
    await async_http.aclose()


@pytest.mark.asyncio
async def test_token_exchange_retries_transient_failure() -> None:
    token_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        if request.url.path == "/auth/token":
            token_calls += 1
            if token_calls == 1:
                return httpx.Response(503)
            return httpx.Response(200, json={"session_token": "jwt"})
        return httpx.Response(200, json={"status": "running", "exit_code": None})

    async_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncIsloClient(IsloClientConfig(access_key="ak_test"), http_client=async_http)
    ref = IsloExecutionRef("request", "sandbox", "sandbox-id", "exec-id")

    with patch("airflow.providers.islo.hooks.islo.asyncio.sleep"):
        assert (await client.execution_result(ref)).state is IsloExecutionState.RUNNING
    assert token_calls == 2
    await async_http.aclose()


def test_hook_resolves_connection_fields() -> None:
    connection = Mock(
        password="ak_test",
        host="https://control.example",
        schema=None,
        extra_dejson={
            "compute_url": "https://compute.example",
            "request_timeout": 12,
            "max_retries": 4,
        },
    )
    hook = IsloHook("islo_test")

    with patch.object(hook, "get_connection", return_value=connection):
        config = hook.get_client_config()

    assert config.access_key == "ak_test"
    assert config.control_url == "https://control.example"
    assert config.compute_url == "https://compute.example"
    assert config.request_timeout == 12
    assert config.max_retries == 4
