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
from unittest.mock import patch

import httpx
import pytest

from airflow.providers.islo.exceptions import IsloConfigurationError, IsloProtocolError
from airflow.providers.islo.hooks.islo import (
    DEFAULT_COMPUTE_URL,
    DEFAULT_MAX_RESPONSE_BYTES,
    AsyncIsloClient,
    IsloClientConfig,
    IsloHook,
)
from airflow.providers.islo.models import (
    IsloExecutionState,
    IsloSandboxConfig,
    IsloSandboxHandle,
    IsloSandboxSpec,
    sandbox_name_from_request_id,
)
from airflow.sdk import Connection

REQUEST_ID = "00000000-0000-0000-0000-000000000001"
SANDBOX_NAME = sandbox_name_from_request_id(REQUEST_ID)


def make_handle() -> IsloSandboxHandle:
    return IsloSandboxHandle(REQUEST_ID, SANDBOX_NAME, "sandbox-id", "exec-id")


def make_spec() -> IsloSandboxSpec:
    return IsloSandboxSpec(
        name=SANDBOX_NAME,
        request_id=REQUEST_ID,
        config=IsloSandboxConfig(snapshot_name="airflow-runtime"),
        ttl_seconds=86400,
    )


def make_result_payload(
    status: object = "running",
    *,
    exec_id: object = "exec-id",
    exit_code: object = None,
    stdout: object = "",
    stderr: object = "",
    truncated: object = False,
) -> dict[str, object]:
    return {
        "exec_id": exec_id,
        "exit_code": exit_code,
        "status": status,
        "stderr": stderr,
        "stdout": stdout,
        "truncated": truncated,
    }


def make_client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    max_retries: int = 3,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
) -> tuple[AsyncIsloClient, httpx.AsyncClient]:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = IsloClientConfig(
        api_key="islo_key_test",
        api_url="https://compute.example",
        max_retries=max_retries,
        max_response_bytes=max_response_bytes,
    )
    return AsyncIsloClient(config, http_client=http_client), http_client


@pytest.mark.asyncio
async def test_client_uses_direct_bearer_auth_and_maps_lifecycle() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/sandboxes" and request.method == "POST":
            return httpx.Response(201, json={"name": SANDBOX_NAME, "id": "sandbox-id"})
        if request.url.path == f"/sandboxes/{SANDBOX_NAME}/exec" and request.method == "POST":
            return httpx.Response(
                200,
                json={"exec_id": "exec-id", "sandbox_id": "sandbox-id", "status": "started"},
            )
        if request.url.path == f"/sandboxes/{SANDBOX_NAME}/exec/exec-id":
            return httpx.Response(
                200,
                json=make_result_payload(
                    "completed",
                    exit_code=0,
                    stdout="task output",
                ),
            )
        if request.url.path == f"/sandboxes/{SANDBOX_NAME}" and request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(500)

    client, http_client = make_client(handler)
    spec = make_spec()

    sandbox_name, sandbox_id = await client.create_sandbox(spec)
    started = await client.execute(
        sandbox_name,
        ["python", "-m", "airflow.sdk.execution_time.execute_workload"],
        {"MODEL": "small"},
        workdir="/workspace",
        timeout_seconds=3600,
    )
    result = await client.execution_result(
        IsloSandboxHandle(spec.request_id, sandbox_name, sandbox_id, started.execution_id)
    )
    await client.delete_sandbox(sandbox_name)

    assert started.sandbox_id == sandbox_id
    assert result.state is IsloExecutionState.SUCCEEDED
    assert result.stdout == "task output"
    assert all(request.url.host == "compute.example" for request in requests)
    assert all(request.headers["authorization"] == "Bearer islo_key_test" for request in requests)
    assert all(request.url.path != "/auth/token" for request in requests)
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
    assert "env" not in create_payload
    assert "workdir" not in create_payload
    exec_payload = json.loads(
        next(request.content for request in requests if request.url.path.endswith("/exec"))
    )
    assert exec_payload == {
        "command": ["python", "-m", "airflow.sdk.execution_time.execute_workload"],
        "env": {"MODEL": "small"},
        "timeout_secs": 3600,
        "workdir": "/workspace",
    }
    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "execute"])
async def test_non_idempotent_submissions_are_not_retried(operation: str) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    client, http_client = make_client(handler, max_retries=5)
    submission = (
        client.create_sandbox(make_spec())
        if operation == "create"
        else client.execute("sandbox", ["true"], {}, workdir=None, timeout_seconds=10)
    )

    with pytest.raises(httpx.HTTPStatusError):
        await submission

    assert calls == 1
    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, json={"name": SANDBOX_NAME, "id": "sandbox-id"}),
        httpx.Response(201, json={"name": SANDBOX_NAME}),
    ],
)
async def test_create_requires_lifecycle_acceptance_and_stable_identity(
    response: httpx.Response,
) -> None:
    client, http_client = make_client(lambda request: response)

    with pytest.raises(IsloProtocolError):
        await client.create_sandbox(make_spec())

    await http_client.aclose()


@pytest.mark.asyncio
async def test_idempotent_read_retries_transient_response() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, headers={"Retry-After": "0"})
        return httpx.Response(200, json=make_result_payload())

    client, http_client = make_client(handler)

    with patch("airflow.providers.islo.hooks.islo.asyncio.sleep") as sleep:
        result = await client.execution_result(make_handle())

    assert result.state is IsloExecutionState.RUNNING
    assert calls == 2
    sleep.assert_awaited_once_with(0.0)
    await http_client.aclose()


@pytest.mark.asyncio
async def test_idempotent_delete_retries_and_accepts_absence() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503)
        return httpx.Response(
            404,
            json={"code": "SANDBOX_NOT_FOUND", "message": "sandbox does not exist"},
        )

    client, http_client = make_client(handler)

    with patch("airflow.providers.islo.hooks.islo.asyncio.sleep"):
        await client.delete_sandbox(SANDBOX_NAME)

    assert calls == 2
    await http_client.aclose()


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(None, id="malformed"),
        pytest.param({"code": "NOT_FOUND", "message": "route not found"}, id="ambiguous-code"),
    ],
)
@pytest.mark.asyncio
async def test_delete_rejects_ambiguous_404(payload: dict[str, str] | None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=payload) if payload is not None else httpx.Response(404)

    client, http_client = make_client(handler)

    with pytest.raises(IsloProtocolError, match="sandbox delete returned 404"):
        await client.delete_sandbox(SANDBOX_NAME)

    await http_client.aclose()


@pytest.mark.asyncio
async def test_delete_requires_204_confirmation() -> None:
    client, http_client = make_client(lambda request: httpx.Response(202))

    with pytest.raises(IsloProtocolError, match="expected 204 deletion confirmation"):
        await client.delete_sandbox(SANDBOX_NAME)

    await http_client.aclose()


@pytest.mark.asyncio
async def test_response_body_is_bounded_while_streaming() -> None:
    client, http_client = make_client(
        lambda request: httpx.Response(200, content=b"x" * 33),
        max_response_bytes=32,
    )

    with pytest.raises(IsloProtocolError, match="max_response_bytes"):
        await client.health_check()

    await http_client.aclose()


@pytest.mark.asyncio
async def test_get_sandbox_id_returns_current_identity() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"name": SANDBOX_NAME, "id": "stable-sandbox-id"})

    client, http_client = make_client(handler)

    assert await client.get_sandbox_id(SANDBOX_NAME) == "stable-sandbox-id"
    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["SANDBOX_NOT_FOUND", "GONE"])
async def test_get_sandbox_id_returns_none_only_for_conclusive_absence(code: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"code": code, "message": "sandbox is gone"})

    client, http_client = make_client(handler)

    assert await client.get_sandbox_id(SANDBOX_NAME) is None
    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(404, content=b"not JSON"),
        httpx.Response(404, json={"code": "NOT_FOUND", "message": "unknown resource"}),
        httpx.Response(200, content=b"not JSON"),
        httpx.Response(200, json={"name": "other-sandbox", "id": "sandbox-id"}),
        httpx.Response(200, json={"name": SANDBOX_NAME}),
        httpx.Response(200, json={"name": SANDBOX_NAME, "id": 7}),
    ],
)
async def test_get_sandbox_id_rejects_ambiguous_or_malformed_responses(
    response: httpx.Response,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return response

    client, http_client = make_client(handler)

    with pytest.raises(IsloProtocolError):
        await client.get_sandbox_id(SANDBOX_NAME)

    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "exit_code", "state"),
    [
        ("pending", None, IsloExecutionState.PENDING),
        ("started", None, IsloExecutionState.RUNNING),
        ("running", 7, IsloExecutionState.RUNNING),
        ("completed", 0, IsloExecutionState.SUCCEEDED),
        ("completed", 7, IsloExecutionState.FAILED),
        ("failed", None, IsloExecutionState.FAILED),
        ("timeout", None, IsloExecutionState.FAILED),
    ],
)
async def test_execution_status_mapping(
    status: str, exit_code: int | None, state: IsloExecutionState
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=make_result_payload(status, exit_code=exit_code))

    client, http_client = make_client(handler)

    result = await client.execution_result(make_handle())

    assert result.state is state
    assert result.exit_code == exit_code
    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["SANDBOX_NOT_FOUND", "GONE"])
async def test_404_confirms_gone_only_for_sandbox_error_codes(code: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"code": code, "message": "sandbox is gone"})

    client, http_client = make_client(handler)

    assert (await client.execution_result(make_handle())).state is IsloExecutionState.GONE
    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"code": "GONE"},
        {"code": "COMMAND_NOT_FOUND", "message": "command is gone"},
    ],
)
async def test_other_or_malformed_404_responses_are_protocol_errors(payload: object) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if payload is None:
            return httpx.Response(404, content=b"not JSON")
        return httpx.Response(404, json=payload)

    client, http_client = make_client(handler)

    with pytest.raises(IsloProtocolError, match="404"):
        await client.execution_result(make_handle())

    await http_client.aclose()


@pytest.mark.asyncio
async def test_command_not_found_is_not_conclusive_absence() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={"code": "COMMAND_NOT_FOUND", "message": "execution does not exist"},
        )

    client, http_client = make_client(handler)

    with pytest.raises(IsloProtocolError, match="COMMAND_NOT_FOUND"):
        await client.execution_result(make_handle())

    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {"status": "running", "stderr": "", "stdout": "", "truncated": False},
        make_result_payload(exec_id="other-exec"),
        make_result_payload(status=7),
        make_result_payload(stdout=[]),
        make_result_payload(stderr=[]),
        make_result_payload(truncated=1),
        make_result_payload(exit_code=True),
        make_result_payload(exit_code="0"),
        make_result_payload(status="completed", exit_code=None),
        {key: value for key, value in make_result_payload(status="completed").items() if key != "exit_code"},
        make_result_payload(status="future-state"),
    ],
)
async def test_malformed_execution_result_is_a_protocol_error(payload: object) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if payload is None:
            return httpx.Response(200, content=b"not JSON")
        return httpx.Response(200, json=payload)

    client, http_client = make_client(handler)

    with pytest.raises(IsloProtocolError):
        await client.execution_result(make_handle())

    await http_client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"exec_id": "exec-id", "sandbox_id": "sandbox-id"},
        {"exec_id": "exec-id", "sandbox_id": "sandbox-id", "status": "running"},
    ],
)
async def test_malformed_exec_start_is_a_protocol_error(payload: object) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if payload is None:
            return httpx.Response(200, content=b"not JSON")
        return httpx.Response(200, json=payload)

    client, http_client = make_client(handler)

    with pytest.raises(IsloProtocolError):
        await client.execute("sandbox", ["true"], {}, workdir=None, timeout_seconds=10)

    await http_client.aclose()


def test_hook_resolves_compute_api_connection() -> None:
    connection = Connection(
        conn_id="islo_test",
        conn_type="islo",
        password=" islo_key_test ",
        host="https://compute.example/",
        extra=json.dumps({"request_timeout": 12, "max_retries": 4, "max_response_bytes": 1024}),
    )
    hook = IsloHook("islo_test")

    with patch.object(hook, "get_connection", return_value=connection):
        config = hook.get_client_config()

    assert config.api_key == "islo_key_test"
    assert config.api_url == "https://compute.example"
    assert config.request_timeout == 12
    assert config.max_retries == 4
    assert config.max_response_bytes == 1024
    assert "islo_key_test" not in repr(config)


def test_hook_uses_default_compute_api_url() -> None:
    connection = Connection(conn_id="islo_test", conn_type="islo", password="islo_key_test")
    hook = IsloHook("islo_test")

    with patch.object(hook, "get_connection", return_value=connection):
        config = hook.get_client_config()

    assert config.api_url == DEFAULT_COMPUTE_URL


@pytest.mark.parametrize(
    ("password", "host", "extra", "message"),
    [
        (None, None, None, "API key"),
        ("   ", None, None, "API key"),
        ("key", "compute.example", None, "absolute HTTP"),
        ("key", "https://compute.example?tenant=one", None, "query string"),
        ("key", None, {"request_timeout": True}, "request_timeout"),
        ("key", None, {"request_timeout": 0}, "finite positive"),
        ("key", None, {"max_retries": True}, "max_retries"),
        ("key", None, {"max_retries": 1.5}, "max_retries"),
        ("key", None, {"max_retries": -1}, "cannot be negative"),
        ("key", None, {"max_response_bytes": 0}, "positive integer"),
    ],
)
def test_hook_rejects_invalid_connection(
    password: str | None,
    host: str | None,
    extra: dict[str, object] | None,
    message: str,
) -> None:
    connection = Connection(
        conn_id="islo_test",
        conn_type="islo",
        password=password,
        host=host,
        extra=json.dumps(extra) if extra is not None else None,
    )
    hook = IsloHook("islo_test")

    with (
        patch.object(hook, "get_connection", return_value=connection),
        pytest.raises(IsloConfigurationError, match=message),
    ):
        hook.get_client_config()
