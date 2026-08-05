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
"""Airflow connection and minimal asynchronous client for the Islo REST API."""

from __future__ import annotations

import asyncio
import math
import random
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote

import httpx

from airflow.providers.islo import __version__
from airflow.providers.islo.exceptions import IsloConfigurationError, IsloProtocolError
from airflow.providers.islo.models import (
    IsloExecutionResult,
    IsloExecutionStart,
    IsloExecutionState,
    IsloSandboxHandle,
    IsloSandboxSpec,
)
from airflow.sdk import BaseHook

DEFAULT_COMPUTE_URL = "https://ca.compute.islo.dev"
DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024
_RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class IsloClientConfig:
    """Resolved, immutable client configuration safe to pass to the runner thread."""

    api_key: str = field(repr=False)
    api_url: str = DEFAULT_COMPUTE_URL
    request_timeout: float = 30.0
    max_retries: int = 3
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES


def _normalize_url(value: str, *, field_name: str) -> str:
    url = value.strip().rstrip("/")
    try:
        parsed = httpx.URL(url)
    except httpx.InvalidURL:
        parsed = None
    if parsed is None or parsed.scheme not in {"http", "https"} or not parsed.host:
        raise IsloConfigurationError(f"{field_name} must be an absolute HTTP(S) URL")
    if parsed.query or parsed.fragment:
        raise IsloConfigurationError(f"{field_name} cannot contain a query string or fragment")
    return url


def _get_json_object(response: httpx.Response, *, operation: str) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError as error:
        raise IsloProtocolError(f"Islo {operation} response was not valid JSON") from error
    if not isinstance(data, dict):
        raise IsloProtocolError(f"Islo {operation} response was not a JSON object")
    return data


class AsyncIsloClient:
    """Small REST client covering only the API surface required by ``IsloExecutor``."""

    def __init__(self, config: IsloClientConfig, *, http_client: httpx.AsyncClient | None = None) -> None:
        self.config = config
        self._http = http_client or httpx.AsyncClient(
            follow_redirects=False,
            timeout=config.request_timeout,
            headers={"User-Agent": f"apache-airflow-providers-islo/{__version__}"},
        )
        self._owns_http_client = http_client is None

    async def close(self) -> None:
        if self._owns_http_client:
            await self._http.aclose()

    @staticmethod
    def _retry_delay(response: httpx.Response | None, attempt: int) -> float:
        if response is not None and (retry_after := response.headers.get("Retry-After")):
            try:
                return min(float(retry_after), 30.0)
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    return max(0.0, min(retry_at.timestamp() - time.time(), 30.0))
                except (TypeError, ValueError):
                    pass
        return min(0.25 * (2**attempt) + random.uniform(0, 0.25), 10.0)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        retryable: bool = False,
    ) -> httpx.Response:
        attempt = 0
        while True:
            response: httpx.Response | None = None
            try:
                request = self._http.build_request(
                    method,
                    f"{self.config.api_url}/{path.lstrip('/')}",
                    headers={"Authorization": f"Bearer {self.config.api_key}"},
                    json=json_body,
                    params=params,
                )
                streamed_response = await self._http.send(request, stream=True)
                try:
                    body = bytearray()
                    async for chunk in streamed_response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > self.config.max_response_bytes:
                            raise IsloProtocolError(
                                "Islo response exceeded the configured max_response_bytes limit"
                            )
                    response = httpx.Response(
                        streamed_response.status_code,
                        headers=streamed_response.headers,
                        content=bytes(body),
                        request=request,
                    )
                finally:
                    await streamed_response.aclose()
            except (httpx.TimeoutException, httpx.TransportError):
                if not retryable or attempt >= self.config.max_retries:
                    raise
            else:
                if not (
                    retryable
                    and response.status_code in _RETRYABLE_STATUS_CODES
                    and attempt < self.config.max_retries
                ):
                    return response
            await asyncio.sleep(self._retry_delay(response, attempt))
            attempt += 1

    async def health_check(self) -> None:
        response = await self._request("GET", "sandboxes", params={"limit": 1}, retryable=True)
        response.raise_for_status()

    async def get_sandbox_id(self, sandbox_name: str) -> str | None:
        """Return the stable ID currently bound to ``sandbox_name``, if conclusively absent."""
        response = await self._request(
            "GET",
            f"sandboxes/{quote(sandbox_name, safe='')}",
            retryable=True,
        )
        error = self._parse_error_response(response)
        if response.status_code == 404:
            if error is not None and error[0] in {"SANDBOX_NOT_FOUND", "GONE"}:
                return None
            detail = f"{error[0]}: {error[1]}" if error is not None else "malformed error response"
            raise IsloProtocolError(f"Islo sandbox lookup returned 404 with {detail}")
        response.raise_for_status()
        data = _get_json_object(response, operation="sandbox lookup")
        returned_name = data.get("name")
        sandbox_id = data.get("id")
        if returned_name != sandbox_name:
            raise IsloProtocolError(
                f"Islo sandbox lookup returned name {returned_name!r}, expected {sandbox_name!r}"
            )
        if not isinstance(sandbox_id, str) or not sandbox_id:
            raise IsloProtocolError("Islo sandbox lookup did not contain a non-empty string id")
        return sandbox_id

    async def create_sandbox(self, spec: IsloSandboxSpec) -> tuple[str, str]:
        config = spec.config
        payload: dict[str, Any] = {
            "disk_gb": config.disk_gb,
            "gateway_profile": config.gateway_profile,
            "image": config.image,
            "internet_enabled": config.internet_enabled,
            "lifecycle": {"delete_after": spec.ttl_seconds},
            "memory_mb": config.memory_mb,
            "name": spec.name,
            "request_id": spec.request_id,
            "snapshot_name": config.snapshot_name,
            "vcpus": config.vcpus,
        }
        response = await self._request(
            "POST",
            "sandboxes",
            json_body={key: value for key, value in payload.items() if value is not None},
        )
        response.raise_for_status()
        if response.status_code != 201:
            raise IsloProtocolError(
                f"Islo create returned {response.status_code}, expected 201 lifecycle acceptance"
            )
        data = _get_json_object(response, operation="create")
        name = data.get("name")
        sandbox_id = data.get("id")
        if not isinstance(name, str) or not name or not isinstance(sandbox_id, str) or not sandbox_id:
            raise IsloProtocolError("Islo create response did not contain string name and id fields")
        return name, sandbox_id

    async def execute(
        self,
        sandbox_name: str,
        command: list[str],
        env: dict[str, str],
        *,
        workdir: str | None,
        timeout_seconds: int,
    ) -> IsloExecutionStart:
        response = await self._request(
            "POST",
            f"sandboxes/{quote(sandbox_name, safe='')}/exec",
            json_body={
                "command": command,
                "env": env or None,
                "timeout_secs": timeout_seconds,
                "workdir": workdir,
            },
        )
        response.raise_for_status()
        data = _get_json_object(response, operation="exec")
        exec_id = data.get("exec_id")
        sandbox_id = data.get("sandbox_id")
        status = data.get("status")
        if not isinstance(exec_id, str) or not exec_id:
            raise IsloProtocolError("Islo exec response did not contain exec_id")
        if not isinstance(sandbox_id, str) or not sandbox_id:
            raise IsloProtocolError("Islo exec response did not contain sandbox_id")
        if status != "started":
            raise IsloProtocolError(f"Islo exec response contained unexpected status: {status!r}")
        return IsloExecutionStart(execution_id=exec_id, sandbox_id=sandbox_id)

    async def execution_result(self, handle: IsloSandboxHandle) -> IsloExecutionResult:
        response = await self._request(
            "GET",
            f"sandboxes/{quote(handle.sandbox_name, safe='')}/exec/{quote(handle.execution_id, safe='')}",
            retryable=True,
        )
        error = self._parse_error_response(response)
        if response.status_code == 404:
            if error is not None and error[0] in {"SANDBOX_NOT_FOUND", "GONE"}:
                return IsloExecutionResult(IsloExecutionState.GONE)
            detail = f"{error[0]}: {error[1]}" if error is not None else "malformed error response"
            raise IsloProtocolError(f"Islo exec lookup returned 404 with {detail}")
        if response.status_code == 400 and error is not None and error[0] == "COMMAND_NOT_FOUND":
            raise IsloProtocolError(f"Islo exec lookup failed with COMMAND_NOT_FOUND: {error[1]}")
        response.raise_for_status()
        data = _get_json_object(response, operation="exec result")
        self._validate_execution_result(data, expected_exec_id=handle.execution_id)

        status = data["status"]
        exit_code = data.get("exit_code")
        if status == "pending":
            state = IsloExecutionState.PENDING
        elif status in {"started", "running"}:
            state = IsloExecutionState.RUNNING
        elif status == "completed":
            if exit_code is None:
                raise IsloProtocolError("Islo completed exec result did not contain an exit_code")
            state = IsloExecutionState.SUCCEEDED if exit_code == 0 else IsloExecutionState.FAILED
        elif status in {"failed", "timeout"}:
            state = IsloExecutionState.FAILED
        else:
            raise IsloProtocolError(f"Islo exec result contained unexpected status: {status!r}")
        return IsloExecutionResult(
            state=state,
            exit_code=exit_code,
            stdout=data["stdout"],
            stderr=data["stderr"],
            truncated=data["truncated"],
        )

    @staticmethod
    def _validate_execution_result(data: dict[str, Any], *, expected_exec_id: str) -> None:
        exec_id = data.get("exec_id")
        if not isinstance(exec_id, str) or not exec_id:
            raise IsloProtocolError("Islo exec result exec_id was not a non-empty string")
        if exec_id != expected_exec_id:
            raise IsloProtocolError(
                f"Islo exec result ID {exec_id!r} did not match requested ID {expected_exec_id!r}"
            )
        status = data.get("status")
        if not isinstance(status, str) or not status:
            raise IsloProtocolError("Islo exec result status was not a non-empty string")
        for field_name in ("stdout", "stderr"):
            if not isinstance(data.get(field_name), str):
                raise IsloProtocolError(f"Islo exec result {field_name} was not a string")
        if not isinstance(data.get("truncated"), bool):
            raise IsloProtocolError("Islo exec result truncated flag was not a boolean")
        exit_code = data.get("exit_code")
        if isinstance(exit_code, bool) or (exit_code is not None and not isinstance(exit_code, int)):
            raise IsloProtocolError("Islo exec result exit_code was not an integer")

    @staticmethod
    def _parse_error_response(response: httpx.Response) -> tuple[str, str] | None:
        if response.status_code < 400:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        if not isinstance(data, dict):
            return None
        code = data.get("code")
        message = data.get("message")
        if not isinstance(code, str) or not code or not isinstance(message, str):
            return None
        return code, message

    async def delete_sandbox(self, sandbox_name: str) -> None:
        response = await self._request(
            "DELETE",
            f"sandboxes/{quote(sandbox_name, safe='')}",
            retryable=True,
        )
        error = self._parse_error_response(response)
        if response.status_code == 404:
            if error is not None and error[0] in {"SANDBOX_NOT_FOUND", "GONE"}:
                return
            detail = f"{error[0]}: {error[1]}" if error is not None else "malformed error response"
            raise IsloProtocolError(f"Islo sandbox delete returned 404 with {detail}")
        response.raise_for_status()
        if response.status_code != 204:
            raise IsloProtocolError(
                f"Islo sandbox delete returned {response.status_code}, expected 204 deletion confirmation"
            )


class IsloHook(BaseHook):
    """Resolve an Islo API client from an Airflow connection."""

    conn_name_attr = "islo_conn_id"
    default_conn_name = "islo_default"
    conn_type = "islo"
    hook_name = "Islo"

    def __init__(self, islo_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.islo_conn_id = islo_conn_id

    def get_client_config(self) -> IsloClientConfig:
        connection = self.get_connection(self.islo_conn_id)
        if not connection.password or not connection.password.strip():
            raise IsloConfigurationError(
                f"Islo connection {self.islo_conn_id!r} must store the API key in Password"
            )
        extras = connection.extra_dejson
        try:
            raw_timeout = extras.get("request_timeout", 30.0)
            if isinstance(raw_timeout, bool):
                raise ValueError
            request_timeout = float(raw_timeout)
            raw_max_retries = extras.get("max_retries", 3)
            if isinstance(raw_max_retries, bool) or (
                isinstance(raw_max_retries, float) and not raw_max_retries.is_integer()
            ):
                raise ValueError
            max_retries = int(raw_max_retries)
            raw_max_response_bytes = extras.get("max_response_bytes", DEFAULT_MAX_RESPONSE_BYTES)
            if isinstance(raw_max_response_bytes, bool) or (
                isinstance(raw_max_response_bytes, float) and not raw_max_response_bytes.is_integer()
            ):
                raise ValueError
            max_response_bytes = int(raw_max_response_bytes)
        except (TypeError, ValueError) as error:
            raise IsloConfigurationError(
                "request_timeout must be numeric; max_retries and max_response_bytes must be integers"
            ) from error
        if not math.isfinite(request_timeout) or request_timeout <= 0:
            raise IsloConfigurationError("request_timeout must be a finite positive number")
        if max_retries < 0:
            raise IsloConfigurationError("max_retries cannot be negative")
        if max_response_bytes <= 0:
            raise IsloConfigurationError("max_response_bytes must be a positive integer")
        return IsloClientConfig(
            api_key=connection.password.strip(),
            api_url=_normalize_url(connection.host or DEFAULT_COMPUTE_URL, field_name="Islo API URL"),
            request_timeout=request_timeout,
            max_retries=max_retries,
            max_response_bytes=max_response_bytes,
        )

    def get_async_client(self) -> AsyncIsloClient:
        return AsyncIsloClient(self.get_client_config())
