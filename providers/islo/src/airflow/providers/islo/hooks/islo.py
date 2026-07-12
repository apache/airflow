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
import random
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote

import httpx

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.islo import __version__
from airflow.providers.islo.exceptions import IsloConfigurationError, IsloProtocolError
from airflow.providers.islo.models import (
    IsloExecutionRef,
    IsloExecutionResult,
    IsloExecutionState,
    IsloSandboxSpec,
)

DEFAULT_CONTROL_URL = "https://api.islo.dev"
DEFAULT_COMPUTE_URL = "https://ca.compute.islo.dev"


@dataclass(frozen=True)
class IsloClientConfig:
    """Resolved, immutable client configuration safe to pass to a manager thread."""

    access_key: str
    control_url: str = DEFAULT_CONTROL_URL
    compute_url: str = DEFAULT_COMPUTE_URL
    request_timeout: float = 30.0
    max_retries: int = 3
    refresh_margin_seconds: float = 60.0


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


class AsyncIsloClient:
    """Small REST client covering only the API surface required by ``IsloExecutor``."""

    def __init__(self, config: IsloClientConfig, *, http_client: httpx.AsyncClient | None = None) -> None:
        self.config = config
        self._http = http_client or httpx.AsyncClient(
            follow_redirects=True,
            timeout=config.request_timeout,
            headers={"User-Agent": f"apache-airflow-providers-islo/{__version__}"},
        )
        self._owns_http_client = http_client is None
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._token_lock = asyncio.Lock()

    async def close(self) -> None:
        if self._owns_http_client:
            await self._http.aclose()

    async def _get_token(self) -> str:
        if self._token and time.monotonic() < self._token_expires_at:
            return self._token
        async with self._token_lock:
            if self._token and time.monotonic() < self._token_expires_at:
                return self._token
            attempt = 0
            while True:
                response: httpx.Response | None = None
                try:
                    response = await self._http.post(
                        f"{self.config.control_url}/auth/token",
                        json={"access_key": self.config.access_key},
                    )
                except (httpx.TimeoutException, httpx.TransportError):
                    if attempt >= self.config.max_retries:
                        raise
                else:
                    if not (
                        response.status_code in {408, 429, 500, 502, 503, 504}
                        and attempt < self.config.max_retries
                    ):
                        response.raise_for_status()
                        break
                await asyncio.sleep(self._retry_delay(response, attempt))
                attempt += 1
            payload = response.json()
            if (
                not isinstance(payload, dict)
                or not isinstance(payload.get("session_token"), str)
                or not payload["session_token"]
            ):
                raise IsloProtocolError("Islo token response did not contain session_token")
            max_age = payload.get("cookie_max_age", 600)
            if not isinstance(max_age, (int, float)):
                max_age = 600
            self._token = payload["session_token"]
            self._token_expires_at = time.monotonic() + max(
                float(max_age) - self.config.refresh_margin_seconds,
                1.0,
            )
            return self._token

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
        retryable: bool,
    ) -> httpx.Response:
        did_refresh = False
        attempt = 0
        while True:
            token = await self._get_token()
            response: httpx.Response | None = None
            try:
                response = await self._http.request(
                    method,
                    f"{self.config.compute_url}/{path.lstrip('/')}",
                    headers={"Authorization": f"Bearer {token}"},
                    json=json_body,
                    params=params,
                )
            except (httpx.TimeoutException, httpx.TransportError):
                if not retryable or attempt >= self.config.max_retries:
                    raise
            else:
                if response.status_code == 401 and not did_refresh:
                    did_refresh = True
                    # Another concurrent request may already have refreshed the token.
                    if self._token == token:
                        self._token = None
                        self._token_expires_at = 0.0
                    continue
                if not (
                    retryable
                    and response.status_code in {408, 429, 500, 502, 503, 504}
                    and attempt < self.config.max_retries
                ):
                    return response
            await asyncio.sleep(self._retry_delay(response, attempt))
            attempt += 1

    async def health_check(self) -> None:
        response = await self._request("GET", "sandboxes", params={"limit": 1}, retryable=True)
        response.raise_for_status()

    async def create_sandbox(self, spec: IsloSandboxSpec) -> tuple[str, str]:
        payload: dict[str, Any] = {
            "disk_gb": spec.disk_gb,
            "env": spec.env or None,
            "gateway_profile": spec.gateway_profile,
            "image": spec.image,
            "internet_enabled": spec.internet_enabled,
            "lifecycle": {"delete_after": spec.ttl_seconds},
            "memory_mb": spec.memory_mb,
            "name": spec.name,
            "request_id": spec.request_id,
            "snapshot_name": spec.snapshot_name,
            "snapshot_url": spec.snapshot_url,
            "vcpus": spec.vcpus,
            "workdir": spec.workdir,
        }
        response = await self._request(
            "POST",
            "sandboxes",
            json_body={key: value for key, value in payload.items() if value is not None},
            retryable=True,
        )
        response.raise_for_status()
        data = response.json()
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("name"), str)
            or not isinstance(data.get("id"), str)
            or not data["name"]
            or not data["id"]
        ):
            raise IsloProtocolError("Islo create response did not contain string name and id fields")
        return data["name"], data["id"]

    async def execute(
        self,
        sandbox_name: str,
        command: list[str],
        env: dict[str, str],
        *,
        workdir: str | None,
        timeout_seconds: int,
    ) -> str:
        response = await self._request(
            "POST",
            f"sandboxes/{quote(sandbox_name, safe='')}/exec",
            json_body={
                "command": command,
                "env": env or None,
                "timeout_secs": timeout_seconds,
                "workdir": workdir,
            },
            retryable=False,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not isinstance(data.get("exec_id"), str) or not data["exec_id"]:
            raise IsloProtocolError("Islo exec response did not contain exec_id")
        return data["exec_id"]

    async def execution_result(self, ref: IsloExecutionRef) -> IsloExecutionResult:
        response = await self._request(
            "GET",
            f"sandboxes/{quote(ref.sandbox_name, safe='')}/exec/{quote(ref.execution_id, safe='')}",
            retryable=True,
        )
        if response.status_code == 404:
            return IsloExecutionResult(IsloExecutionState.GONE)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise IsloProtocolError("Islo exec result was not a JSON object")
        status = str(data.get("status", "")).lower()
        exit_code = data.get("exit_code")
        if exit_code is not None and not isinstance(exit_code, int):
            raise IsloProtocolError("Islo exec result exit_code was not an integer")
        if status in {"failed", "timeout", "timed_out", "cancelled", "canceled", "terminated"}:
            return IsloExecutionResult(IsloExecutionState.FAILED, exit_code)
        if status in {"completed", "succeeded", "success"}:
            state = IsloExecutionState.SUCCEEDED if exit_code in {None, 0} else IsloExecutionState.FAILED
            return IsloExecutionResult(state, exit_code)
        if exit_code is not None:
            state = IsloExecutionState.SUCCEEDED if exit_code == 0 else IsloExecutionState.FAILED
            return IsloExecutionResult(state, exit_code)
        if status in {"pending", "queued", "starting", "created"}:
            return IsloExecutionResult(IsloExecutionState.PENDING)
        if status in {"running", "executing", "in_progress"}:
            return IsloExecutionResult(IsloExecutionState.RUNNING)
        return IsloExecutionResult(IsloExecutionState.UNKNOWN)

    async def execution_output(self, ref: IsloExecutionRef) -> tuple[str, str, bool]:
        response = await self._request(
            "GET",
            f"sandboxes/{quote(ref.sandbox_name, safe='')}/exec/{quote(ref.execution_id, safe='')}",
            retryable=True,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise IsloProtocolError("Islo exec result was not a JSON object")
        return str(data.get("stdout") or ""), str(data.get("stderr") or ""), bool(data.get("truncated"))

    async def delete_sandbox(self, sandbox_name: str) -> None:
        response = await self._request(
            "DELETE",
            f"sandboxes/{quote(sandbox_name, safe='')}",
            retryable=True,
        )
        if response.status_code != 404:
            response.raise_for_status()


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
        if not connection.password:
            raise IsloConfigurationError(
                f"Islo connection {self.islo_conn_id!r} must store the access key in Password"
            )
        extras = connection.extra_dejson
        control_url = connection.host or DEFAULT_CONTROL_URL
        if connection.schema and "://" not in control_url:
            control_url = f"{connection.schema}://{control_url}"
        compute_url = str(extras.get("compute_url") or DEFAULT_COMPUTE_URL)
        try:
            request_timeout = float(extras.get("request_timeout", 30.0))
            max_retries = int(extras.get("max_retries", 3))
        except (TypeError, ValueError) as error:
            raise IsloConfigurationError(
                "request_timeout must be numeric and max_retries must be an integer"
            ) from error
        if request_timeout <= 0:
            raise IsloConfigurationError("request_timeout must be greater than zero")
        if max_retries < 0:
            raise IsloConfigurationError("max_retries cannot be negative")
        return IsloClientConfig(
            access_key=connection.password,
            control_url=_normalize_url(control_url, field_name="Islo control URL"),
            compute_url=_normalize_url(compute_url, field_name="Islo compute URL"),
            request_timeout=request_timeout,
            max_retries=max_retries,
        )

    def get_async_client(self) -> AsyncIsloClient:
        return AsyncIsloClient(self.get_client_config())
