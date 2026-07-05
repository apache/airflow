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
"""Thin async HTTP client for the Airflow REST API, with auth and a write-safety gate.

Every tool in :mod:`airflow_mcp_server.server` funnels its HTTP calls through
:func:`call_api`, which is the single choke point that decides whether a method is
allowed (read-only by default) and turns HTTP failures into readable ``ToolError``
messages the calling agent can act on.

Configuration is read from the environment *at call time* (not at import time) so
that tests can monkeypatch it per-case:

- ``AIRFLOW_API_URL`` -- base URL of the Airflow API server (default
  ``http://localhost:28080``, Breeze's host-forwarded port).
- ``AIRFLOW_USERNAME`` / ``AIRFLOW_PASSWORD`` -- SimpleAuthManager credentials used to
  log in via ``POST {AIRFLOW_API_URL}/auth/token`` (default ``admin`` / ``admin``,
  Breeze's default SimpleAuthManager user).
- ``AIRFLOW_ACCESS_TOKEN`` -- optional pre-issued JWT; when set, the login flow is
  skipped entirely and this value is used for every request.
- ``AIRFLOW_MCP_ALLOW_WRITES`` -- ``"true"``/``"1"`` to allow ``POST``/``PATCH``/``PUT``
  requests.
- ``AIRFLOW_MCP_ALLOW_DELETES`` -- ``"true"``/``"1"`` to allow ``DELETE`` requests. This is a
  *separate, stricter* gate than writes: it is off by default even when writes are enabled,
  because deletions are irreversible. Enabling it is a deliberate, explicit opt-in.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx
from fastmcp.exceptions import ToolError

_DEFAULT_API_URL = "http://localhost:28080"
_DEFAULT_USERNAME = "admin"
_DEFAULT_PASSWORD = "admin"
_WRITE_METHODS = {"POST", "PATCH", "PUT"}
_WRITES_ENV = "AIRFLOW_MCP_ALLOW_WRITES"
_DELETES_ENV = "AIRFLOW_MCP_ALLOW_DELETES"
_TRUTHY_VALUES = {"true", "1"}
_BODY_SNIPPET_LIMIT = 2000

_http_client: httpx.AsyncClient | None = None
_http_client_loop: asyncio.AbstractEventLoop | None = None
_cached_token: str | None = None


def _build_http_client() -> httpx.AsyncClient:
    """Build the underlying async HTTP client. Tests patch this to inject a mock transport."""
    return httpx.AsyncClient(timeout=30.0)


def _get_http_client() -> httpx.AsyncClient:
    """Return the shared HTTP client, rebuilding it if the running event loop has changed.

    httpx.AsyncClient binds to the event loop it was created under; without this check, a
    process that runs multiple asyncio event loops in sequence (e.g. successive
    ``asyncio.run()`` calls in a script, or per-test event loops) would reuse a client tied
    to a now-closed loop and fail with "Event loop is closed". The long-running server
    process only ever has one loop, so this is a no-op there.
    """
    global _http_client, _http_client_loop
    current_loop = asyncio.get_running_loop()
    if _http_client is None or _http_client_loop is not current_loop:
        _http_client = _build_http_client()
        _http_client_loop = current_loop
    return _http_client


def reset_client_state() -> None:
    """Drop the cached HTTP client and auth token so the next call rebuilds them from scratch."""
    global _http_client, _http_client_loop, _cached_token
    _http_client = None
    _http_client_loop = None
    _cached_token = None


def _get_base_url() -> str:
    return os.environ.get("AIRFLOW_API_URL", _DEFAULT_API_URL).rstrip("/")


async def _fetch_token() -> str:
    username = os.environ.get("AIRFLOW_USERNAME", _DEFAULT_USERNAME)
    password = os.environ.get("AIRFLOW_PASSWORD", _DEFAULT_PASSWORD)
    base_url = _get_base_url()
    response = await _get_http_client().post(
        f"{base_url}/auth/token",
        json={"username": username, "password": password},
    )
    if response.status_code != 201:
        raise ToolError(
            f"Failed to authenticate with Airflow at {base_url}/auth/token: "
            f"HTTP {response.status_code}: {response.text[:_BODY_SNIPPET_LIMIT]}"
        )
    return response.json()["access_token"]


async def _get_token(*, force_refresh: bool = False) -> str:
    """Return the bearer token to use, preferring an explicit pre-issued token if configured."""
    global _cached_token
    env_token = os.environ.get("AIRFLOW_ACCESS_TOKEN")
    if env_token:
        return env_token
    if _cached_token is None or force_refresh:
        _cached_token = await _fetch_token()
    return _cached_token


def _is_gate_enabled(env_var: str) -> bool:
    return os.environ.get(env_var, "").strip().lower() in _TRUTHY_VALUES


def _check_method_gate(method: str) -> None:
    """Enforce the single method safety choke point for all Airflow API calls.

    ``GET`` is always allowed; ``POST``/``PATCH``/``PUT`` need ``AIRFLOW_MCP_ALLOW_WRITES``;
    ``DELETE`` needs its own, stricter ``AIRFLOW_MCP_ALLOW_DELETES`` gate (off by default even
    when writes are enabled, since deletions are irreversible).
    """
    if method in ("GET", "QUERY"):
        return
    if method == "DELETE":
        if not _is_gate_enabled(_DELETES_ENV):
            raise ToolError(
                "DELETE is an irreversible operation and is disabled by default, separately from "
                f"other writes. Set {_DELETES_ENV}=true to enable deletions. Because deletions "
                "cannot be undone, your MCP client should also require explicit confirmation "
                "before each such call."
            )
        return
    if method not in _WRITE_METHODS:
        raise ToolError(f"Unsupported HTTP method: {method!r}")
    if not _is_gate_enabled(_WRITES_ENV):
        raise ToolError(
            f"{method} is a write operation and this MCP server is read-only by default. "
            f"Set the {_WRITES_ENV}=true environment variable to enable writes "
            f"(DELETE additionally requires the stricter {_DELETES_ENV})."
        )


def _normalize_path(path: str) -> str:
    if not path.startswith("/api/v2/"):
        raise ToolError(f"path must start with '/api/v2/', got: {path!r}")
    # httpx resolves RFC 3986 dot-segments before sending, so a '..' anywhere in the path
    # (including one smuggled in via an interpolated dag_id/task_id) could redirect the call
    # out of /api/v2 into another mounted sub-app (/auth, /execution, /ui) or to a different
    # resource. No legitimate Airflow API path contains a '..' segment, so reject them.
    if ".." in path.split("/"):
        raise ToolError(f"path must not contain '..' segments, got: {path!r}")
    return path


async def call_api(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    """Call the Airflow REST API and return the parsed response.

    Applies the read/write safety gate, authenticates (fetching and caching a JWT via
    ``/auth/token`` unless ``AIRFLOW_ACCESS_TOKEN`` is set), retries once on a 401 with a
    freshly-fetched token, and raises :class:`fastmcp.exceptions.ToolError` -- with the
    HTTP status and a truncated response body -- on any failure. Returns the parsed JSON
    body, or ``{"text": <body>}`` when the response is not JSON.
    """
    method = method.upper()
    _check_method_gate(method)
    path = _normalize_path(path)
    url = f"{_get_base_url()}{path}"
    client = _get_http_client()

    async def _send(token: str) -> httpx.Response:
        request_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            request_headers.update(headers)
        return await client.request(method, url, params=params, json=json_body, headers=request_headers)

    response = await _send(await _get_token())
    if response.status_code == 401:
        response = await _send(await _get_token(force_refresh=True))

    if response.status_code >= 400:
        raise ToolError(
            f"Airflow API {method} {path} failed: HTTP {response.status_code}: "
            f"{response.text[:_BODY_SNIPPET_LIMIT]}"
        )

    if "application/json" in response.headers.get("content-type", ""):
        return response.json()
    return {"text": response.text}
