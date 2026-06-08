#
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
"""
Provide the :mod:`airflowctl` HTTP API client to the local Airflow CLI.

The local CLI talks to the API server through the same typed client that ``airflowctl``
uses, but without the keyring-backed credential store. For each invocation it mints a
short-lived JWT **in memory** (via the active auth manager) and builds a client with it;
nothing is persisted. Set the ``AIRFLOW_CLI_TOKEN`` environment variable to supply a token
explicitly (required for auth managers whose tokens cannot be minted locally, such as
Keycloak, or when targeting a remote API server).
"""

from __future__ import annotations

import atexit
import os
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, TypeVar

import httpx

# Re-exported so command modules import the client surface from a single place.
from airflowctl.api.client import NEW_API_CLIENT, Client, ClientKind

from airflow.configuration import conf
from airflow.typing_compat import ParamSpec

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager

__all__ = [
    "NEW_API_CLIENT",
    "Client",
    "ClientKind",
    "get_cli_api_client",
    "provide_api_client",
]

PS = ParamSpec("PS")
RT = TypeVar("RT")

# Validity of the in-memory CLI token. It only needs to outlive a single CLI command
# (including the client's request retries) and is never persisted or logged.
_CLI_TOKEN_VALID_FOR_SECONDS = 300

_api_client: Client | None = None


def _resolve_base_url() -> str:
    """Resolve the API server base URL from configuration."""
    base_url = conf.get("api", "base_url", fallback=None)
    if base_url:
        return base_url
    host = conf.get("api", "host", fallback="localhost") or "localhost"
    port = conf.get("api", "port", fallback="8080") or "8080"
    return f"http://{host}:{port}"


def _mint_cli_token() -> str:
    """
    Return a token for the CLI to authenticate against the API server.

    Prefers an explicit ``AIRFLOW_CLI_TOKEN`` (the universal override), otherwise mints a
    short-lived JWT through the active auth manager. The token lives only in this process.
    """
    if token := os.environ.get("AIRFLOW_CLI_TOKEN"):
        return token

    from airflow.api_fastapi.app import get_auth_manager, init_auth_manager

    # The CLI runs outside the API server, so the auth manager singleton is usually not
    # initialized yet; initialize it on demand. ``init_auth_manager`` reuses the cached
    # instance when one already exists, so this is safe to call here.
    try:
        auth_manager: BaseAuthManager = get_auth_manager()
    except RuntimeError:
        auth_manager = init_auth_manager()
    return auth_manager.generate_jwt(
        auth_manager.get_cli_user(),
        expiration_time_in_seconds=_CLI_TOKEN_VALID_FOR_SECONDS,
    )


def get_cli_api_client() -> Client:
    """Return the process-wide singleton airflowctl client for the local CLI."""
    global _api_client
    if _api_client is None:
        _api_client = Client(
            base_url=_resolve_base_url(),
            token=_mint_cli_token(),
            kind=ClientKind.CLI,
            limits=httpx.Limits(max_keepalive_connections=1, max_connections=1),
        )
        atexit.register(_api_client.close)
    return _api_client


def provide_api_client(func: Callable[PS, RT]) -> Callable[PS, RT]:
    """
    Provide the CLI API client to the decorated command function.

    Injects ``api_client=get_cli_api_client()`` when the caller does not pass one. Tests
    (or callers that already hold a client) pass ``api_client=`` explicitly to bypass it.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> RT:
        if "api_client" not in kwargs:
            kwargs["api_client"] = get_cli_api_client()
        return func(*args, **kwargs)

    return wrapper
