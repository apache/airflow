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
Shared Snowflake REST API authentication (OAuth, PAT, key-pair JWT).

Every Snowflake REST caller (the SQL API, Cortex Agents, the Cortex chat-completions
endpoint used by pydantic-ai) authenticates the same three ways: an OAuth access token,
a Programmatic Access Token (PAT), or a JWT signed with the connection's private key.
:class:`SnowflakeRestTokenProvider` produces those headers from a :class:`SnowflakeHook`
so each caller does not need to duplicate the branching or the token caching.

This module intentionally imports nothing from ``common.ai``, ``pydantic-ai``, or
``httpx2`` -- it is plain Snowflake REST auth and must stay usable by callers that never
touch those optional dependencies.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.snowflake.hooks.snowflake import _validate_account_component
from airflow.providers.snowflake.utils.sql_api_generate_jwt import JWTGenerator

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@dataclass(frozen=True)
class SnowflakeRestToken:
    """A REST bearer token and the ``X-Snowflake-Authorization-Token-Type`` value it needs."""

    token: str = field(repr=False)
    token_type: str


class SnowflakeRestTokenProvider:
    """
    Produce Snowflake REST auth headers from a :class:`SnowflakeHook`, caching what it can.

    The branch taken mirrors ``SnowflakeSqlApiHook.get_headers``: ``authenticator == "oauth"``
    reads the token ``hook._get_conn_params()`` already resolved (which itself refreshes an
    expiring OAuth or Azure token, so this provider does not cache that branch at all);
    ``authenticator == "programmatic_access_token"`` reads the PAT from the connection password;
    anything else signs a key-pair JWT. The private key is loaded once and kept. The
    ``JWTGenerator`` is built once and kept, because it renews its own token on the
    ``token_renewal_delta`` schedule.

    :param hook: The ``SnowflakeHook`` (or subclass) whose connection supplies credentials.
    :param token_life_time: Passed to the ``JWTGenerator`` for the key-pair branch.
    :param token_renewal_delta: Passed to the ``JWTGenerator`` for the key-pair branch. When this
        is greater than or equal to ``token_life_time``, the JWT is renewed on every call instead
        -- otherwise the scheduled renewal would land after the token has already expired, and
        every call in between would serve a token Snowflake rejects.
    :param private_key_loader: Callable returning the private key for the key-pair branch,
        called at most once. Defaults to ``hook.get_private_key``. A caller that already
        maintains its own ``private_key`` attribute (e.g. ``SnowflakeSqlApiHook``) can pass a
        loader that populates it, so that attribute keeps working for existing callers.
    """

    def __init__(
        self,
        hook: SnowflakeHook,
        *,
        token_life_time: timedelta = JWTGenerator.LIFETIME,
        token_renewal_delta: timedelta = JWTGenerator.RENEWAL_DELTA,
        private_key_loader: Callable[[], PrivateKeyTypes | None] | None = None,
    ) -> None:
        self._hook = hook
        self._token_life_time = token_life_time
        self._token_renewal_delta = (
            timedelta(0) if token_renewal_delta >= token_life_time else token_renewal_delta
        )
        self._private_key_loader: Callable[[], PrivateKeyTypes | None] = (
            private_key_loader or hook.get_private_key
        )
        self._private_key: PrivateKeyTypes | None = None
        self._jwt_generator: JWTGenerator | None = None
        self._lock = threading.Lock()

    def get_token(self) -> SnowflakeRestToken:
        """Return the current REST bearer token, refreshing or renewing it as needed."""
        with self._lock:
            conn_config = self._hook._get_conn_params()

            if conn_config.get("authenticator") == "oauth":
                token = conn_config.get("token")
                if not token:
                    raise ValueError("OAuth authentication did not produce an access token.")
                return SnowflakeRestToken(token=token, token_type="OAUTH")

            if conn_config.get("authenticator") == "programmatic_access_token":
                pat = conn_config.get("password")
                if not pat:
                    raise ValueError(
                        "Programmatic Access Token (PAT) authentication requires the connection "
                        "password field to contain the PAT token value."
                    )
                return SnowflakeRestToken(token=pat, token_type="PROGRAMMATIC_ACCESS_TOKEN")

            if self._private_key is None:
                self._private_key = self._private_key_loader()
            if self._private_key is None:
                if conn_config.get("workload_identity_provider"):
                    raise ValueError(
                        "Workload identity federation is not supported for Snowflake REST APIs; use "
                        "OAuth, PAT, or key-pair authentication instead. A connection that also "
                        "sets a private key uses key-pair authentication."
                    )
                raise ValueError(
                    "Snowflake REST API authentication requires an OAuth access token, a "
                    "Programmatic Access Token (PAT), or a private key for key-pair JWT auth; "
                    "none is configured on this connection."
                )

            account = conn_config.get("account")
            user = conn_config.get("user")
            if not account or not user:
                missing = " and ".join(
                    name for name, value in (("account", account), ("login (user)", user)) if not value
                )
                raise ValueError(
                    "Key-pair JWT authentication requires the Snowflake connection's account and "
                    f"login (user); missing: {missing}."
                )

            if self._jwt_generator is None:
                self._jwt_generator = JWTGenerator(
                    account,
                    user,
                    private_key=self._private_key,
                    lifetime=self._token_life_time,
                    renewal_delay=self._token_renewal_delta,
                )
            token = self._jwt_generator.get_token()
            return SnowflakeRestToken(token=token, token_type="KEYPAIR_JWT")

    def build_auth_headers(self) -> dict[str, str]:
        """Return the two REST auth headers: ``Authorization`` and the token-type header."""
        token = self.get_token()
        return {
            "Authorization": f"Bearer {token.token}",
            "X-Snowflake-Authorization-Token-Type": token.token_type,
        }


def get_cortex_base_url(conn_config: dict[str, Any]) -> str:
    """
    Return the base URL for a Snowflake account's Cortex REST endpoints.

    The extra field ``host`` wins when set. Otherwise it is built from ``account`` and, when
    set, ``region`` -- the same host ``SnowflakeHook.account_identifier`` builds for the SQL API,
    which is what a legacy account-locator identifier (``xy12345`` plus ``us-east-2.aws``) needs.

    :param conn_config: A connection params dict as returned by ``SnowflakeHook._get_conn_params``
        or ``_get_static_conn_params`` (only ``host``, ``account`` and ``region`` are read).
    """
    host = conn_config.get("host")
    if host:
        return f"https://{host}"

    account = _validate_account_component(conn_config["account"], "account")
    region = conn_config.get("region")
    if region:
        region = _validate_account_component(region, "region")
        return f"https://{account}.{region}.snowflakecomputing.com"
    return f"https://{account}.snowflakecomputing.com"
