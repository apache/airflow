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
Shared Snowflake REST API authentication (OAuth, PAT, key-pair JWT, workload identity).

Every Snowflake REST caller (the SQL API, Cortex Agents, the Cortex chat-completions
endpoint used by pydantic-ai) authenticates the same four ways: an OAuth access token,
a Programmatic Access Token (PAT), a JWT signed with the connection's private key, or a
Workload Identity Federation (WIF) token issued by the workload's cloud.
:class:`SnowflakeRestTokenProvider` produces those headers from a :class:`SnowflakeHook`
so each caller does not need to duplicate the branching or the token caching.

This module intentionally imports nothing from ``common.ai``, ``pydantic-ai``, or
``httpx2`` -- it is plain Snowflake REST auth and must stay usable by callers that never
touch those optional dependencies.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import jwt

from airflow.providers.snowflake.hooks.snowflake import _validate_account_component
from airflow.providers.snowflake.utils.sql_api_generate_jwt import JWTGenerator

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# Re-fetch a workload identity token this many seconds before it expires.
WIF_TOKEN_EXPIRY_BUFFER = 30
# Audience Snowflake expects in the identity token: its domain for AWS and GCP, and its Entra
# application for Azure.
WIF_AUDIENCE = "snowflakecomputing.com"
WIF_AZURE_RESOURCE = "api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad"


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
    a connection without a private key that sets ``workload_identity_provider`` uses Workload
    Identity Federation; anything else signs a key-pair JWT. The private key is loaded once and
    kept. The ``JWTGenerator`` is built once and kept, because it renews its own token on the
    ``token_renewal_delta`` schedule. The workload identity token is requested from the cloud
    with its own client library (``boto3``, ``azure-identity`` or ``google-auth``, imported only
    for the provider the connection names), cached until shortly before its ``exp`` claim, and
    not cached when it carries none.

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
        self._wif_token: SnowflakeRestToken | None = None
        self._wif_token_expires_at: float = 0.0
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
                    return self._get_workload_identity_token(conn_config)
                raise ValueError(
                    "Snowflake REST API authentication requires an OAuth access token, a "
                    "Programmatic Access Token (PAT), a private key for key-pair JWT auth, or a "
                    "workload_identity_provider for Workload Identity Federation; "
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

    def _get_workload_identity_token(self, conn_config: dict[str, Any]) -> SnowflakeRestToken:
        """
        Return a Workload Identity Federation token, fetching a new one when the cached one expires.

        The caller must hold ``self._lock``.
        """
        if self._wif_token is not None and time.time() < self._wif_token_expires_at:
            return self._wif_token

        provider_name = str(conn_config["workload_identity_provider"]).upper()
        if provider_name == "AWS":
            credential = _fetch_aws_identity_token()
        elif provider_name == "AZURE":
            credential = _fetch_azure_identity_token()
        elif provider_name == "GCP":
            credential = _fetch_gcp_identity_token()
        elif provider_name == "OIDC":
            credential = self._get_oidc_token(conn_config)
        else:
            raise ValueError(
                f"Unknown workload_identity_provider {provider_name!r}; expected one of "
                "AWS, AZURE, GCP or OIDC."
            )

        token = SnowflakeRestToken(
            token=f"WIF.{provider_name}.{credential}",
            token_type="WORKLOAD_IDENTITY_FEDERATION",
        )
        expires_at = self._get_jwt_expiry(credential)
        self._wif_token = token if expires_at else None
        self._wif_token_expires_at = (expires_at or 0.0) - WIF_TOKEN_EXPIRY_BUFFER
        return token

    @staticmethod
    def _get_oidc_token(conn_config: dict[str, Any]) -> str:
        """Return the OIDC ID token from the connection ``token`` or ``token_file_path`` extra."""
        if token := conn_config.get("token"):
            return token
        if token_file_path := conn_config.get("token_file_path"):
            return Path(token_file_path).read_text().strip()
        raise ValueError(
            "Workload Identity Federation with the OIDC provider requires the connection extra "
            "'token' or 'token_file_path'."
        )

    @staticmethod
    def _get_jwt_expiry(credential: str) -> float | None:
        """Return the ``exp`` claim of ``credential`` without verifying it, or None if it has none."""
        try:
            exp = jwt.decode(credential, options={"verify_signature": False}).get("exp")
        except jwt.PyJWTError:
            return None
        return float(exp) if isinstance(exp, int | float) else None

    def build_auth_headers(self) -> dict[str, str]:
        """Return the two REST auth headers: ``Authorization`` and the token-type header."""
        token = self.get_token()
        return {
            "Authorization": f"Bearer {token.token}",
            "X-Snowflake-Authorization-Token-Type": token.token_type,
        }


def _fetch_aws_identity_token() -> str:
    """Return the JWT from ``sts:GetWebIdentityToken`` for the workload's AWS identity."""
    try:
        import boto3
    except ImportError as e:
        raise ValueError(
            "Workload Identity Federation with the AWS provider requires the boto3 package."
        ) from e
    sts = boto3.client("sts")
    if not hasattr(sts, "get_web_identity_token"):
        raise ValueError(
            "Workload Identity Federation with the AWS provider requires a boto3 version that "
            "supports sts:GetWebIdentityToken."
        )
    response = sts.get_web_identity_token(Audience=[WIF_AUDIENCE], SigningAlgorithm="ES384")
    return response["WebIdentityToken"]


def _fetch_azure_identity_token() -> str:
    """
    Return an Entra access token for Snowflake's application, issued to the workload's identity.

    Uses the AKS workload identity when the pod has one (the Azure Workload Identity webhook sets
    ``AZURE_CLIENT_ID``, ``AZURE_TENANT_ID`` and ``AZURE_FEDERATED_TOKEN_FILE``), and the managed
    identity otherwise. A user-assigned managed identity is selected with
    ``MANAGED_IDENTITY_CLIENT_ID``.
    """
    try:
        from azure.identity import ManagedIdentityCredential, WorkloadIdentityCredential
    except ImportError as e:
        raise ValueError(
            "Workload Identity Federation with the AZURE provider requires the azure-identity package."
        ) from e
    credential: ManagedIdentityCredential | WorkloadIdentityCredential
    if all(
        os.environ.get(name) for name in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE")
    ):
        credential = WorkloadIdentityCredential()
    else:
        credential = ManagedIdentityCredential(client_id=os.environ.get("MANAGED_IDENTITY_CLIENT_ID"))
    try:
        return credential.get_token(f"{WIF_AZURE_RESOURCE}/.default").token
    finally:
        credential.close()


def _fetch_gcp_identity_token() -> str:
    """Return a Google ID token for Snowflake's audience, issued to the workload's service account."""
    try:
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token
    except ImportError as e:
        raise ValueError(
            "Workload Identity Federation with the GCP provider requires the google-auth package."
        ) from e
    return id_token.fetch_id_token(Request(), WIF_AUDIENCE)


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
