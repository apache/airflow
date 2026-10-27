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
import time
from functools import cache
from typing import cast
from urllib.parse import urlsplit

import jwt
from asgiref.sync import async_to_sync
from fastapi import HTTPException, status
from jwt import PyJWKClient
from keycloak import KeycloakAuthenticationError, KeycloakError, KeycloakOpenID

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLI_AUTHORIZATION_CODE_ENABLED_KEY,
    CONF_CLIENT_ID_KEY,
    CONF_JWT_FEDERATED_CLIENT_IDS_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
    CONF_SERVER_URL_KEY,
)
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

# RFC 8252 section 7.3 loopback redirects. ``localhost`` is left out because it can resolve
# to a non-loopback interface.
CLI_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1"})
CLI_CALLBACK_PATH = "/callback"


def is_cli_authorization_code_enabled() -> bool:
    return conf.getboolean(CONF_SECTION_NAME, CONF_CLI_AUTHORIZATION_CODE_ENABLED_KEY, fallback=False)


def is_cli_loopback_redirect_uri(redirect_uri: str) -> bool:
    """Return whether ``redirect_uri`` is a loopback callback a command-line client listens on."""
    try:
        parts = urlsplit(redirect_uri)
        port = parts.port
    except ValueError:
        return False
    return (
        parts.scheme == "http"
        and parts.hostname in CLI_LOOPBACK_HOSTS
        and port is not None
        and parts.username is None
        and parts.path == CLI_CALLBACK_PATH
        and not parts.query
        and not parts.fragment
    )


def _create_api_jwt_from_keycloak_tokens(
    client: KeycloakOpenID, tokens: dict, expiration_time_in_seconds: int
) -> str:
    userinfo_raw: dict | bytes = client.userinfo(tokens["access_token"])
    userinfo: dict = json.loads(userinfo_raw) if isinstance(userinfo_raw, bytes) else userinfo_raw

    user = KeycloakAuthManagerUser(
        user_id=userinfo["sub"],
        name=userinfo["preferred_username"],
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
    )
    return get_auth_manager().generate_api_jwt(user, expiration_time_in_seconds=expiration_time_in_seconds)


@cache
def _get_jwks_client(issuer: str) -> PyJWKClient:
    """Return the cached JWK client for a Keycloak realm issuer."""
    return PyJWKClient(f"{issuer}/protocol/openid-connect/certs")


def create_token_for(
    username: str,
    password: str,
    expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
) -> str:
    client = KeycloakAuthManager.get_keycloak_client()

    try:
        tokens = client.token(username, password)
    except KeycloakAuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid credentials",
        )

    userinfo_raw: dict | bytes = client.userinfo(tokens["access_token"])
    # Decode bytes to dict if necessary
    userinfo: dict = json.loads(userinfo_raw) if isinstance(userinfo_raw, bytes) else userinfo_raw

    user = KeycloakAuthManagerUser(
        user_id=userinfo["sub"],
        name=userinfo["preferred_username"],
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
    )

    return get_auth_manager().generate_api_jwt(user, expiration_time_in_seconds=expiration_time_in_seconds)


def create_jwt_federated_token(
    assertion: str,
    expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
) -> str:
    """
    Create a token from a Keycloak access token obtained outside of Airflow.

    This authentication flow accepts an access token issued by Keycloak through any
    Keycloak-native mechanism (e.g. a "Signed JWT - Federated" client bound to an
    external OIDC identity provider such as a Kubernetes ServiceAccount issuer, or AWS
    IAM outbound identity federation). Airflow does not obtain the token on the caller's
    behalf, the caller must obtain it directly from Keycloak's token endpoint. Airflow
    verifies the token and calls Keycloak's UserInfo endpoint to validate it and retrieve
    user information.

    The token's signature, issuer, and audience are verified against this realm's JWKS.
    The ``aud`` claim (a string or a list) must include this Airflow client's id, which
    requires an Audience mapper on the federated client's scope in Keycloak. The calling
    client (``azp``) must also appear in the ``jwt_federated_client_ids`` allow-list
    below -- an ``aud`` match alone only proves the token was meant for Airflow, not
    that the issuing client has been vetted for machine auth. Any Keycloak validation
    error is returned as a generic ``403 Invalid Keycloak assertion`` response.
    """
    realm = conf.get(CONF_SECTION_NAME, CONF_REALM_KEY)
    server_url = conf.get(CONF_SECTION_NAME, CONF_SERVER_URL_KEY)
    client_id = conf.get(CONF_SECTION_NAME, CONF_CLIENT_ID_KEY)
    issuer = f"{server_url.rstrip('/')}/realms/{realm}"

    try:
        jwks_client = _get_jwks_client(issuer)
        signing_key = jwks_client.get_signing_key_from_jwt(assertion)
        claims = jwt.decode(
            assertion,
            signing_key.key,
            algorithms=["RS256"],
            audience=client_id,
            issuer=issuer,
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid Keycloak assertion",
        )

    allowed_client_ids = {
        allowed.strip()
        for allowed in conf.get(CONF_SECTION_NAME, CONF_JWT_FEDERATED_CLIENT_IDS_KEY, fallback="").split(",")
        if allowed.strip()
    }
    federated_client_id = claims.get("azp") or claims.get("client_id")
    if not federated_client_id or federated_client_id not in allowed_client_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid Keycloak assertion",
        )

    # Confirm the assertion is still live (not revoked) and fetch the same shape of
    # user info create_client_credentials_token uses, rather than trusting the JWT's
    # own claims alone.
    client = KeycloakAuthManager.get_keycloak_client()
    try:
        userinfo_raw: dict | bytes = client.userinfo(assertion)
    except KeycloakError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid Keycloak assertion",
        )
    userinfo: dict = json.loads(userinfo_raw) if isinstance(userinfo_raw, bytes) else userinfo_raw

    user = KeycloakAuthManagerUser(
        user_id=userinfo["sub"],
        name=userinfo.get("preferred_username", userinfo.get("clientId", "service-account")),
        access_token=assertion,
        refresh_token=None,
    )

    return get_auth_manager().generate_api_jwt(user, expiration_time_in_seconds=expiration_time_in_seconds)


def create_client_credentials_token(
    client_id: str,
    client_secret: str,
    expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_expiration_time"),
) -> str:
    """
    Create token using OAuth2 client_credentials grant type.

    This authentication flow uses the provided client_id and client_secret
    to obtain a token for a service account. The Keycloak client must have:
    - Service accounts roles: ON
    - Client Authentication: ON (confidential client)

    The service account must be configured with the appropriate roles/permissions.

    Only the client Airflow is configured to use is accepted. The route this is reached
    from is unauthenticated, so without that restriction the credentials of any
    confidential client in the realm would be usable to obtain an Airflow token.
    """
    if client_id != conf.get(CONF_SECTION_NAME, CONF_CLIENT_ID_KEY):
        # Deliberately the same response as a failed credential exchange below: telling
        # the caller which of the two checks rejected them would make the endpoint a
        # discovery oracle for client ids in the realm.
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Client credentials authentication failed",
        )

    # Get Keycloak client with service account credentials
    client = KeycloakAuthManager.get_keycloak_client(
        client_id=client_id,
        client_secret=client_secret,
    )

    try:
        tokens = client.token(grant_type="client_credentials")
    except KeycloakAuthenticationError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Client credentials authentication failed",
        )

    # For client_credentials, get the service account user info
    # The token represents the service account associated with the client
    userinfo_raw: dict | bytes = client.userinfo(tokens["access_token"])
    # Decode bytes to dict if necessary
    userinfo: dict = json.loads(userinfo_raw) if isinstance(userinfo_raw, bytes) else userinfo_raw

    user = KeycloakAuthManagerUser(
        user_id=userinfo["sub"],
        name=userinfo.get("preferred_username", userinfo.get("clientId", "service-account")),
        access_token=tokens["access_token"],
        refresh_token=tokens.get(
            "refresh_token"
        ),  # client_credentials may not return refresh_token (RFC6749 section 4.4.3)
    )

    return get_auth_manager().generate_api_jwt(user, expiration_time_in_seconds=expiration_time_in_seconds)


def create_token_from_authorization_code(
    code: str,
    code_verifier: str,
    redirect_uri: str,
    expiration_time_in_seconds: int = conf.getint("api_auth", "jwt_cli_expiration_time"),
) -> str:
    """
    Create a CLI token from an authorization code obtained through ``/login/cli``.

    Keycloak checks ``code_verifier`` against the challenge ``/login/cli`` forwarded, so
    the code is useless to anyone who intercepted it on the loopback redirect.
    """
    if not is_cli_authorization_code_enabled():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The authorization_code grant is not enabled for command-line clients",
        )
    if not is_cli_loopback_redirect_uri(redirect_uri):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid redirect_uri")

    client = KeycloakAuthManager.get_keycloak_client()
    try:
        tokens = client.token(
            grant_type="authorization_code",
            code=code,
            redirect_uri=redirect_uri,
            code_verifier=code_verifier,
            scope="openid",
        )
    except KeycloakError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid authorization code")

    return _create_api_jwt_from_keycloak_tokens(client, tokens, expiration_time_in_seconds)


def refresh_cli_token(token: str) -> str:
    """
    Create a CLI token with fresh Keycloak tokens from a CLI token that is still valid.

    The new token expires with the old one, so refreshing never extends a CLI session.
    Expired and revoked tokens are refused by ``get_user_from_token``.
    """
    auth_manager = get_auth_manager()
    try:
        user = cast("KeycloakAuthManagerUser | None", async_to_sync(auth_manager.get_user_from_token)(token))
    except jwt.PyJWTError:
        user = None
    if not user or not user.refresh_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

    client = KeycloakAuthManager.get_keycloak_client()
    try:
        tokens = client.refresh_token(user.refresh_token)
    except KeycloakError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

    user.access_token = tokens["access_token"]
    user.refresh_token = tokens["refresh_token"]
    # The signature and expiry were verified by get_user_from_token above.
    expires_at = jwt.decode(token, options={"verify_signature": False})["exp"]
    return auth_manager.generate_api_jwt(user, expiration_time_in_seconds=int(expires_at - time.time()))
