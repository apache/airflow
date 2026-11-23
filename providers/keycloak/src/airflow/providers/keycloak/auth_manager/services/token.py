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
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
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
    CONF_CLIENT_ID_KEY,
    CONF_JWT_FEDERATED_CLIENT_IDS_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
    CONF_SERVER_URL_KEY,
    DEVICE_CODE_GRANT_TYPE,
)
from airflow.providers.keycloak.auth_manager.exceptions import (
    AuthManagerRefreshTokenExpiredException,
    is_keycloak_outage,
)
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import (
    KeycloakAuthManager,
    is_cli_authorization_code_enabled,
    is_cli_device_code_enabled,
)
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser
from airflow.providers.keycloak.version_compat import AIRFLOW_V_3_2_PLUS

# RFC 8252 section 7.3 loopback redirects. ``localhost`` is left out because it can resolve
# to a non-loopback interface.
CLI_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1"})
CLI_CALLBACK_PATH = "/callback"
# RFC 8628 section 3.5 errors that the polling client acts on, so they are passed on as-is.
DEVICE_CODE_CLIENT_ERRORS = frozenset(
    {"authorization_pending", "slow_down", "access_denied", "expired_token"}
)

log = logging.getLogger(__name__)


def build_keycloak_unavailable_error(error: Exception) -> HTTPException:
    log.warning(
        "Keycloak is unavailable (%s, response code %s)",
        type(error).__name__,
        getattr(error, "response_code", None),
    )
    return HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Keycloak is unavailable")


@contextmanager
def translate_keycloak_outage(rejected_detail: str | None = None) -> Iterator[None]:
    """
    Raise a ``503`` instead of a Keycloak error that :func:`is_keycloak_outage` matches.

    With ``rejected_detail``, any other Keycloak error becomes a ``403`` with that detail.
    """
    try:
        yield
    except KeycloakError as e:
        if is_keycloak_outage(e):
            raise build_keycloak_unavailable_error(e) from e
        if rejected_detail is None:
            raise
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=rejected_detail)


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
        # Reject userinfo: ``:x@127.0.0.1:1`` still has a loopback hostname.
        and "@" not in parts.netloc
        and parts.path == CLI_CALLBACK_PATH
        and not parts.query
        and not parts.fragment
    )


def build_user_from_userinfo(
    client: KeycloakOpenID, tokens: dict, *, is_service_account: bool = False
) -> KeycloakAuthManagerUser:
    """Build the Airflow user that Keycloak's UserInfo endpoint describes for ``tokens``."""
    userinfo_raw: dict | bytes = client.userinfo(tokens["access_token"])
    userinfo: dict = json.loads(userinfo_raw) if isinstance(userinfo_raw, bytes) else userinfo_raw

    # The fallback is for machine clients only: a real user must never appear as "service-account".
    name = (
        userinfo.get("preferred_username", userinfo.get("clientId", "service-account"))
        if is_service_account
        else userinfo["preferred_username"]
    )

    return KeycloakAuthManagerUser(
        user_id=userinfo["sub"],
        name=name,
        access_token=tokens["access_token"],
        # Absent for client_credentials and clients with "Use Refresh Tokens" off.
        refresh_token=tokens.get("refresh_token"),
    )


def _create_api_jwt_from_keycloak_tokens(
    client: KeycloakOpenID,
    tokens: dict,
    expiration_time_in_seconds: int,
    *,
    is_service_account: bool = False,
) -> str:
    user = build_user_from_userinfo(client, tokens, is_service_account=is_service_account)
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

    with translate_keycloak_outage():
        try:
            tokens = client.token(username, password)
        except KeycloakAuthenticationError:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid credentials",
            )
        return _create_api_jwt_from_keycloak_tokens(client, tokens, expiration_time_in_seconds)


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
    except jwt.PyJWKClientConnectionError as e:
        raise build_keycloak_unavailable_error(e) from e
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

    # Confirm the assertion is still live (not revoked) rather than trusting the JWT's
    # own claims alone.
    client = KeycloakAuthManager.get_keycloak_client()
    with translate_keycloak_outage(rejected_detail="Invalid Keycloak assertion"):
        user = build_user_from_userinfo(client, {"access_token": assertion}, is_service_account=True)

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

    with translate_keycloak_outage():
        try:
            tokens = client.token(grant_type="client_credentials")
        except KeycloakAuthenticationError:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Client credentials authentication failed",
            )
        return _create_api_jwt_from_keycloak_tokens(
            client, tokens, expiration_time_in_seconds, is_service_account=True
        )


def create_token_from_authorization_code(
    code: str,
    code_verifier: str,
    redirect_uri: str,
    expiration_time_in_seconds: int,
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
    with translate_keycloak_outage(rejected_detail="Invalid authorization code"):
        tokens = client.token(
            grant_type="authorization_code",
            code=code,
            redirect_uri=redirect_uri,
            code_verifier=code_verifier,
            scope="openid",
        )
    with translate_keycloak_outage():
        return _create_api_jwt_from_keycloak_tokens(client, tokens, expiration_time_in_seconds)


def _require_cli_device_code_enabled() -> None:
    if not is_cli_device_code_enabled():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Device code login is not enabled for command-line clients",
        )


def _find_oauth_error(error: KeycloakError) -> str | None:
    """Return the RFC 6749 ``error`` code of a Keycloak error response, or ``None``."""
    try:
        body = json.loads(error.response_body or b"")
    except (TypeError, ValueError):
        return None
    code = body.get("error") if isinstance(body, dict) else None
    return code if isinstance(code, str) else None


def start_device_authorization() -> dict:
    """Start a device code login of a command-line client and return Keycloak's RFC 8628 response."""
    _require_cli_device_code_enabled()
    client = KeycloakAuthManager.get_keycloak_client()
    try:
        with translate_keycloak_outage():
            return client.device(scope="openid")
    except KeycloakError as e:
        # Usually the Keycloak client does not have the OAuth 2.0 Device Authorization Grant turned on.
        log.error("Keycloak refused the device authorization request: %s", _find_oauth_error(e) or e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Keycloak refused the device authorization request",
        )


def create_token_from_device_code(device_code: str, expiration_time_in_seconds: int) -> str:
    """
    Create a CLI token from a device code that the user approved.

    While the login is not approved, this raises a ``400`` whose ``detail`` is the RFC 8628 error code.
    """
    _require_cli_device_code_enabled()
    client = KeycloakAuthManager.get_keycloak_client()
    try:
        with translate_keycloak_outage():
            tokens = client.token(grant_type=DEVICE_CODE_GRANT_TYPE, device_code=device_code, scope="openid")
    except KeycloakError as e:
        error = _find_oauth_error(e)
        if error in DEVICE_CODE_CLIENT_ERRORS:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=error)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid device code")
    with translate_keycloak_outage():
        return _create_api_jwt_from_keycloak_tokens(client, tokens, expiration_time_in_seconds)


def refresh_cli_token(token: str, expiration_time_in_seconds: int) -> str:
    """
    Create a CLI token with fresh Keycloak tokens from a CLI token that is still valid.

    The new token expires no later than the old one, so refreshing never extends a CLI session, and
    lasts at most ``expiration_time_in_seconds``, so a longer-lived API token does not refresh into a
    CLI token outliving ``[api_auth] jwt_cli_expiration_time``. On Airflow 3.2+ the old token is
    revoked; earlier versions have no token revocation, so it stays usable until it expires. Expired
    and revoked tokens are refused by ``get_user_from_token``.
    """
    auth_manager = cast("KeycloakAuthManager", get_auth_manager())
    try:
        user = cast("KeycloakAuthManagerUser | None", async_to_sync(auth_manager.get_user_from_token)(token))
    except jwt.PyJWTError:
        user = None
    if not user:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")
    if not user.refresh_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="This token cannot be refreshed because Keycloak issued it without a refresh token",
        )

    # get_user_from_token allows [api_auth] jwt_leeway past exp.
    expires_at = jwt.decode(token, options={"verify_signature": False})["exp"]
    expiration_time_in_seconds = min(int(expires_at - time.time()), expiration_time_in_seconds)
    # BaseAuthManager._get_token_signer caches one signer per lifetime: keep the distinct lifetimes few.
    if expiration_time_in_seconds > 60:
        expiration_time_in_seconds -= expiration_time_in_seconds % 60
    if expiration_time_in_seconds <= 0:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

    try:
        with translate_keycloak_outage():
            tokens = auth_manager.refresh_tokens(user=user)
    except AuthManagerRefreshTokenExpiredException as e:
        # refresh_tokens raises this for every Keycloak error it gets back, outages included.
        if isinstance(e.__context__, KeycloakError) and is_keycloak_outage(e.__context__):
            raise build_keycloak_unavailable_error(e.__context__) from e
        tokens = {}
    # Before Airflow 3.1.7 there is no AuthManagerRefreshTokenExpiredException: refresh_tokens returns {}.
    if not tokens:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token")

    user.access_token = tokens["access_token"]
    # RFC 6749 section 6: a refresh response may omit a new refresh token, and the old one stays valid.
    user.refresh_token = tokens.get("refresh_token", user.refresh_token)
    new_token = auth_manager.generate_api_jwt(user, expiration_time_in_seconds=expiration_time_in_seconds)
    if AIRFLOW_V_3_2_PLUS:
        # Otherwise the old token stays usable and refreshable until it expires.
        auth_manager.revoke_token(token)
    return new_token
