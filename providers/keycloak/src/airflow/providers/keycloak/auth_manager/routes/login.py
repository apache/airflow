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
import secrets
from typing import cast
from urllib.parse import quote, urljoin

from fastapi import Request  # noqa: TC002
from fastapi.responses import HTMLResponse, RedirectResponse

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX, get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.providers.keycloak.version_compat import AIRFLOW_V_3_1_1_PLUS, AIRFLOW_V_3_1_8_PLUS

if AIRFLOW_V_3_1_8_PLUS:
    from airflow.api_fastapi.app import get_cookie_path
else:
    get_cookie_path = lambda: "/"

try:
    from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
except ImportError:

    class AuthManagerRefreshTokenExpiredException(Exception):  # type: ignore[no-redef]
        """In case it is using a version of Airflow without ``AuthManagerRefreshTokenExpiredException``."""

        pass


from airflow.api_fastapi.common.router import AirflowRouter
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

log = logging.getLogger(__name__)
login_router = AirflowRouter(tags=["KeycloakAuthManagerLogin"])

COOKIE_NAME_ID_TOKEN = "_id_token"
COOKIE_NAME_OAUTH_STATE = "_oauth_state"


@login_router.get("/login")
def login(request: Request) -> RedirectResponse:
    """Initiate the authentication."""
    client = KeycloakAuthManager.get_keycloak_client()
    redirect_uri = request.url_for("login_callback")
    state = secrets.token_urlsafe(32)
    auth_url = client.auth_url(redirect_uri=str(redirect_uri), scope="openid", state=state)
    response = RedirectResponse(auth_url)
    secure = bool(conf.get("api", "ssl_cert", fallback=""))
    cookie_path = get_cookie_path()
    response.set_cookie(
        COOKIE_NAME_OAUTH_STATE, state, max_age=300, path=cookie_path, httponly=True, secure=secure
    )
    return response


@login_router.get("/login_callback")
def login_callback(request: Request):
    """Authenticate the user."""
    code = request.query_params.get("code")
    if not code:
        return HTMLResponse("Missing code", status_code=400)
    state_q = request.query_params.get("state", "")
    state_c = request.cookies.get(COOKIE_NAME_OAUTH_STATE, "")
    if not state_q or not state_c or not secrets.compare_digest(state_q, state_c):
        return HTMLResponse("Invalid OAuth state parameter", status_code=403)

    client = KeycloakAuthManager.get_keycloak_client()
    redirect_uri = request.url_for("login_callback")

    tokens = client.token(
        grant_type="authorization_code",
        code=code,
        redirect_uri=str(redirect_uri),
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
    token = get_auth_manager().generate_jwt(user)

    response = RedirectResponse(url=conf.get("api", "base_url", fallback="/"), status_code=303)
    secure = bool(conf.get("api", "ssl_cert", fallback=""))
    # In Airflow 3.1.1 authentication changes, front-end no longer handle the token
    # See https://github.com/apache/airflow/pull/55506
    cookie_path = get_cookie_path()
    if AIRFLOW_V_3_1_1_PLUS:
        response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, path=cookie_path, secure=secure, httponly=True)
    else:
        response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, path=cookie_path, secure=secure)

    # Save id token as separate cookie.
    # Cookies have a size limit (usually 4k), saving all the tokens in a same cookie goes beyond this limit
    response.set_cookie(
        COOKIE_NAME_ID_TOKEN, tokens["id_token"], path=cookie_path, secure=secure, httponly=True
    )

    return response


@login_router.get("/logout")
def logout(request: Request):
    """Log out the user from Keycloak."""
    auth_manager = cast("KeycloakAuthManager", get_auth_manager())
    keycloak_config = auth_manager.get_keycloak_client().well_known()
    end_session_endpoint = keycloak_config["end_session_endpoint"]

    id_token = request.cookies.get(COOKIE_NAME_ID_TOKEN)
    base_url = conf.get("api", "base_url", fallback="/")
    post_logout_redirect_uri = urljoin(base_url, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/logout_callback")

    # Validate id_token format before using in redirect (JWT tokens have 3 parts separated by dots)
    if id_token and id_token.count(".") == 2 and all(c.isalnum() or c in ".-_" for c in id_token):
        encoded_id_token = quote(id_token, safe="")
        logout_url = (
            f"{end_session_endpoint}?post_logout_redirect_uri={post_logout_redirect_uri}"
            f"&id_token_hint={encoded_id_token}"
        )
    else:
        logout_url = str(post_logout_redirect_uri)

    return RedirectResponse(logout_url)


@login_router.get("/logout_callback")
def logout_callback(request: Request):
    """
    Complete the log-out.

    This callback is redirected by Keycloak after the user has been logged out from Keycloak.
    """
    login_url = get_auth_manager().get_url_login()
    secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
    cookie_path = get_cookie_path()
    response = RedirectResponse(login_url)
    response.delete_cookie(
        key=COOKIE_NAME_JWT_TOKEN,
        path=cookie_path,
        secure=secure,
        httponly=True,
    )
    response.delete_cookie(
        key=COOKIE_NAME_ID_TOKEN,
        path=cookie_path,
        secure=secure,
        httponly=True,
    )
    return response
