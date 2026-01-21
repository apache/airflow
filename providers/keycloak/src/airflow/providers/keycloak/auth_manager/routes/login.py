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

import logging
from typing import Annotated, cast

from fastapi import Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.status import HTTP_401_UNAUTHORIZED

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.providers.keycloak.version_compat import AIRFLOW_V_3_1_1_PLUS

try:
    from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
except ImportError:

    class AuthManagerRefreshTokenExpiredException(Exception):  # type: ignore[no-redef]
        """In case it is using a version of Airflow without ``AuthManagerRefreshTokenExpiredException``."""

        pass


from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.security import get_user
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

log = logging.getLogger(__name__)
login_router = AirflowRouter(tags=["KeycloakAuthManagerLogin"])

COOKIE_NAME_ID_TOKEN = "_id_token"


@login_router.get("/login")
def login(request: Request) -> RedirectResponse:
    """Initiate the authentication."""
    client = KeycloakAuthManager.get_keycloak_client()
    redirect_uri = request.url_for("login_callback")
    auth_url = client.auth_url(redirect_uri=str(redirect_uri), scope="openid")
    return RedirectResponse(auth_url)


@login_router.get("/login_callback")
def login_callback(request: Request):
    """Authenticate the user."""
    code = request.query_params.get("code")
    if not code:
        return HTMLResponse("Missing code", status_code=400)

    client = KeycloakAuthManager.get_keycloak_client()
    redirect_uri = request.url_for("login_callback")

    tokens = client.token(
        grant_type="authorization_code",
        code=code,
        redirect_uri=str(redirect_uri),
    )
    userinfo = client.userinfo(tokens["access_token"])
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
    if AIRFLOW_V_3_1_1_PLUS:
        response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, secure=secure, httponly=True)
    else:
        response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, secure=secure)

    # Save id token as separate cookie.
    # Cookies have a size limit (usually 4k), saving all the tokens in a same cookie goes beyond this limit
    response.set_cookie(COOKIE_NAME_ID_TOKEN, tokens["id_token"], secure=secure, httponly=True)

    return response


@login_router.get("/logout")
def logout(request: Request):
    """Log out the user from Keycloak."""
    auth_manager = cast("KeycloakAuthManager", get_auth_manager())
    keycloak_config = auth_manager.get_keycloak_client().well_known()
    end_session_endpoint = keycloak_config["end_session_endpoint"]

    id_token = request.cookies.get(COOKIE_NAME_ID_TOKEN)
    post_logout_redirect_uri = request.url_for("logout_callback")

    if id_token:
        logout_url = f"{end_session_endpoint}?post_logout_redirect_uri={post_logout_redirect_uri}&id_token_hint={id_token}"
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
    response = RedirectResponse(login_url)
    response.delete_cookie(
        key=COOKIE_NAME_JWT_TOKEN,
        secure=secure,
        httponly=True,
    )
    response.delete_cookie(
        key=COOKIE_NAME_ID_TOKEN,
        secure=secure,
        httponly=True,
    )
    return response


@login_router.get("/refresh")
def refresh(
    request: Request, user: Annotated[KeycloakAuthManagerUser, Depends(get_user)]
) -> RedirectResponse:
    """Refresh the token."""
    auth_manager = cast("KeycloakAuthManager", get_auth_manager())
    try:
        refreshed_user = auth_manager.refresh_user(user=user)
    except AuthManagerRefreshTokenExpiredException:
        raise HTTPException(status_code=HTTP_401_UNAUTHORIZED, detail="Refresh token Expired")
    redirect_url = request.query_params.get("next", conf.get("api", "base_url", fallback="/"))
    response = RedirectResponse(url=redirect_url, status_code=303)

    if refreshed_user:
        token = auth_manager.generate_jwt(refreshed_user)
        secure = bool(conf.get("api", "ssl_cert", fallback=""))
        response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, secure=secure)

    return response
