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

from fastapi import Depends, Request
from starlette.responses import HTMLResponse, RedirectResponse

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.security import get_user
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser
from airflow.providers.keycloak.version_compat import AIRFLOW_V_3_1_1_PLUS

log = logging.getLogger(__name__)
login_router = AirflowRouter(tags=["KeycloakAuthManagerLogin"])


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
    return response


@login_router.get("/logout")
def logout(request: Request, user: Annotated[KeycloakAuthManagerUser, Depends(get_user)]):
    """Log out the user from Keycloak."""
    auth_manager = cast("KeycloakAuthManager", get_auth_manager())
    keycloak_config = auth_manager.get_keycloak_client().well_known()
    end_session_endpoint = keycloak_config["end_session_endpoint"]

    # Use the refresh flow to get the id token, it avoids us to save the id token
    token_id = auth_manager.refresh_tokens(user=user).get("id_token")
    post_logout_redirect_uri = request.url_for("logout_callback")

    if token_id:
        logout_url = f"{end_session_endpoint}?post_logout_redirect_uri={post_logout_redirect_uri}&id_token_hint={token_id}"
    else:
        logout_url = f"{end_session_endpoint}?post_logout_redirect_uri={post_logout_redirect_uri}"

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
    return response


@login_router.get("/refresh")
def refresh(
    request: Request, user: Annotated[KeycloakAuthManagerUser, Depends(get_user)]
) -> RedirectResponse:
    """Refresh the token."""
    auth_manager = cast("KeycloakAuthManager", get_auth_manager())
    refreshed_user = auth_manager.refresh_user(user=user)
    redirect_url = request.query_params.get("next", conf.get("api", "base_url", fallback="/"))
    response = RedirectResponse(url=redirect_url, status_code=303)

    if refreshed_user:
        token = auth_manager.generate_jwt(refreshed_user)
        secure = bool(conf.get("api", "ssl_cert", fallback=""))
        response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, secure=secure)

    return response
