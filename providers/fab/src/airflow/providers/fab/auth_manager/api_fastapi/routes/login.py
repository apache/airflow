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

from typing import Any

from fastapi import Body, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.providers.common.compat.sdk import conf
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginResponse
from airflow.providers.fab.auth_manager.api_fastapi.routes.router import auth_router
from airflow.providers.fab.auth_manager.api_fastapi.services.login import FABAuthManagerLogin
from airflow.providers.fab.version_compat import AIRFLOW_V_3_1_8_PLUS

if AIRFLOW_V_3_1_8_PLUS:
    from airflow.api_fastapi.app import get_cookie_path
else:
    get_cookie_path = lambda: "/"


def _get_flask_app():
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

    auth_manager = get_auth_manager()
    if not isinstance(auth_manager, FabAuthManager):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "FabAuthManager is not configured as the auth manager. "
                "Ensure AUTH_MANAGER is set to FabAuthManager in your Airflow configuration."
            ),
        )
    if not auth_manager.flask_app:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Flask app is not initialized. Check that FabAuthManager started up correctly.",
        )
    return auth_manager.flask_app


@auth_router.post(
    "/token",
    response_model=LoginResponse,
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token(request: Request, body: dict[str, Any] = Body(...)) -> LoginResponse:
    """Generate a new API token."""
    with _get_flask_app().app_context():
        return FABAuthManagerLogin.create_token(headers=dict(request.headers), body=body)


@auth_router.post(
    "/token/cli",
    response_model=LoginResponse,
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token_cli(request: Request, body: dict[str, Any] = Body(...)) -> LoginResponse:
    """Generate a new CLI API token."""
    with _get_flask_app().app_context():
        return FABAuthManagerLogin.create_token(
            headers=dict(request.headers),
            body=body,
            expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time"),
        )


@auth_router.get(
    "/logout",
    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
)
def logout(request: Request) -> RedirectResponse:
    """Clear session cookies and redirect to the login page."""
    with _get_flask_app().app_context():
        login_url = get_auth_manager().get_url_login()
        secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
        cookie_path = get_cookie_path()
        response = RedirectResponse(login_url)
        response.delete_cookie(
            key="session",
            path=cookie_path,
            secure=secure,
            httponly=True,
        )
        response.delete_cookie(
            key=COOKIE_NAME_JWT_TOKEN,
            path=cookie_path,
            secure=secure,
            httponly=True,
        )
        return response
