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

from fastapi import status
from starlette.responses import RedirectResponse

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.simple.datamodels.login import LoginBody, LoginResponse
from airflow.api_fastapi.auth.managers.simple.services.login import SimpleAuthManagerLogin
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.configuration import conf

login_router = AirflowRouter(tags=["SimpleAuthManagerLogin"])


@login_router.post(
    "/token",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token(
    body: LoginBody,
) -> LoginResponse:
    """Authenticate the user."""
    return LoginResponse(access_token=SimpleAuthManagerLogin.create_token(body=body))


@login_router.get(
    "/token",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_403_FORBIDDEN]),
)
def create_token_all_admins() -> LoginResponse:
    """Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True."""
    return LoginResponse(access_token=SimpleAuthManagerLogin.create_token_all_admins())


@login_router.get(
    "/token/login",
    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    responses=create_openapi_http_exception_doc([status.HTTP_403_FORBIDDEN]),
)
def login_all_admins() -> RedirectResponse:
    """Login the user with no credentials."""
    response = RedirectResponse(url=conf.get("api", "base_url"))
    secure = conf.has_option("api", "ssl_cert")
    response.set_cookie(
        COOKIE_NAME_JWT_TOKEN,
        SimpleAuthManagerLogin.create_token_all_admins(),
        secure=secure,
    )
    return response


@login_router.post(
    "/token/cli",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token_cli(
    body: LoginBody,
) -> LoginResponse:
    """Authenticate the user for the CLI."""
    return LoginResponse(
        access_token=SimpleAuthManagerLogin.create_token(
            body=body, expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time")
        )
    )
