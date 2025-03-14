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

from urllib.parse import urljoin

from fastapi import HTTPException, status
from starlette.responses import RedirectResponse

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.simple.datamodels.login import LoginBody, LoginResponse
from airflow.api_fastapi.auth.managers.simple.services.login import SimpleAuthManagerLogin
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
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
    return SimpleAuthManagerLogin.create_token(body=body)


@login_router.get(
    "/token",
    status_code=status.HTTP_307_TEMPORARY_REDIRECT,
    responses=create_openapi_http_exception_doc([status.HTTP_403_FORBIDDEN]),
)
def create_token_all_admins() -> RedirectResponse:
    """Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True."""
    is_simple_auth_manager_all_admins = conf.getboolean("core", "simple_auth_manager_all_admins")
    if not is_simple_auth_manager_all_admins:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "This method is only allowed if ``[core] simple_auth_manager_all_admins`` is True",
        )
    user = SimpleAuthManagerUser(
        username="Anonymous",
        role="ADMIN",
    )
    url = urljoin(conf.get("api", "base_url"), f"?token={get_auth_manager().generate_jwt(user)}")
    return RedirectResponse(url=url)


@login_router.post(
    "/token/cli",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token_cli(
    body: LoginBody,
) -> LoginResponse:
    """Authenticate the user for the CLI."""
    return SimpleAuthManagerLogin.create_token(
        body=body, expiration_time_in_sec=conf.getint("api_auth", "jwt_cli_expiration_time")
    )
