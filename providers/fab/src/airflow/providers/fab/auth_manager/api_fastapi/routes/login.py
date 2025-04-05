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

from starlette import status

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.configuration import conf
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginBody, LoginResponse
from airflow.providers.fab.auth_manager.api_fastapi.services.login import FABAuthManagerLogin

login_router = AirflowRouter(tags=["FabAuthManager"])


@login_router.post(
    "/token",
    response_model=LoginResponse,
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token(body: LoginBody) -> LoginResponse:
    """Generate a new API token."""
    return FABAuthManagerLogin.create_token(body=body)


@login_router.post(
    "/token/cli",
    response_model=LoginResponse,
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token_cli(body: LoginBody) -> LoginResponse:
    """Generate a new CLI API token."""
    return FABAuthManagerLogin.create_token(
        body=body, expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time")
    )
