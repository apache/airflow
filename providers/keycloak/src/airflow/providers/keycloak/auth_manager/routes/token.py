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

from starlette import status

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.datamodels.token import (
    TokenBody,
    TokenPasswordBody,
    TokenResponse,
)

log = logging.getLogger(__name__)
token_router = AirflowRouter(tags=["KeycloakAuthManagerToken"])


@token_router.post(
    "/token",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token(body: TokenBody) -> TokenResponse:
    token = body.root.create_token(
        expiration_time_in_seconds=int(conf.getint("api_auth", "jwt_expiration_time"))
    )
    return TokenResponse(access_token=token)


@token_router.post(
    "/token/cli",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token_cli(body: TokenPasswordBody) -> TokenResponse:
    token = body.create_token(
        expiration_time_in_seconds=int(conf.getint("api_auth", "jwt_cli_expiration_time"))
    )
    return TokenResponse(access_token=token)
