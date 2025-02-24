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

from datetime import timedelta
from typing import cast

from starlette import status

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.configuration import conf
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from airflow.providers.fab.auth_manager.models.login import CLIAPITokenResponse

login_router = AirflowRouter(tags=["FabAuthManager"])


@login_router.post(
    "/token/cli",
    response_model=CLIAPITokenResponse,
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_401_UNAUTHORIZED]),
)
def create_token_cli() -> CLIAPITokenResponse:
    """Generate a new CLI API token."""
    auth_manager = cast(FabAuthManager, get_auth_manager())

    # There is no user base expiration in FAB, so we can't use this.
    # We can only set this globally but this would impact global expiration time for all tokens.
    auth_manager.appbuilder.app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(
        seconds=conf.getint("api", "auth_jwt_cli_expiration_time")
    )

    # Get token for implementing custom expiration time for JWT token
    # token = auth_manager.get_jwt_token(user=auth_manager.get_user())

    # Refresh the token with the new expiration time
    return CLIAPITokenResponse(
        jwt_cli_token=auth_manager.security_manager.refresh_jwt_token(user=auth_manager.get_user())
    )
