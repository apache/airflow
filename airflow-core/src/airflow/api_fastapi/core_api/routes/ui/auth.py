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

from fastapi import Depends

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.auth import (
    AuthenticatedMeResponse,
    GenerateTokenBody,
    GenerateTokenResponse,
    MenuItemCollectionResponse,
    TokenType,
)
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.configuration import conf

log = logging.getLogger(__name__)

auth_router = AirflowRouter(tags=["Auth Links"])


@auth_router.get("/auth/menus")
def get_auth_menus(
    user: GetUserDep,
) -> MenuItemCollectionResponse:
    authorized_menu_items = get_auth_manager().get_authorized_menu_items(user=user)
    extra_menu_items = get_auth_manager().get_extra_menu_items(user=user)

    return MenuItemCollectionResponse(
        authorized_menu_items=authorized_menu_items,
        extra_menu_items=extra_menu_items,
    )


@auth_router.get("/auth/me")
def get_current_user_info(
    user: GetUserDep,
) -> AuthenticatedMeResponse:
    """Convienently get the current authenticated user information."""
    return AuthenticatedMeResponse(
        id=user.get_id(),
        username=user.get_name(),
    )


@auth_router.post("/auth/token", dependencies=[Depends(action_logging())])
def generate_token(
    body: GenerateTokenBody,
    user: GetUserDep,
) -> GenerateTokenResponse:
    """Generate a JWT token for the authenticated user."""
    if body.token_type == TokenType.CLI:
        expiration_seconds = conf.getint("api_auth", "jwt_cli_expiration_time")
    else:
        expiration_seconds = conf.getint("api_auth", "jwt_expiration_time")

    access_token = get_auth_manager().generate_jwt(user, expiration_time_in_seconds=expiration_seconds)

    log.info(
        "User %s generated a %s token (expires in %d seconds)",
        user.get_name(),
        body.token_type.value,
        expiration_seconds,
    )

    return GenerateTokenResponse(
        access_token=access_token,
        token_type=body.token_type,
        expires_in_seconds=expiration_seconds,
    )
