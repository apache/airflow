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

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.auth import (
    FabAuthenticatedMeResponse,
    MenuItemCollectionResponse,
    SimpleAuthenticatedMeResponse,
)
from airflow.api_fastapi.core_api.security import GetUserDep

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
def get_current_user(
    user: GetUserDep,
) -> SimpleAuthenticatedMeResponse | FabAuthenticatedMeResponse:
    """Get current authenticated user information."""
    auth_manager = get_auth_manager()
    if isinstance(auth_manager, SimpleAuthManager):
        return SimpleAuthenticatedMeResponse(
            username=user.username,
            role=user.role,
        )

    if auth_manager.get_auth_manager_type() == "FabAuthManager":
        return FabAuthenticatedMeResponse(
            id=user.id,
            first_name=user.first_name,
            last_name=user.last_name,
            username=user.username,
            email=user.email,
            roles=[role.name for role in user.roles] if user.roles else None,
        )
    raise NotImplementedError("Unsupported auth manager type")
