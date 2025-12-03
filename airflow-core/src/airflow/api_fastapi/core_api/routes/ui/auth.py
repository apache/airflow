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
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.auth import (
    AuthenticatedMeResponse,
    MenuItemCollectionResponse,
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
def get_current_user_info(
    user: GetUserDep,
) -> AuthenticatedMeResponse:
    """Convienently get the current authenticated user information."""
    return AuthenticatedMeResponse(
        id=user.get_id(),
        username=user.get_name(),
    )
