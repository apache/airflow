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

from typing import cast

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.auth import MenuItem, MenuItemCollectionResponse
from airflow.api_fastapi.core_api.security import GetUserDep

auth_router = AirflowRouter(tags=["Auth Links"])


@auth_router.get("/auth/links")
def get_auth_links(
    user: GetUserDep,
) -> MenuItemCollectionResponse:
    menu_items = get_auth_manager().get_menu_items(user=user)

    return MenuItemCollectionResponse(
        menu_items=cast(list[MenuItem], menu_items),
        total_entries=len(menu_items),
    )
