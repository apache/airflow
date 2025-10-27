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

from typing import TYPE_CHECKING

from fastapi import Depends, status

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleBody, RoleResponse
from airflow.providers.fab.auth_manager.api_fastapi.security import requires_fab_custom_view
from airflow.providers.fab.auth_manager.api_fastapi.services.roles import FABAuthManagerRoles
from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
from airflow.providers.fab.www.security import permissions

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleBody, RoleResponse


roles_router = AirflowRouter(prefix="/fab/v1", tags=["FabAuthManager"])


@roles_router.post(
    "/roles",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_409_CONFLICT,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("POST", permissions.RESOURCE_ROLE))],
)
def create_role(body: RoleBody) -> RoleResponse:
    """Create a new role (actions can be empty)."""
    with get_application_builder():
        return FABAuthManagerRoles.create_role(body=body)
