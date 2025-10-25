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

from fastapi import Depends
from starlette import status
from starlette.exceptions import HTTPException

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import get_user
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleBody, RoleResponse
from airflow.providers.fab.auth_manager.api_fastapi.services.roles import FABAuthManagerRoles
from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
from airflow.providers.fab.www.security import permissions

roles_router = AirflowRouter(prefix="/fab/v1", tags=["FabAuthManager"])


def requires_fab_custom_view(method: str, resource_name: str):
    def _check(user=Depends(get_user)):
        if not get_auth_manager().is_authorized_custom_view(
            method=method, resource_name=resource_name, user=user
        ):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    return _check


@roles_router.post(
    "/roles",
    response_model=RoleResponse,
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_409_CONFLICT,
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("POST", permissions.RESOURCE_ROLE))],
)
def create_role(body: RoleBody) -> RoleResponse:
    """Create a new role (actions can be empty)."""
    with get_application_builder():
        return FABAuthManagerRoles.create_role(body=body)
