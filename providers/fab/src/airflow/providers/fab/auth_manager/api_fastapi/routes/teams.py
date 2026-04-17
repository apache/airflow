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

from fastapi import Depends, Path, Query, status

from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import RoleCollectionResponse
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.teams import (
    TeamBody,
    TeamCollectionResponse,
    TeamResponse,
)
from airflow.providers.fab.auth_manager.api_fastapi.parameters import get_effective_limit
from airflow.providers.fab.auth_manager.api_fastapi.routes.router import fab_router
from airflow.providers.fab.auth_manager.api_fastapi.security import requires_fab_custom_view
from airflow.providers.fab.auth_manager.api_fastapi.services.teams import FABAuthManagerTeams
from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
from airflow.providers.fab.www.security import permissions


@fab_router.get(
    "/teams",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("GET", permissions.RESOURCE_TEAM))],
)
def get_teams(
    order_by: str = Query("id", description="Field to order by. Prefix with '-' for descending."),
    limit: int = Depends(get_effective_limit()),
    offset: int = Query(0, ge=0, description="Number of items to skip before starting to collect results."),
) -> TeamCollectionResponse:
    """Get existing teams."""
    with get_application_builder():
        return FABAuthManagerTeams.get_teams(order_by=order_by, limit=limit, offset=offset)


@fab_router.post(
    "/teams",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_409_CONFLICT,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("POST", permissions.RESOURCE_TEAM))],
)
def create_team(body: TeamBody) -> TeamResponse:
    """Create a new team."""
    with get_application_builder():
        return FABAuthManagerTeams.create_team(body=body)


@fab_router.get(
    "/teams/{team}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("GET", permissions.RESOURCE_TEAM))],
)
def get_team(team: str = Path(..., min_length=1)) -> TeamResponse:
    """Get a team by name."""
    with get_application_builder():
        return FABAuthManagerTeams.get_team(name=team)


@fab_router.get(
    "/teams/{team}/roles",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_fab_custom_view("GET", permissions.RESOURCE_TEAM)),
        Depends(requires_fab_custom_view("GET", permissions.RESOURCE_ROLE)),
    ],
)
def get_team_roles(
    team: str = Path(..., min_length=1),
    order_by: str = Query("id", description="Field to order by. Prefix with '-' for descending."),
    limit: int = Depends(get_effective_limit()),
    offset: int = Query(0, ge=0, description="Number of items to skip before starting to collect results."),
) -> RoleCollectionResponse:
    """Get all roles associated with a team."""
    with get_application_builder():
        return FABAuthManagerTeams.get_team_roles(team=team)


@fab_router.delete(
    "/teams/{team}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("DELETE", permissions.RESOURCE_TEAM))],
)
def delete_user(team: str = Path(..., min_length=1)):
    """Delete a team by name."""
    with get_application_builder():
        FABAuthManagerTeams.delete_team(name=team)
