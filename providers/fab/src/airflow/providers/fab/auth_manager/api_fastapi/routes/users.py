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
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.users import (
    UserBody,
    UserCollectionResponse,
    UserPatchBody,
    UserResponse,
)
from airflow.providers.fab.auth_manager.api_fastapi.parameters import get_effective_limit
from airflow.providers.fab.auth_manager.api_fastapi.routes.router import fab_router
from airflow.providers.fab.auth_manager.api_fastapi.security import requires_fab_custom_view
from airflow.providers.fab.auth_manager.api_fastapi.services.users import FABAuthManagerUsers
from airflow.providers.fab.auth_manager.cli_commands.utils import get_application_builder
from airflow.providers.fab.www.security import permissions


@fab_router.post(
    "/users",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_409_CONFLICT,
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("POST", permissions.RESOURCE_USER))],
)
def create_user(body: UserBody) -> UserResponse:
    with get_application_builder():
        return FABAuthManagerUsers.create_user(body=body)


@fab_router.get(
    "/users",
    response_model=UserCollectionResponse,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("GET", permissions.RESOURCE_USER))],
)
def get_users(
    order_by: str = Query("id", description="Field to order by. Prefix with '-' for descending."),
    limit: int = Depends(get_effective_limit()),
    offset: int = Query(0, ge=0, description="Number of items to skip before starting to collect results."),
) -> UserCollectionResponse:
    """List users with pagination and ordering."""
    with get_application_builder():
        return FABAuthManagerUsers.get_users(order_by=order_by, limit=limit, offset=offset)


@fab_router.get(
    "/users/{username}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("GET", permissions.RESOURCE_USER))],
)
def get_user(username: str = Path(..., min_length=1)) -> UserResponse:
    """Get a user by username."""
    with get_application_builder():
        return FABAuthManagerUsers.get_user(username=username)


@fab_router.patch(
    "/users/{username}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("PUT", permissions.RESOURCE_USER))],
)
def update_user(
    body: UserPatchBody,
    username: str = Path(..., min_length=1),
    update_mask: str | None = Query(None, description="Comma-separated list of fields to update"),
) -> UserResponse:
    """Update an existing user."""
    with get_application_builder():
        return FABAuthManagerUsers.update_user(username=username, body=body, update_mask=update_mask)


@fab_router.delete(
    "/users/{username}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_fab_custom_view("DELETE", permissions.RESOURCE_USER))],
)
def delete_user(username: str = Path(..., min_length=1)):
    """Delete a user by username."""
    with get_application_builder():
        FABAuthManagerUsers.delete_user(username=username)
