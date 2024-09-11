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

import warnings
from typing import TYPE_CHECKING

from airflow.api_connexion.exceptions import BadRequest
from airflow.providers.fab.auth_manager.api_endpoints import role_and_permission_endpoint, user_endpoint
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from typing import Callable

    from airflow.api_connexion.types import APIResponse


def _require_fab(func: Callable) -> Callable:
    """
    Raise an HTTP error 400 if the auth manager is not FAB.

    Intended to decorate endpoints that have been migrated from Airflow API to FAB API.
    """

    def inner(*args, **kwargs):
        from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

        auth_mgr = get_auth_manager()
        if not isinstance(auth_mgr, FabAuthManager):
            raise BadRequest(
                detail="This endpoint is only available when using the default auth manager FabAuthManager."
            )
        else:
            warnings.warn(
                "This API endpoint is deprecated. "
                "Please use the API under /auth/fab/v1 instead for this operation.",
                DeprecationWarning,
                stacklevel=1,  # This decorator wrapped multiple times, better point to this file
            )
            return func(*args, **kwargs)

    return inner


### role


@_require_fab
def get_role(**kwargs) -> APIResponse:
    """Get role."""
    return role_and_permission_endpoint.get_role(**kwargs)


@_require_fab
def get_roles(**kwargs) -> APIResponse:
    """Get roles."""
    return role_and_permission_endpoint.get_roles(**kwargs)


@_require_fab
def delete_role(**kwargs) -> APIResponse:
    """Delete a role."""
    return role_and_permission_endpoint.delete_role(**kwargs)


@_require_fab
def patch_role(**kwargs) -> APIResponse:
    """Update a role."""
    kwargs.pop("body", None)
    return role_and_permission_endpoint.patch_role(**kwargs)


@_require_fab
def post_role(**kwargs) -> APIResponse:
    """Create a new role."""
    kwargs.pop("body", None)
    return role_and_permission_endpoint.post_role(**kwargs)


### permissions
@_require_fab
def get_permissions(**kwargs) -> APIResponse:
    """Get permissions."""
    return role_and_permission_endpoint.get_permissions(**kwargs)


### user
@_require_fab
def get_user(**kwargs) -> APIResponse:
    """Get a user."""
    return user_endpoint.get_user(**kwargs)


@_require_fab
def get_users(**kwargs) -> APIResponse:
    """Get users."""
    return user_endpoint.get_users(**kwargs)


@_require_fab
def post_user(**kwargs) -> APIResponse:
    """Create a new user."""
    kwargs.pop("body", None)
    return user_endpoint.post_user(**kwargs)


@_require_fab
def patch_user(**kwargs) -> APIResponse:
    """Update a user."""
    kwargs.pop("body", None)
    return user_endpoint.patch_user(**kwargs)


@_require_fab
def delete_user(**kwargs) -> APIResponse:
    """Delete a user."""
    return user_endpoint.delete_user(**kwargs)
