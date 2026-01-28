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
"""Security dependencies for FastAPI endpoints."""
from __future__ import annotations

from typing import Annotated, Callable, TypeVar

from fastapi import Depends, Request
from flask import current_app

from airflow.providers.fab.www.api_fastapi.exceptions import Unauthenticated
from airflow.www.extensions.init_appbuilder import get_appbuilder

T = TypeVar("T")


def check_authentication() -> Callable[[Request], None]:
    """
    Check authentication for FastAPI endpoint.

    Returns a dependency function that validates authentication.
    """

    def _check_authentication(request: Request) -> None:
        """Verify user is authenticated."""
        for view_class in current_app.appbuilder.baseviews:
            if request.url.path.startswith(view_class.route_base):
                method_name = request.method.lower()
                if method_name == "head":
                    method_name = "get"

                if method_name in view_class.class_permission_name:
                    permission_str = view_class.class_permission_name[method_name]
                else:
                    permission_str = view_class.class_permission_name.get("get")

                if current_app.appbuilder.sm.check_authorization(
                    perms=[(permission_str, view_class.class_permission_name)],
                    dag_id=None,
                ):
                    return

        raise Unauthenticated(headers={"WWW-Authenticate": 'Basic realm="Authentication required"'})

    return _check_authentication


def requires_access_custom_view(
    permissions: list[tuple[str, str]],
) -> Callable[[Request], None]:
    """
    Check user has required permissions for custom view.

    :param permissions: List of (permission_name, view_name) tuples
    """

    def _check_access(request: Request) -> None:
        """Verify user has the required permissions."""
        appbuilder = get_appbuilder()
        if not appbuilder.sm.check_authorization(perms=permissions, dag_id=None):
            raise Unauthenticated(headers={"WWW-Authenticate": 'Basic realm="Authentication required"'})

    return _check_access


# Common security dependency type
AuthenticatedRequest = Annotated[Request, Depends(check_authentication())]
