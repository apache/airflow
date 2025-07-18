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

from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, TypeVar, cast

from flask import Response, current_app

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.fab.www.api_connexion.exceptions import PermissionDenied, Unauthenticated

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.providers.fab.www.airflow_flask_app import AirflowApp

T = TypeVar("T", bound=Callable)


def check_authentication() -> None:
    """Check that the request has valid authorization information."""
    for auth in cast("AirflowApp", current_app).api_auth:
        response = auth.requires_authentication(Response)()
        if response.status_code == 200:
            return

    # since this handler only checks authentication, not authorization,
    # we should always return 401
    raise Unauthenticated(headers=response.headers)


def _requires_access(*, is_authorized_callback: Callable[[], bool], func: Callable, args, kwargs) -> bool:
    """
    Define the behavior whether the user is authorized to access the resource.

    :param is_authorized_callback: callback to execute to figure whether the user is authorized to access
        the resource
    :param func: the function to call if the user is authorized
    :param args: the arguments of ``func``
    :param kwargs: the keyword arguments ``func``

    :meta private:
    """
    check_authentication()
    if is_authorized_callback():
        return func(*args, **kwargs)
    raise PermissionDenied()


def requires_access_custom_view(
    method: ResourceMethod,
    resource_name: str,
) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_custom_view(
                    method=method,
                    resource_name=resource_name,
                    user=get_auth_manager().get_user(),
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast("T", decorated)

    return requires_access_decorator
