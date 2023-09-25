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

from functools import wraps
from typing import TYPE_CHECKING, Callable, Sequence, TypeVar, cast

from flask import Response

from airflow.api_connexion.exceptions import PermissionDenied, Unauthenticated
from airflow.auth.managers.models.resource_details import (
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    DatasetDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

T = TypeVar("T", bound=Callable)


def check_authentication() -> None:
    """Check that the request has valid authorization information."""
    for auth in get_airflow_app().api_auth:
        response = auth.requires_authentication(Response)()
        if response.status_code == 200:
            return
    # since this handler only checks authentication, not authorization,
    # we should always return 401
    raise Unauthenticated(headers=response.headers)


def requires_access(permissions: Sequence[tuple[str, str]] | None = None) -> Callable[[T], T]:
    """Check current user's permissions against required permissions."""
    appbuilder = get_airflow_app().appbuilder
    if appbuilder.update_perms:
        appbuilder.sm.sync_resource_permissions(permissions)

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            check_authentication()
            if appbuilder.sm.check_authorization(permissions, kwargs.get("dag_id")):
                return func(*args, **kwargs)
            raise PermissionDenied()

        return cast(T, decorated)

    return requires_access_decorator


def _requires_access(*, is_authorized_callback: Callable[[], bool], func: Callable, args, kwargs):
    """
    Define the behavior whether the user is authorized to access the resource.

    :param is_authorized_callback: callback to execute to figure whether the user is authorized to access
        the resource?
    :param func: the function to call if the user is authorized
    :param args: the arguments of ``func``
    :param kwargs: the keyword arguments ``func``

    :meta private:
    """
    check_authentication()
    if is_authorized_callback():
        return func(*args, **kwargs)
    raise PermissionDenied()


def requires_authentication(func: T):
    """Decorator for functions that require authentication."""

    @wraps(func)
    def decorated(*args, **kwargs):
        check_authentication()
        return func(*args, **kwargs)

    return cast(T, decorated)


def requires_access_configuration(method: ResourceMethod) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            section: str | None = kwargs.get("section")
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_configuration(
                    method=method, details=ConfigurationDetails(section=section)
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_connection(method: ResourceMethod) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            connection_id: str | None = kwargs.get("connection_id")
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_connection(
                    method=method, details=ConnectionDetails(conn_id=connection_id)
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_dag(
    method: ResourceMethod, access_entity: DagAccessEntity | None = None
) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            dag_id: str | None = kwargs.get("dag_id")
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_dag(
                    method=method,
                    access_entity=access_entity,
                    details=DagDetails(id=dag_id),
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_dataset(method: ResourceMethod) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            uri: str | None = kwargs.get("uri")
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_dataset(
                    method=method, details=DatasetDetails(uri=uri)
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_pool(method: ResourceMethod) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            pool_name: str | None = kwargs.get("pool_name")
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_pool(
                    method=method, details=PoolDetails(name=pool_name)
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_variable(method: ResourceMethod) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            variable_key: str | None = kwargs.get("variable_key")
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_variable(
                    method=method, details=VariableDetails(key=variable_key)
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_website() -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_website(),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator
