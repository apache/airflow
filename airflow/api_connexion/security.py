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
from functools import wraps
from typing import TYPE_CHECKING, Callable, Sequence, TypeVar, cast

from flask import Response, g

from airflow.api_connexion.exceptions import PermissionDenied, Unauthenticated
from airflow.auth.managers.models.resource_details import (
    AccessView,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    DatasetDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.exceptions import RemovedInAirflow3Warning
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
    """
    Check current user's permissions against required permissions.

    Deprecated. Do not use this decorator, use one of the decorator `has_access_*` defined in
    airflow/api_connexion/security.py instead.
    This decorator will only work with FAB authentication and not with other auth providers.

    This decorator might be used in user plugins, do not remove it.
    """
    warnings.warn(
        "The 'requires_access' decorator is deprecated. Please use one of the decorator `requires_access_*`"
        "defined in airflow/api_connexion/security.py instead.",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )
    from airflow.auth.managers.fab.decorators.auth import _requires_access_fab

    return _requires_access_fab(permissions)


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
    def _is_authorized_callback(dag_id: str):
        def callback():
            access = get_auth_manager().is_authorized_dag(
                method=method,
                access_entity=access_entity,
                details=DagDetails(id=dag_id),
            )

            # ``access`` means here:
            # - if a DAG id is provided (``dag_id`` not None): is the user authorized to access this DAG
            # - if no DAG id is provided: is the user authorized to access all DAGs
            if dag_id or access:
                return access

            # No DAG id is provided and the user is not authorized to access all DAGs
            # If method is "GET", return whether the user has read access to any DAGs
            # If method is "PUT", return whether the user has edit access to any DAGs
            return (method == "GET" and any(get_auth_manager().get_permitted_dag_ids(methods=["GET"]))) or (
                method == "PUT" and any(get_auth_manager().get_permitted_dag_ids(methods=["PUT"]))
            )

        return callback

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            dag_id: str | None = kwargs.get("dag_id") if kwargs.get("dag_id") != "~" else None
            return _requires_access(
                is_authorized_callback=_is_authorized_callback(dag_id),
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


def requires_access_view(access_view: AccessView) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_view(access_view=access_view),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_custom_view(
    fab_action_name: str,
    fab_resource_name: str,
) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_custom_view(
                    fab_action_name=fab_action_name, fab_resource_name=fab_resource_name
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def get_readable_dags() -> list[str]:
    return get_airflow_app().appbuilder.sm.get_accessible_dag_ids(g.user)
