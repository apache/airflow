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
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from flask import Response, g

from airflow.api_connexion.exceptions import PermissionDenied, Unauthenticated
from airflow.auth.managers.models.resource_details import (
    AccessView,
    AssetDetails,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
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


def _requires_access(
    *, is_authorized_callback: Callable[[], bool], func: Callable, args, kwargs
) -> bool:
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
        def callback() -> bool | DagAccessEntity:
            if dag_id:
                # a DAG id is provided; is the user authorized to access this DAG?
                return get_auth_manager().is_authorized_dag(
                    method=method,
                    access_entity=access_entity,
                    details=DagDetails(id=dag_id),
                )
            else:
                # here we know dag_id is not provided.
                # check is the user authorized to access all DAGs?
                if get_auth_manager().is_authorized_dag(
                    method=method,
                    access_entity=access_entity,
                ):
                    return True
                elif access_entity:
                    # no dag_id provided, and user does not have access to all dags
                    return False

            # dag_id is not provided, and the user is not authorized to access *all* DAGs
            # so we check that the user can access at least *one* dag
            # but we leave it to the endpoint function to properly restrict access beyond that
            if method not in ("GET", "PUT"):
                return False
            return any(get_auth_manager().get_permitted_dag_ids(methods=[method]))

        return callback

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            dag_id: str | None = (
                kwargs.get("dag_id") if kwargs.get("dag_id") != "~" else None
            )
            return _requires_access(
                is_authorized_callback=_is_authorized_callback(dag_id),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_asset(method: ResourceMethod) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            uri: str | None = kwargs.get("uri")
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_asset(
                    method=method, details=AssetDetails(uri=uri)
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
                is_authorized_callback=lambda: get_auth_manager().is_authorized_view(
                    access_view=access_view
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def requires_access_custom_view(
    method: ResourceMethod,
    resource_name: str,
) -> Callable[[T], T]:
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            return _requires_access(
                is_authorized_callback=lambda: get_auth_manager().is_authorized_custom_view(
                    method=method, resource_name=resource_name
                ),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator


def get_readable_dags() -> set[str]:
    return get_auth_manager().get_permitted_dag_ids(user=g.user)
