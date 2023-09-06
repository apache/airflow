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

from flask import flash, g, redirect, render_template, request

from airflow.auth.managers.models.resource_details import (
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
)
from airflow.configuration import conf
from airflow.utils.net import get_hostname
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from airflow.auth.managers.models.resource_method import ResourceMethod
    from airflow.models import Connection

T = TypeVar("T", bound=Callable)


def get_access_denied_message():
    return conf.get("webserver", "access_denied_message")


def _has_access_simple(is_authorized_callback: Callable[[], bool]) -> Callable[[T], T]:
    """
    Generic Decorator that checks current user's permissions against required permissions.

    This works only for resources with no details. This function is used in some ``has_access_`` functions
    below.

    :param is_authorized_callback: callback to execute to figure whether the user authorized to access
        the resource?
    """

    def has_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            return _has_access(
                is_authorized=is_authorized_callback(),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return has_access_decorator


def _has_access(*, is_authorized: bool, func: Callable, args, kwargs):
    """
    Define the behavior whether the user is authorized to access the resource.

    :param is_authorized: whether the user is authorized to access the resource
    :param func: the function to call if the user is authorized
    :param args: the arguments of ``func``
    :param kwargs: the keyword arguments ``func``

    :meta private:
    """
    if is_authorized:
        return func(*args, **kwargs)
    elif get_auth_manager().is_logged_in() and not g.user.perms:
        return (
            render_template(
                "airflow/no_roles_permissions.html",
                hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "redact",
                logout_url=get_auth_manager().get_url_logout(),
            ),
            403,
        )
    else:
        access_denied = get_access_denied_message()
        flash(access_denied, "danger")
    return redirect(get_auth_manager().get_url_login(next=request.url))


def has_access_cluster_activity(method: ResourceMethod) -> Callable[[T], T]:
    return _has_access_simple(lambda: get_auth_manager().is_authorized_cluster_activity(method=method))


def has_access_configuration(method: ResourceMethod) -> Callable[[T], T]:
    return _has_access_simple(lambda: get_auth_manager().is_authorized_configuration(method=method))


def has_access_connection(method: ResourceMethod) -> Callable[[T], T]:
    def has_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            connections: set[Connection] = set(args[1])
            connections_details = [
                ConnectionDetails(conn_id=connection.conn_id) for connection in connections
            ]
            is_authorized = all(
                [
                    get_auth_manager().is_authorized_connection(
                        method=method, connection_details=connection_details
                    )
                    for connection_details in connections_details
                ]
            )
            return _has_access(
                is_authorized=is_authorized,
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return has_access_decorator


def has_access_dag(method: ResourceMethod, access_entity: DagAccessEntity | None = None) -> Callable[[T], T]:
    def has_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            dag_id = (
                kwargs.get("dag_id")
                or request.args.get("dag_id")
                or request.form.get("dag_id")
                or (request.is_json and request.json.get("dag_id"))
                or None
            )
            is_authorized = get_auth_manager().is_authorized_dag(
                method=method,
                dag_access_entity=access_entity,
                dag_details=None if not dag_id else DagDetails(id=dag_id),
            )

            return _has_access(
                is_authorized=is_authorized,
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return has_access_decorator


def has_access_dataset(method: ResourceMethod) -> Callable[[T], T]:
    """Decorator that checks current user's permissions against required permissions for datasets."""
    return _has_access_simple(lambda: get_auth_manager().is_authorized_dataset(method=method))


def has_access_variable(method: ResourceMethod) -> Callable[[T], T]:
    """Decorator that checks current user's permissions against required permissions for variables."""
    return _has_access_simple(lambda: get_auth_manager().is_authorized_variable(method=method))


def has_access_website() -> Callable[[T], T]:
    """Decorator that checks current user's permissions to access the website."""
    return _has_access_simple(lambda: get_auth_manager().is_authorized_website())
