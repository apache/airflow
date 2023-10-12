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

import logging
import warnings
from functools import wraps
from typing import TYPE_CHECKING, Callable, Sequence, TypeVar, cast

from flask import flash, g, redirect, render_template, request

from airflow.auth.managers.models.resource_details import (
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
)
from airflow.configuration import conf
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils.net import get_hostname
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod
    from airflow.models.connection import Connection

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)


def get_access_denied_message():
    return conf.get("webserver", "access_denied_message")


def has_access(permissions: Sequence[tuple[str, str]] | None = None) -> Callable[[T], T]:
    """
    Factory for decorator that checks current user's permissions against required permissions.

    Deprecated. Do not use this decorator, use one of the decorator `has_access_*` defined in
    airflow/www/auth.py instead.
    This decorator will only work with FAB authentication and not with other auth providers.

    This decorator is widely used in user plugins, do not remove it. See
    https://github.com/apache/airflow/pull/33213#discussion_r1346287224
    """
    warnings.warn(
        "The 'has_access' decorator is deprecated. Please use one of the decorator `has_access_*`"
        "defined in airflow/www/auth.py instead.",
        RemovedInAirflow3Warning,
        stacklevel=2,
    )
    from airflow.auth.managers.fab.decorators.auth import _has_access_fab

    return _has_access_fab(permissions)


def _has_access_no_details(is_authorized_callback: Callable[[], bool]) -> Callable[[T], T]:
    """
    Generic Decorator that checks current user's permissions against required permissions.

    This works only for resources with no details. This function is used in some ``has_access_`` functions
    below.

    :param is_authorized_callback: callback to execute to figure whether the user is authorized to access
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
    return _has_access_no_details(lambda: get_auth_manager().is_authorized_cluster_activity(method=method))


def has_access_configuration(method: ResourceMethod) -> Callable[[T], T]:
    return _has_access_no_details(lambda: get_auth_manager().is_authorized_configuration(method=method))


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
                    get_auth_manager().is_authorized_connection(method=method, details=connection_details)
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
            dag_id_kwargs = kwargs.get("dag_id")
            dag_id_args = request.args.get("dag_id")
            dag_id_form = request.form.get("dag_id")
            dag_id_json = request.json.get("dag_id") if request.is_json else None
            all_dag_ids = [dag_id_kwargs, dag_id_args, dag_id_form, dag_id_json]
            unique_dag_ids = set(dag_id for dag_id in all_dag_ids if dag_id is not None)

            if len(unique_dag_ids) > 1:
                log.warning(
                    f"There are different dag_ids passed in the request: {unique_dag_ids}. Returning 403."
                )
                log.warning(
                    f"kwargs: {dag_id_kwargs}, args: {dag_id_args}, "
                    f"form: {dag_id_form}, json: {dag_id_json}"
                )
                return (
                    render_template(
                        "airflow/no_roles_permissions.html",
                        hostname=get_hostname()
                        if conf.getboolean("webserver", "EXPOSE_HOSTNAME")
                        else "redact",
                        logout_url=get_auth_manager().get_url_logout(),
                    ),
                    403,
                )
            dag_id = unique_dag_ids.pop() if unique_dag_ids else None

            is_authorized = get_auth_manager().is_authorized_dag(
                method=method,
                access_entity=access_entity,
                details=None if not dag_id else DagDetails(id=dag_id),
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
    return _has_access_no_details(lambda: get_auth_manager().is_authorized_dataset(method=method))


def has_access_variable(method: ResourceMethod) -> Callable[[T], T]:
    """Decorator that checks current user's permissions against required permissions for variables."""
    return _has_access_no_details(lambda: get_auth_manager().is_authorized_variable(method=method))


def has_access_website() -> Callable[[T], T]:
    """Decorator that checks current user's permissions to access the website."""
    return _has_access_no_details(lambda: get_auth_manager().is_authorized_website())
