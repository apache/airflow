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

import functools
import logging
from collections.abc import Callable, Sequence
from functools import wraps
from typing import TYPE_CHECKING, TypeVar, cast

from flask import flash, redirect, render_template, request, url_for
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.const import (
    FLAMSG_ERR_SEC_ACCESS_DENIED,
    LOGMSG_ERR_SEC_ACCESS_DENIED,
    PERMISSION_PREFIX,
)

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.providers.common.compat.sdk import conf
from airflow.providers.fab.www.utils import get_fab_auth_manager
from airflow.utils.net import get_hostname

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.api_fastapi.auth.managers.models.batch_apis import (
        IsAuthorizedDagRequest,
    )
    from airflow.models import DagRun, Pool, TaskInstance, Variable
    from airflow.models.connection import Connection
    from airflow.models.xcom import XComModel

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)


def get_access_denied_message():
    return conf.get("fab", "access_denied_message")


def has_access_with_pk(f):
    """
    Check permissions on views.

    The implementation is very similar from
    https://github.com/dpgaspar/Flask-AppBuilder/blob/c6fecdc551629e15467fde5d06b4437379d90592/flask_appbuilder/security/decorators.py#L134

    The difference is that this decorator will pass the resource ID to check permissions. It allows
    fined-grained access control using resource IDs.
    """
    if hasattr(f, "_permission_name"):
        permission_str = f._permission_name
    else:
        permission_str = f.__name__

    def wraps(self, *args, **kwargs):
        permission_str = f"{PERMISSION_PREFIX}{f._permission_name}"
        if self.method_permission_name:
            _permission_name = self.method_permission_name.get(f.__name__)
            if _permission_name:
                permission_str = f"{PERMISSION_PREFIX}{_permission_name}"
        if permission_str in self.base_permissions and self.appbuilder.sm.has_access(
            action_name=permission_str,
            resource_name=self.class_permission_name,
            resource_pk=kwargs.get("pk"),
        ):
            return f(self, *args, **kwargs)
        log.warning(LOGMSG_ERR_SEC_ACCESS_DENIED, permission_str, self.__class__.__name__)
        flash(as_unicode(FLAMSG_ERR_SEC_ACCESS_DENIED), "danger")
        return redirect(get_auth_manager().get_url_login(next_url=request.url))

    f._permission_name = permission_str
    return functools.update_wrapper(wraps, f)


def _has_access_no_details(is_authorized_callback: Callable[[], bool]) -> Callable[[T], T]:
    """
    Check current user's permissions against required permissions.

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

        return cast("T", decorated)

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
    if get_fab_auth_manager().is_logged_in() and not get_auth_manager().is_authorized_view(
        access_view=AccessView.WEBSITE,
        user=get_fab_auth_manager().get_user(),
    ):
        return (
            render_template(
                "airflow/no_roles_permissions.html",
                hostname=get_hostname() if conf.getboolean("fab", "EXPOSE_HOSTNAME") else "",
                logout_url=get_fab_auth_manager().get_url_logout(),
            ),
            403,
        )
    if not get_fab_auth_manager().is_logged_in():
        return redirect(get_auth_manager().get_url_login(next_url=request.url))
    access_denied = get_access_denied_message()
    flash(access_denied, "danger")
    return redirect(url_for("FabIndexView.index"))


def has_access_configuration(method: ResourceMethod) -> Callable[[T], T]:
    return _has_access_no_details(
        lambda: get_auth_manager().is_authorized_configuration(
            method=method, user=get_fab_auth_manager().get_user()
        )
    )


def has_access_connection(method: ResourceMethod) -> Callable[[T], T]:
    def has_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            connections: set[Connection] = set(args[1])
            is_authorized = all(
                get_auth_manager().is_authorized_connection(
                    method=method,
                    details=ConnectionDetails(conn_id=connection.conn_id),
                    user=get_auth_manager().get_user(),
                )
                for connection in connections
            )
            return _has_access(
                is_authorized=is_authorized,
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast("T", decorated)

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
                    "There are different dag_ids passed in the request: %s. Returning 403.", unique_dag_ids
                )
                log.warning(
                    "kwargs: %s, args: %s, form: %s, json: %s",
                    dag_id_kwargs,
                    dag_id_args,
                    dag_id_form,
                    dag_id_json,
                )
                return (
                    render_template(
                        "airflow/no_roles_permissions.html",
                        hostname=get_hostname() if conf.getboolean("fab", "EXPOSE_HOSTNAME") else "",
                        logout_url=get_auth_manager().get_url_logout(),
                    ),
                    403,
                )
            dag_id = unique_dag_ids.pop() if unique_dag_ids else None

            is_authorized = get_auth_manager().is_authorized_dag(
                method=method,
                access_entity=access_entity,
                details=None if not dag_id else DagDetails(id=dag_id),
                user=get_auth_manager().get_user(),
            )

            return _has_access(
                is_authorized=is_authorized,
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast("T", decorated)

    return has_access_decorator


def has_access_dag_entities(method: ResourceMethod, access_entity: DagAccessEntity) -> Callable[[T], T]:
    def has_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            items: set[XComModel | DagRun | TaskInstance] = set(args[1])
            requests: Sequence[IsAuthorizedDagRequest] = [
                {
                    "method": method,
                    "access_entity": access_entity,
                    "details": DagDetails(id=item.dag_id),
                }
                for item in items
                if item is not None
            ]
            is_authorized = get_auth_manager().batch_is_authorized_dag(
                requests, user=get_auth_manager().get_user()
            )
            return _has_access(
                is_authorized=is_authorized,
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast("T", decorated)

    return has_access_decorator


def has_access_asset(method: ResourceMethod) -> Callable[[T], T]:
    """Check current user's permissions against required permissions for assets."""
    return _has_access_no_details(
        lambda: get_auth_manager().is_authorized_asset(method=method, user=get_fab_auth_manager().get_user())
    )


def has_access_pool(method: ResourceMethod) -> Callable[[T], T]:
    def has_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            pools: set[Pool] = set(args[1])
            is_authorized = all(
                get_auth_manager().is_authorized_pool(
                    method=method, details=PoolDetails(name=pool.pool), user=get_auth_manager().get_user()
                )
                for pool in pools
            )
            return _has_access(
                is_authorized=is_authorized,
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast("T", decorated)

    return has_access_decorator


def has_access_variable(method: ResourceMethod) -> Callable[[T], T]:
    def has_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            variables: set[Variable] = set(args[1])
            is_authorized = all(
                get_auth_manager().is_authorized_variable(
                    method=method,
                    details=VariableDetails(key=variable.key),
                    user=get_auth_manager().get_user(),
                )
                for variable in variables
            )
            return _has_access(
                is_authorized=is_authorized,
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast("T", decorated)

    return has_access_decorator


def has_access_view(access_view: AccessView = AccessView.WEBSITE) -> Callable[[T], T]:
    """Check current user's permissions to access the website."""
    return _has_access_no_details(
        lambda: get_auth_manager().is_authorized_view(
            access_view=access_view, user=get_fab_auth_manager().get_user()
        )
    )
