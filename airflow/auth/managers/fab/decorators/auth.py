#
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
from functools import wraps
from typing import Callable, Sequence, TypeVar, cast

from flask import current_app, render_template, request

from airflow.api_connexion.exceptions import PermissionDenied
from airflow.api_connexion.security import check_authentication
from airflow.configuration import conf
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.utils.net import get_hostname
from airflow.www.auth import _has_access
from airflow.www.extensions.init_auth_manager import get_auth_manager

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)


def _requires_access_fab(permissions: Sequence[tuple[str, str]] | None = None) -> Callable[[T], T]:
    """
    Check current user's permissions against required permissions.

    This decorator is only kept for backward compatible reasons. The decorator
    ``airflow.api_connexion.security.requires_access``, which redirects to this decorator, might be used in
    user plugins. Thus, we need to keep it.

    :meta private:
    """
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


def _has_access_fab(permissions: Sequence[tuple[str, str]] | None = None) -> Callable[[T], T]:
    """
    Factory for decorator that checks current user's permissions against required permissions.

    This decorator is only kept for backward compatible reasons. The decorator
    ``airflow.www.auth.has_access``, which redirects to this decorator, is widely used in user plugins.
    Thus, we need to keep it.
    See https://github.com/apache/airflow/pull/33213#discussion_r1346287224

    :meta private:
    """

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            __tracebackhide__ = True  # Hide from pytest traceback.

            appbuilder = current_app.appbuilder

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

            return _has_access(
                is_authorized=appbuilder.sm.check_authorization(permissions, dag_id),
                func=func,
                args=args,
                kwargs=kwargs,
            )

        return cast(T, decorated)

    return requires_access_decorator
