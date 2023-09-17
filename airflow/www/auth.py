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

from flask import current_app, flash, g, redirect, render_template, request

from airflow.configuration import conf
from airflow.utils.net import get_hostname
from airflow.www.extensions.init_auth_manager import get_auth_manager

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)


def get_access_denied_message():
    return conf.get("webserver", "access_denied_message")


def has_access(permissions: Sequence[tuple[str, str]] | None = None) -> Callable[[T], T]:
    """Factory for decorator that checks current user's permissions against required permissions."""

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

            if appbuilder.sm.check_authorization(permissions, dag_id):
                return func(*args, **kwargs)
            elif get_auth_manager().is_logged_in() and not g.user.perms:
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
            else:
                access_denied = get_access_denied_message()
                flash(access_denied, "danger")
            return redirect(get_auth_manager().get_url_login(next=request.url))

        return cast(T, decorated)

    return requires_access_decorator
