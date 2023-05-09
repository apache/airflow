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
from typing import Callable, Sequence, TypeVar, cast

from flask import current_app, flash, g, redirect, render_template, request, url_for

from airflow.configuration import conf
from airflow.utils.net import get_hostname

T = TypeVar("T", bound=Callable)


def has_access(permissions: Sequence[tuple[str, str]] | None = None) -> Callable[[T], T]:
    """Factory for decorator that checks current user's permissions against required permissions."""

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            __tracebackhide__ = True  # Hide from pytest traceback.

            appbuilder = current_app.appbuilder

            dag_id = (
                request.args.get("dag_id")
                or request.form.get("dag_id")
                or (request.is_json and request.json.get("dag_id"))
                or None
            )
            if appbuilder.sm.check_authorization(permissions, dag_id):
                return func(*args, **kwargs)
            elif not g.user.is_anonymous and not g.user.perms:
                return (
                    render_template(
                        "airflow/no_roles_permissions.html",
                        hostname=get_hostname()
                        if conf.getboolean("webserver", "EXPOSE_HOSTNAME")
                        else "redact",
                        logout_url=appbuilder.get_url_for_logout,
                    ),
                    403,
                )
            else:
                access_denied = "Access is Denied"
                flash(access_denied, "danger")
            return redirect(
                url_for(
                    appbuilder.sm.auth_view.__class__.__name__ + ".login",
                    next=request.url,
                )
            )

        return cast(T, decorated)

    return requires_access_decorator
