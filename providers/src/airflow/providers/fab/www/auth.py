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
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from flask import flash, redirect, render_template, request, url_for

from airflow.api_fastapi.app import get_auth_manager
from airflow.auth.managers.models.resource_details import (
    AccessView,
    DagAccessEntity,
    DagDetails,
)
from airflow.configuration import conf
from airflow.utils.net import get_hostname

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

T = TypeVar("T", bound=Callable)

log = logging.getLogger(__name__)


def get_access_denied_message():
    return conf.get("webserver", "access_denied_message")


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
    elif get_auth_manager().is_logged_in() and not get_auth_manager().is_authorized_view(
        access_view=AccessView.WEBSITE
    ):
        return (
            render_template(
                "airflow/no_roles_permissions.html",
                hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "",
                logout_url=get_auth_manager().get_url_logout(),
            ),
            403,
        )
    elif not get_auth_manager().is_logged_in():
        return redirect(get_auth_manager().get_url_login(next_url=request.url))
    else:
        access_denied = get_access_denied_message()
        flash(access_denied, "danger")
    return redirect(url_for("Airflow.index"))


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
                        hostname=get_hostname() if conf.getboolean("webserver", "EXPOSE_HOSTNAME") else "",
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
