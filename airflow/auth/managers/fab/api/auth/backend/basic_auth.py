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
"""Basic authentication backend."""
from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

from flask import Response, request
from flask_appbuilder.const import AUTH_LDAP
from flask_login import login_user

from airflow.utils.airflow_flask_app import get_airflow_app

if TYPE_CHECKING:
    from airflow.auth.managers.fab.models import User

CLIENT_AUTH: tuple[str, str] | Any | None = None

T = TypeVar("T", bound=Callable)


def init_app(_):
    """Initialize authentication backend."""


def auth_current_user() -> User | None:
    """Authenticate and set current user if Authorization header exists."""
    auth = request.authorization
    if auth is None or not auth.username or not auth.password:
        return None

    ab_security_manager = get_airflow_app().appbuilder.sm
    user = None
    if ab_security_manager.auth_type == AUTH_LDAP:
        user = ab_security_manager.auth_user_ldap(auth.username, auth.password)
    if user is None:
        user = ab_security_manager.auth_user_db(auth.username, auth.password)
    if user is not None:
        login_user(user, remember=False)
    return user


def requires_authentication(function: T):
    """Decorate functions that require authentication."""

    @wraps(function)
    def decorated(*args, **kwargs):
        if auth_current_user() is not None:
            return function(*args, **kwargs)
        else:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})

    return cast(T, decorated)
