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
from typing import TYPE_CHECKING, Callable, TypeVar, cast

from flask import Response, request, session

from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.utils.airflow_flask_app import get_airflow_app

if TYPE_CHECKING:
    from requests.auth import AuthBase

log = logging.getLogger(__name__)

CLIENT_AUTH: tuple[str, str] | AuthBase | None = None


def init_app(_): ...


T = TypeVar("T", bound=Callable)


def _lookup_user(username: str):
    users = get_airflow_app().config.get("SIMPLE_AUTH_MANAGER_USERS", [])
    return next((user for user in users if user["username"] == username), None)


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        user_id = request.remote_user
        if not user_id:
            log.debug("Missing REMOTE_USER.")
            return Response("Forbidden", 403)

        log.debug("Looking for user: %s", user_id)

        user_dict = _lookup_user(user_id)
        if not user_dict:
            return Response("Forbidden", 403)

        log.debug("Found user: %s", user_dict)
        session["user"] = SimpleAuthManagerUser(username=user_dict["username"], role=user_dict["role"])

        return function(*args, **kwargs)

    return cast(T, decorated)
