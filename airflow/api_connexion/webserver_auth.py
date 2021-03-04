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

from functools import wraps
from typing import Callable, TypeVar, cast

from flask import Response, current_app, request
from flask_appbuilder.const import AUTH_LDAP
from flask_jwt_extended import verify_jwt_in_request
from flask_login import login_user

from airflow.configuration import conf

SECRET = conf.get("webserver", "secret_key")
T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def auth_current_user():
    """Checks the authentication and return the current user"""
    auth_header = request.headers.get("Authorization", None)
    if auth_header:
        if auth_header.startswith("Basic"):
            user = None
            auth = request.authorization
            if auth is None or not (auth.username and auth.password):
                return None
            ab_security_manager = current_app.appbuilder.sm
            if ab_security_manager.auth_type == AUTH_LDAP:
                user = ab_security_manager.auth_user_ldap(auth.username, auth.password)
            if user is None:
                user = ab_security_manager.auth_user_db(auth.username, auth.password)
            if user is not None:
                login_user(user, remember=False)
                return user
    try:
        verify_jwt_in_request()
        return 1
    except Exception:
        pass
    return None


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        if auth_current_user() is not None:
            return function(*args, **kwargs)
        else:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Bearer"})

    return cast(T, decorated)
