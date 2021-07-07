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
"""Basic authentication backend"""
from functools import wraps
from typing import Any, Callable, Optional, Tuple, TypeVar, Union, cast

from flask import Response, current_app, request
from flask_appbuilder.const import AUTH_LDAP
from flask_appbuilder.security.sqla.models import User
from flask_login import login_user

CLIENT_AUTH: Optional[Union[Tuple[str, str], Any]] = None


def init_app(_):
    """Initializes authentication backend"""


T = TypeVar("T", bound=Callable)


def auth_current_user() -> Optional[User]:
    """Authenticate and set current user if Authorization header exists"""
    auth = request.authorization
    if auth is None or not auth.username or not auth.password:
        return None

    ab_security_manager = current_app.appbuilder.sm
    user = None
    if ab_security_manager.auth_type == AUTH_LDAP:
        user = ab_security_manager.auth_user_ldap(auth.username, auth.password)
    if user is None:
        user = ab_security_manager.auth_user_db(auth.username, auth.password)
    if user is not None:
        login_user(user, remember=False)
    return user


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        if auth_current_user() is not None:
            return function(*args, **kwargs)
        else:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Basic"})

    return cast(T, decorated)
