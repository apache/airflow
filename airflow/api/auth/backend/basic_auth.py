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
import binascii
from base64 import b64decode
from functools import wraps
from typing import Callable, Optional, Tuple, TypeVar, Union, cast

from flask import Response, _request_ctx_stack, current_app, request  # type: ignore
from flask_appbuilder.security.sqla.models import User
from requests.auth import AuthBase

CLIENT_AUTH: Optional[Union[Tuple[str, str], AuthBase]] = None


def init_app(_):
    """Initializes authentication backend"""


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def auth_current_user() -> Optional[User]:
    """Authenticate and set current user if Authorization header exists"""
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return None

    auth_parts = auth_header.split(" ", 2)
    if len(auth_parts) != 2:
        return None
    if auth_parts[0].lower() != "basic":
        return None

    try:
        cred_parts = b64decode(auth_parts[1]).decode().split(":", 2)
    except binascii.Error:  # incorrect base64 encoding
        return None

    if len(cred_parts) != 2:
        return None

    (username_or_email, password) = cred_parts
    user = current_app.appbuilder.sm.auth_user_db(username_or_email, password)
    if user is not None:
        ctx = _request_ctx_stack.top
        ctx.user = user
    return user


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""
    @wraps(function)
    def decorated(*args, **kwargs):
        if auth_current_user() is not None:
            return function(*args, **kwargs)
        else:
            return Response("Unauthorized", 401)

    return cast(T, decorated)
