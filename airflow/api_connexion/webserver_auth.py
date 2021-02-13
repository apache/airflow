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

from datetime import datetime, timedelta
from functools import wraps
from typing import Callable, TypeVar, cast

import jwt
from flask import Response, current_app, request

from airflow.configuration import conf

SECRET = conf.get("webserver", "secret_key")
T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def encode_auth_token(user_id):
    """Generate authentication token"""
    expire_time = conf.getint("webserver", "session_lifetime_minutes")
    payload = {
        'exp': datetime.utcnow() + timedelta(minutes=expire_time),
        'iat': datetime.utcnow(),
        'sub': user_id,
    }
    return jwt.encode(payload, SECRET, algorithm='HS256')


def decode_auth_token(auth_token):
    """Decode authentication token"""
    try:
        payload = jwt.decode(auth_token, SECRET, algorithms=['HS256'])
        return payload['sub']
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return


def auth_current_user():
    """Checks the authentication and return the current user"""
    auth = request.headers.get("Authorization", None)
    token = None
    if auth:
        try:
            token = auth.split(" ")[1]
        except IndexError:
            return
    if token:
        user = decode_auth_token(token)
        if not isinstance(user, int):
            user = None
        return user


def requires_authentication(function: T):
    """Decorator for functions that require authentication"""

    @wraps(function)
    def decorated(*args, **kwargs):
        if auth_current_user() is not None:
            return function(*args, **kwargs)
        else:
            return Response("Unauthorized", 401, {"WWW-Authenticate": "Bearer"})

    return cast(T, decorated)
