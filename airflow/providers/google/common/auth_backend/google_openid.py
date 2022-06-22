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
"""Authentication backend that use Google credentials for authorization."""
import logging
from functools import wraps
from typing import Callable, Optional, TypeVar, cast

import google
import google.auth.transport.requests
import google.oauth2.id_token
from flask import Response, _request_ctx_stack, current_app, request as flask_request  # type: ignore
from google.auth import exceptions
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from airflow.configuration import conf
from airflow.providers.google.common.utils.id_token_credentials import get_default_id_token_credentials

log = logging.getLogger(__name__)

_GOOGLE_ISSUERS = ("accounts.google.com", "https://accounts.google.com")
AUDIENCE = conf.get("api", "google_oauth2_audience")


def create_client_session():
    """Create a HTTP authorized client."""
    service_account_path = conf.get("api", "google_key_path")
    if service_account_path:
        id_token_credentials = service_account.IDTokenCredentials.from_service_account_file(
            service_account_path
        )
    else:
        id_token_credentials = get_default_id_token_credentials(target_audience=AUDIENCE)
    return AuthorizedSession(credentials=id_token_credentials)


def init_app(_):
    """Initializes authentication."""


def _get_id_token_from_request(request) -> Optional[str]:
    authorization_header = request.headers.get("Authorization")

    if not authorization_header:
        return None

    authorization_header_parts = authorization_header.split(" ", 2)

    if len(authorization_header_parts) != 2 or authorization_header_parts[0].lower() != "bearer":
        return None

    id_token = authorization_header_parts[1]
    return id_token


def _verify_id_token(id_token: str) -> Optional[str]:
    try:
        request_adapter = google.auth.transport.requests.Request()
        id_info = google.oauth2.id_token.verify_token(id_token, request_adapter, AUDIENCE)
    except exceptions.GoogleAuthError:
        return None

    # This check is part of google-auth v1.19.0 (2020-07-09), In order not to create strong version
    # requirements to too new version, we check it in our code too.
    # One day, we may delete this code and set minimum version in requirements.
    if id_info.get("iss") not in _GOOGLE_ISSUERS:
        return None

    if not id_info.get("email_verified", False):
        return None

    return id_info.get("email")


def _lookup_user(user_email: str):
    security_manager = current_app.appbuilder.sm  # type: ignore[attr-defined]
    user = security_manager.find_user(email=user_email)

    if not user:
        return None

    if not user.is_active:
        return None

    return user


def _set_current_user(user):
    ctx = _request_ctx_stack.top
    ctx.user = user


T = TypeVar("T", bound=Callable)


def requires_authentication(function: T):
    """Decorator for functions that require authentication."""

    @wraps(function)
    def decorated(*args, **kwargs):
        access_token = _get_id_token_from_request(flask_request)
        if not access_token:
            log.debug("Missing ID Token")
            return Response("Forbidden", 403)

        userid = _verify_id_token(access_token)
        if not userid:
            log.debug("Invalid ID Token")
            return Response("Forbidden", 403)

        log.debug("Looking for user with e-mail: %s", userid)

        user = _lookup_user(userid)
        if not user:
            return Response("Forbidden", 403)

        log.debug("Found user: %s", user)

        _set_current_user(user)

        return function(*args, **kwargs)

    return cast(T, decorated)
