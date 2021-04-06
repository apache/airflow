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
from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast

try:
    from flask import _app_ctx_stack as ctx_stack
except ImportError:  # pragma: no cover
    from flask import _request_ctx_stack as ctx_stack
from flask import Response, current_app, request
from flask_jwt_extended.config import config
from flask_jwt_extended.utils import verify_token_claims
from flask_jwt_extended.view_decorators import _decode_jwt_from_request, _load_user

from airflow.api_connexion.exceptions import PermissionDenied, Unauthenticated
from airflow.models.auth import Tokens

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def verify_jwt_access_token_():
    """Verify JWT in request"""
    if request.method not in config.exempt_methods:
        jwt_data, jwt_header = _decode_jwt_from_request(request_type='access')
        ctx_stack.top.jwt = jwt_data
        ctx_stack.top.jwt_header = jwt_header
        verify_token_claims(jwt_data)
        _load_user(jwt_data[config.identity_claim_key])
        token = Tokens.get_token(jwt_data)
        if token and token.is_revoked:
            raise Unauthenticated(detail="Token revoked")
        if token and token.expiry_delta < datetime.now() + timedelta(minutes=1):
            Tokens.delete_token(token)
            raise Unauthenticated(detail="Token expired and we have deleted it")
        if not token:
            raise Unauthenticated(detail="Token Unknown")


def verify_jwt_refresh_token_in_request_():
    """
    Ensure that the requester has a valid refresh token. Raises an appropiate
    exception if there is no token or the token is invalid.
    """
    if request.method not in config.exempt_methods:
        jwt_data, jwt_header = _decode_jwt_from_request(request_type='refresh')
        ctx_stack.top.jwt = jwt_data
        ctx_stack.top.jwt_header = jwt_header
        _load_user(jwt_data[config.identity_claim_key])
        token = Tokens.get_token(jwt_data)
        if token and token.is_revoked:
            raise Unauthenticated(detail="Token revoked")
        if token and token.expiry_delta < datetime.now() + timedelta(minutes=1):
            Tokens.delete_token(token)
            raise Unauthenticated(detail="Token expired and we have deleted it")
        if not token:
            raise Unauthenticated(detail="Token Unknown")


def jwt_refresh_token_required_(fn):
    """
    A decorator to protect a an endpoint.
    If you decorate an endpoint with this, it will ensure that the requester
    has a valid refresh token before allowing the endpoint to be called.
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        verify_jwt_refresh_token_in_request_()
        return fn(*args, **kwargs)

    return wrapper


def check_authentication() -> None:
    """Checks that the request has valid authorization information."""
    response = current_app.api_auth.requires_authentication(Response)()
    if response.status_code == 200:
        return
    try:
        verify_jwt_access_token_()
        return
    except Exception:  # pylint: disable=broad-except
        pass
        # since this handler only checks authentication, not authorization,
        # we should always return 401
    raise Unauthenticated(headers=response.headers)


def requires_access(permissions: Optional[Sequence[Tuple[str, str]]] = None) -> Callable[[T], T]:
    """Factory for decorator that checks current user's permissions against required permissions."""
    appbuilder = current_app.appbuilder
    appbuilder.sm.sync_resource_permissions(permissions)

    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            check_authentication()
            if appbuilder.sm.check_authorization(permissions, kwargs.get('dag_id')):
                return func(*args, **kwargs)
            raise PermissionDenied()

        return cast(T, decorated)

    return requires_access_decorator
