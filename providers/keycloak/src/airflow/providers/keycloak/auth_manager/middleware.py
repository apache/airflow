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

from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from jwt import ExpiredSignatureError, InvalidTokenError
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.configuration import conf

if TYPE_CHECKING:
    from fastapi import Request


class KeycloakRefreshTokenMiddleware(BaseHTTPMiddleware):
    """Middleware that automatically refreshes the Keycloak token if it is expired."""

    def __init__(self, app: Any, auth_manager: Any) -> None:
        """
        Initialize the middleware with app and auth manager.

        :param app: The FastAPI application
        :param auth_manager: The KeycloakAuthManager instance
        """
        super().__init__(app)
        self.auth_manager = auth_manager

    async def dispatch(self, request: Request, call_next):
        """Refresh the token if it is expired."""
        try:
            return await call_next(request)
        except (InvalidTokenError, ExpiredSignatureError):
            code = None
            if "code" in request.query_params:
                code = request.query_params.get("code")
                print(f"Code found in query params: {code}")

            if not code:
                return await call_next(request)

            q_params = dict(request.query_params)
            q_params["code"] = code
            request.scope["query_params"] = urlencode(q_params).encode("utf-8")

            user = self.auth_manager.get_user_from_code(
                code=code, redirect_uri=request.url_for("login_callback")
            )
            if not user.refresh_token:
                return await call_next(request)
            token = self.auth_manager.refresh_token(user.refresh_token)
            response = await call_next(request)
            # Set the new token in the response cookies
            secure = bool(conf.get("api", "ssl_cert", fallback=""))
            response.set_cookie(COOKIE_NAME_JWT_TOKEN, token, secure=secure)
            return response
