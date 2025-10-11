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

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.core_api.security import resolve_user_from_token
from airflow.configuration import conf


class JWTRefreshMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle JWT token refresh.

    This middleware:
    1. Extracts JWT token from cookies and build the user from the token
    2. Calls ``refresh_user`` method from auth manager with the user
    3. If ``refresh_user`` returns a user, generate a JWT token based upon this user and send it in the
       response as cookie
    """

    async def dispatch(self, request: Request, call_next):
        new_user = None
        current_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
        if current_token:
            new_user = await self._refresh_user(current_token)
            if new_user:
                request.state.user = new_user

        response = await call_next(request)

        if new_user:
            # If we created a new user, serialize it and set it as a cookie
            new_token = get_auth_manager().generate_jwt(new_user)
            secure = bool(conf.get("api", "ssl_cert", fallback=""))
            response.set_cookie(
                COOKIE_NAME_JWT_TOKEN,
                new_token,
                httponly=True,
                secure=secure,
                samesite="lax",
            )

        return response

    @staticmethod
    async def _refresh_user(current_token: str) -> BaseUser | None:
        user = await resolve_user_from_token(current_token)
        return get_auth_manager().refresh_user(user=user)
