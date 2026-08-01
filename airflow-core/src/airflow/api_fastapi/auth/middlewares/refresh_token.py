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

from typing import TYPE_CHECKING

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.app import get_auth_manager, get_cookie_path, request_cookie_is_secure
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
from airflow.api_fastapi.core_api.security import (
    USER_INJECTED_BY_TRUSTED_MIDDLEWARE,
    resolve_user_from_token,
)

if TYPE_CHECKING:
    from fastapi import Request, Response

    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser


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
        new_token = None
        current_user = None
        new_user = None
        try:
            try:
                new_user, current_user = await self._refresh_user(request)
            except (HTTPException, AuthManagerRefreshTokenExpiredException):
                # Receive a HTTPException when the Airflow token is expired
                # Receive a AuthManagerRefreshTokenExpiredException when the potential underlying refresh
                # token used by the auth manager is expired
                new_token = ""

            if user := (new_user or current_user):
                # Stamp the trust sentinel alongside the user so `get_user()`
                # can distinguish this trusted assignment from a stray write
                # by unrelated middleware.
                request.state.user = user
                request.state.user_authenticated_via = USER_INJECTED_BY_TRUSTED_MIDDLEWARE

            response = await call_next(request)

            if new_user or new_token is not None:
                secure = request_cookie_is_secure(request)
                cookie_path = get_cookie_path()
                if new_token == "":
                    response.set_cookie(
                        COOKIE_NAME_JWT_TOKEN,
                        new_token,
                        path=cookie_path,
                        httponly=True,
                        secure=secure,
                        samesite="lax",
                        max_age=0,
                    )
                    if cookie_path != "/":
                        response.set_cookie(
                            key=COOKIE_NAME_JWT_TOKEN,
                            path="/",
                            httponly=True,
                            secure=secure,
                            samesite="lax",
                            max_age=0,
                        )
                else:
                    response = await self._set_new_token(new_user, secure, response, cookie_path)

        except HTTPException as exc:
            # If any HTTPException is raised during user resolution or refresh, return it as response
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
        return response

    @classmethod
    async def _set_new_token(
        cls,
        new_user: BaseUser | None,
        secure: bool,
        response: Response,
        cookie_path: str | None = None,
    ) -> Response:
        """
        Set Cookies in the response based on a new JWT token and a new user model.

        :param new_user: User model for the JWT token
        :param secure: HTTP secure property for cookies
        :param response: FastAPI response object to set the cookies on
        :param cookie_path: Path for cookies in the response
        """
        if cookie_path is None:
            cookie_path = get_cookie_path()
        if new_user:
            # If we created a new user, serialize it and set it as a cookie
            new_token = get_auth_manager().generate_jwt(new_user)
        else:
            new_token = ""
        response.set_cookie(
            COOKIE_NAME_JWT_TOKEN,
            new_token,
            path=cookie_path,
            httponly=True,
            secure=secure,
            samesite="lax",
            max_age=0 if new_token == "" else None,
        )
        # Clear any stale _token cookie at root path "/".
        # Older Airflow instances may have set the cookie there;
        # without this, the root-path cookie keeps being sent on
        # every request, causing an infinite redirect loop.
        if cookie_path != "/":
            response.set_cookie(
                key=COOKIE_NAME_JWT_TOKEN,
                path="/",
                httponly=True,
                secure=secure,
                samesite="lax",
                max_age=0,
            )
        return response

    @staticmethod
    async def _refresh_user(request: Request) -> tuple[BaseUser | None, BaseUser | None]:
        """
        Refresh the logged in user using the current JWT Token.

        If the user is not authenticated, return ``None, None``

        :param request: FastAPI Request
        """
        current_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
        if not current_token:
            return None, None
        user = await resolve_user_from_token(current_token)
        return get_auth_manager().refresh_user(user=user), user
