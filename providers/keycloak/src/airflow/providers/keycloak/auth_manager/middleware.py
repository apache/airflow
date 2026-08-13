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

from typing import TYPE_CHECKING, cast

from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from jwt import ExpiredSignatureError, InvalidTokenError
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.core_api import security as core_api_security
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.constants import (
    COOKIE_NAME_ACCESS_TOKEN,
    COOKIE_NAME_REFRESH_TOKEN,
)
from airflow.providers.keycloak.version_compat import AIRFLOW_V_3_1_8_PLUS

try:
    from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
except ImportError:

    class AuthManagerRefreshTokenExpiredException(Exception):  # type: ignore[no-redef]
        """In case it is using a version of Airflow without ``AuthManagerRefreshTokenExpiredException``."""

        pass


if AIRFLOW_V_3_1_8_PLUS:
    from airflow.api_fastapi.app import get_cookie_path
else:

    def get_cookie_path() -> str:
        return "/"


if TYPE_CHECKING:
    from fastapi import Request, Response

    from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
    from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser


class KeycloakJWTMiddleware(BaseHTTPMiddleware):
    """
    Attach the Keycloak JWT tokens to the user.

    Gets the Keycloak JWT tokens from the request cookies
    and attaches them to the user. If the token is expired,
    attempt to refresh it using the refresh token.
    """

    async def dispatch(self, request: Request, call_next):
        user = None
        new_token = None
        new_user = None
        try:
            try:
                new_user, current_user = await self._refresh_user(request)
                user = new_user or current_user
            except (
                AuthManagerRefreshTokenExpiredException,
                HTTPException,
            ):
                new_token = ""

            if user is not None:
                request.state.user = user

                user_injected = getattr(
                    core_api_security,
                    "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
                    None,
                )
                if user_injected is not None:
                    request.state.user_authenticated_via = user_injected

            response = await call_next(request)

            if new_user or new_token is not None:
                secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
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
                            COOKIE_NAME_JWT_TOKEN,
                            "",
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
        new_user: KeycloakAuthManagerUser | None,
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
        if new_user:
            # Update keycloak token cookies
            response.set_cookie(
                COOKIE_NAME_ACCESS_TOKEN,
                new_user.access_token,
                path=cookie_path,
                secure=secure,
                samesite="lax",
                httponly=True,
            )
            if new_user.refresh_token:
                response.set_cookie(
                    COOKIE_NAME_REFRESH_TOKEN,
                    new_user.refresh_token,
                    path=cookie_path,
                    secure=secure,
                    samesite="lax",
                    httponly=True,
                )
            else:
                # No refresh token
                response.set_cookie(
                    COOKIE_NAME_REFRESH_TOKEN,
                    "",
                    path=cookie_path,
                    secure=secure,
                    samesite="lax",
                    httponly=True,
                    max_age=0,
                )
        else:
            # User is not populated, clear cookies for login
            response.set_cookie(
                COOKIE_NAME_ACCESS_TOKEN,
                "",
                path=cookie_path,
                secure=secure,
                samesite="lax",
                httponly=True,
                max_age=0,
            )
            response.set_cookie(
                COOKIE_NAME_REFRESH_TOKEN,
                "",
                path=cookie_path,
                secure=secure,
                samesite="lax",
                httponly=True,
                max_age=0,
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
    async def _refresh_user(
        request: Request,
    ) -> tuple[KeycloakAuthManagerUser | None, KeycloakAuthManagerUser | None]:
        jwt_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
        access_token = request.cookies.get(COOKIE_NAME_ACCESS_TOKEN)
        refresh_token = request.cookies.get(COOKIE_NAME_REFRESH_TOKEN)
        if not jwt_token:
            # User is not logged into Airflow
            return None, None
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="User is not logged into Keycloak."
            )
        auth_manager = cast("KeycloakAuthManager", get_auth_manager())
        try:
            user = await auth_manager.get_user_from_token(jwt_token, access_token, refresh_token)
        except ExpiredSignatureError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Expired")
        except InvalidTokenError:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid JWT token")
        return get_auth_manager().refresh_user(user=user), user
