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
from airflow.api_fastapi.core_api import security as core_api_security  #
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_SECTION_NAME,
    CONF_USE_SEPARATE_COOKIES_KEY,
    COOKIE_NAME_ACCESS_TOKEN,
    COOKIE_NAME_REFRESH_TOKEN,
)
from airflow.providers.keycloak.version_compat import AIRFLOW_V_3_1_8_PLUS, AIRFLOW_V_3_4_PLUS

if AIRFLOW_V_3_1_8_PLUS:
    from airflow.api_fastapi.app import get_cookie_path
else:
    get_cookie_path = lambda: "/"

if AIRFLOW_V_3_4_PLUS:
    from airflow.api_fastapi.auth.middlewares.refresh_token import JWTRefreshMiddleware
else:

    class JWTRefreshMiddleware:  # noqa: D101
        async def dispatch(self, request, call_next):
            raise RuntimeError("JWTRefreshMiddleware should not be called from here.")


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

    Backwards compatible version of KeycloakJWTRefreshMiddleware
    """

    async def dispatch(self, request: Request, call_next):
        jwt_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
        new_token = None
        try:
            if jwt_token:
                access_token = request.cookies.get(COOKIE_NAME_ACCESS_TOKEN)
                if access_token:
                    refresh_token = request.cookies.get(COOKIE_NAME_REFRESH_TOKEN)
                    auth_manager = cast("KeycloakAuthManager", get_auth_manager())

                    try:
                        current_user = await auth_manager.get_user_from_token(
                            jwt_token, access_token, refresh_token
                        )
                    except ExpiredSignatureError:
                        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Expired")
                    except InvalidTokenError:
                        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid JWT token")

                    new_user = auth_manager.refresh_user(user=current_user)
                    user = new_user or current_user
                    if new_user:
                        new_token = auth_manager.generate_jwt(new_user)

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

            if new_token:
                cookie_path = get_cookie_path()
                secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
                response.set_cookie(
                    COOKIE_NAME_JWT_TOKEN,
                    new_token,
                    path=cookie_path,
                    secure=secure,
                    httponly=True,
                    max_age=(0 if new_token == "" else None),
                )
                if new_user:
                    response.set_cookie(
                        COOKIE_NAME_ACCESS_TOKEN,
                        new_user.access_token,
                        path=cookie_path,
                        secure=secure,
                        httponly=True,
                    )
                    response.set_cookie(
                        COOKIE_NAME_REFRESH_TOKEN,
                        new_user.refresh_token,
                        path=cookie_path,
                        secure=secure,
                        httponly=True,
                    )
        except HTTPException as exc:
            # If any HTTPException is raised during user resolution or refresh, return it as response
            return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

        return response


class KeycloakJWTRefreshMiddleware(JWTRefreshMiddleware):
    """JWTRefreshMiddleware that handles the user's Keycloak JWTs from the request cookies."""

    @classmethod
    async def _set_new_token(
        cls,
        new_token: str,
        new_user: KeycloakAuthManagerUser,
        secure: bool,
        response: Response,
        cookie_path: str | None = None,
    ):
        if cookie_path is None:
            cookie_path = get_cookie_path()
        response = await super()._set_new_token(new_token, new_user, secure, response, cookie_path)

        if conf.getboolean(CONF_SECTION_NAME, CONF_USE_SEPARATE_COOKIES_KEY, fallback=False):
            response.set_cookie(
                COOKIE_NAME_ACCESS_TOKEN,
                new_user.access_token,
                path=cookie_path,
                secure=secure,
                httponly=True,
            )
            response.set_cookie(
                COOKIE_NAME_REFRESH_TOKEN,
                new_user.refresh_token,
                path=cookie_path,
                secure=secure,
                httponly=True,
            )
        return response

    @staticmethod
    async def _refresh_user(
        current_token: str,
        request: Request,
    ):
        user = await core_api_security.resolve_user_from_token(current_token)
        if conf.getboolean(CONF_SECTION_NAME, CONF_USE_SEPARATE_COOKIES_KEY, fallback=False):
            access_token = request.get(COOKIE_NAME_ACCESS_TOKEN, "")
            refresh_token = request.get(COOKIE_NAME_REFRESH_TOKEN, None)
            if access_token == "":
                return None, None
            user.access_token = access_token
            user.refresh_token = refresh_token
        return get_auth_manager().refresh_user(user), user
