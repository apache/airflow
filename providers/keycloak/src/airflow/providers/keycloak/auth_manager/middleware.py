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

import logging
from typing import TYPE_CHECKING, cast

from fastapi import HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from jwt import ExpiredSignatureError, InvalidTokenError
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import MutableHeaders

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.core_api import security as core_api_security
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.constants import (
    COOKIE_NAME_ACCESS_TOKEN,
    COOKIE_NAME_REFRESH_TOKEN,
)
from airflow.providers.keycloak.version_compat import (
    AIRFLOW_V_3_1_8_PLUS,
    AIRFLOW_V_3_4_PLUS,
    AIRFLOW_V_3_5_PLUS,
)

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


if AIRFLOW_V_3_4_PLUS:
    from airflow.api_fastapi.auth.middlewares.refresh_token import UNAUTHENTICATED_PATHS
else:
    # Unauthenticated endpoints that never read the user, so resolving it there would only cost a
    # JWT decode and a revocation lookup on every health check and UI boot.
    UNAUTHENTICATED_PATHS = frozenset(["/api/v2/monitor/health", "/api/v2/version"])

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
    from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
    from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

if AIRFLOW_V_3_5_PLUS:
    from airflow.api_fastapi.app import request_cookie_is_secure
    from airflow.api_fastapi.auth.middlewares.refresh_token import JWTRefreshMiddleware  # pyright: ignore[reportAssignmentType]
else:

    def request_cookie_is_secure(request: Request) -> bool:
        return request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))

    class JWTRefreshMiddleware:  # type: ignore[no-redef]
        """Backport shim for JWTRefreshMiddleware."""

        def __init__(self, app: ASGIApp, skip_path_prefixes: tuple[str, ...] = ()):
            self.app = app
            self.skip_path_prefixes = skip_path_prefixes

        async def __call__(self, scope: Scope, receive: Receive, send: Send):
            await self.app(scope, receive, send)
            return


log = logging.getLogger(__name__)


class KeycloakJWTMiddleware(JWTRefreshMiddleware):
    """
    Attach the Keycloak JWT tokens to the user.

    Gets the Keycloak JWT tokens from the request cookies
    and attaches them to the user. If the token is expired,
    attempt to refresh it using the refresh token.
    """

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if AIRFLOW_V_3_5_PLUS:
            return await super().__call__(scope, receive, send)

        # For Airflow v3.4.0- we mirror the implementation from JWTRefreshMiddleware.__call__()
        if scope["type"] != "http" or self._is_skipped(scope):
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        current_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
        if current_token is None:
            await self.app(scope, receive, send)
            return

        new_user = None
        session_invalidated = False
        try:
            new_user, current_user = await self.refresh_user(request)
            if user := (new_user or current_user):
                # Stamp the trust sentinel alongside the user so `get_user()`
                # can distinguish this trusted assignment from a stray write
                # by unrelated middleware.
                request.state.user = user
                request.state.user_authenticated_via = getattr(
                    core_api_security,
                    "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
                    None,
                )
        except (HTTPException, AuthManagerRefreshTokenExpiredException) as e:
            # Receive a HTTPException when the Airflow token is expired
            # Receive a AuthManagerRefreshTokenExpiredException when the potential underlying refresh
            # token used by the auth manager is expired
            log.error("Session Invalidated: %s", e)
            session_invalidated = True

        if session_invalidated or new_user is not None:
            extra_cookies = self.get_extra_cookies(request, new_user)
            token_cookie, cleanup_cookies = self.build_cookie_headers(
                request, await self.generate_new_user_token(request, new_user), extra_cookies
            )
        else:
            token_cookie, cleanup_cookies = (None, [])
        response_started = False

        async def send_with_cookies(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                headers = MutableHeaders(scope=message)
                route_cookies = headers.getlist("set-cookie")
                route_set_token = any(
                    value.startswith(f"{COOKIE_NAME_JWT_TOKEN}=") for value in route_cookies
                )
                del headers["set-cookie"]
                # The route owns the token it sets: never send ours, so a refreshed token can't outlive a logout.
                cookie_values = (
                    cleanup_cookies
                    if route_set_token or not token_cookie
                    else [token_cookie, *cleanup_cookies]
                )
                # Ours go first: for the same cookie name and path the later Set-Cookie wins, so the
                # cookies set by the route itself take precedence.
                for value in [*cookie_values, *route_cookies]:
                    headers.append("set-cookie", value)
            await send(message)

        try:
            await self.app(scope, receive, send_with_cookies)
        except HTTPException as exc:
            if response_started:
                raise
            # An HTTPException escaping the inner middlewares is returned as a response
            response = JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
            await response(scope, receive, send)

    def _is_skipped(self, scope: Scope) -> bool:
        path = scope["path"].removeprefix(scope.get("root_path", ""))
        return path in UNAUTHENTICATED_PATHS or path.startswith(self.skip_path_prefixes)

    @staticmethod
    def get_extra_cookies(request: Request, new_user: BaseUser | None) -> dict[str, str]:
        if new_user:
            user = cast("KeycloakAuthManagerUser", new_user)
            return {
                COOKIE_NAME_ACCESS_TOKEN: user.access_token,
                COOKIE_NAME_REFRESH_TOKEN: user.refresh_token or "",
            }
        return {}

    @staticmethod
    async def generate_new_user_token(request: Request, new_user: BaseUser | None) -> str:
        """If the Keycloak JWT is still valid, invalidate the Airflow JWT, otherwise generate a new JWT."""
        if new_user:
            if not await KeycloakJWTMiddleware._is_jwt_still_valid(request):
                return ""
            return get_auth_manager().generate_jwt(new_user)
        return ""

    @staticmethod
    def build_cookie_headers(
        request: Request, new_token: str, extra_cookies: dict[str, str]
    ) -> tuple[str, list[str]]:
        cookie_path = get_cookie_path()
        secure = request_cookie_is_secure(request)
        # Only used to render the Set-Cookie header values
        response = Response()
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
            response.delete_cookie(
                key=COOKIE_NAME_JWT_TOKEN,
                path="/",
                httponly=True,
                secure=secure,
                samesite="lax",
            )
        # Add any additional cookies required
        for cookie, value in extra_cookies.items():
            response.set_cookie(
                cookie,
                value,
                path=cookie_path,
                httponly=True,
                secure=secure,
                samesite="lax",
                max_age=0 if value == "" else None,
            )
        token_cookie, *cleanup_cookies = (
            value.decode("latin-1") for key, value in response.raw_headers if key == b"set-cookie"
        )
        return token_cookie, cleanup_cookies

    @staticmethod
    async def _get_user_from_request(request: Request) -> KeycloakAuthManagerUser | None:
        jwt_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN, "")
        access_token = request.cookies.get(COOKIE_NAME_ACCESS_TOKEN, "")
        refresh_token = request.cookies.get(COOKIE_NAME_REFRESH_TOKEN)
        auth_manager = cast("KeycloakAuthManager", get_auth_manager())
        args: tuple[str] | tuple[str, str | None, str | None] = (
            (jwt_token,) if AIRFLOW_V_3_5_PLUS else (jwt_token, access_token, refresh_token)
        )

        user = await auth_manager.get_user_from_token(*args)
        if user:
            if AIRFLOW_V_3_5_PLUS:
                # Re-attach Keycloak JWTs from cookies
                user.access_token = access_token
                user.refresh_token = refresh_token
            sub = auth_manager._token_subject(user.access_token)
            user_id = user.get_id()
            if sub != user_id:
                log.error("Token subject %s does not match user %s", sub, user_id)
                raise InvalidTokenError("Keycloak access token does not belong to this Airflow session")
        return user

    @staticmethod
    async def _is_jwt_still_valid(request: Request) -> bool:
        """
        Check whether the Airflow JWT sent with the request is still accepted.

        It was valid when the request came in, so it is rejected now only if it was revoked
        while the request was handled. Expiry in the meantime is not treated as revocation:
        the refreshed session replaces the token anyway.
        """
        jwt_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN)
        if not jwt_token:
            return True
        try:
            return await KeycloakJWTMiddleware._get_user_from_request(request) is not None
        except ExpiredSignatureError:
            return True
        except InvalidTokenError:
            return False

    @staticmethod
    async def refresh_user(request: Request):
        if not request.cookies.get(COOKIE_NAME_JWT_TOKEN):
            # User is not logged into Airflow.
            return None, None
        if not request.cookies.get(COOKIE_NAME_ACCESS_TOKEN):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="User is not logged into Keycloak."
            )
        try:
            user = await KeycloakJWTMiddleware._get_user_from_request(request)
        except ExpiredSignatureError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Expired")
        except InvalidTokenError:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid JWT token")
        return await run_in_threadpool(get_auth_manager().refresh_user, user=user), user
