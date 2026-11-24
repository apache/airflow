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
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, cast

from fastapi import HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from jwt import ExpiredSignatureError, InvalidTokenError
from keycloak import KeycloakError
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
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.version_compat import (
    AIRFLOW_V_3_1_7_PLUS,
    AIRFLOW_V_3_1_8_PLUS,
    AIRFLOW_V_3_4_1_PLUS,
    AIRFLOW_V_3_4_PLUS,
)

if AIRFLOW_V_3_1_7_PLUS:
    from airflow.providers.keycloak.auth_manager.exceptions import AuthManagerRefreshTokenExpiredException
else:
    # AuthManagerRefreshTokenExpiredException was added in Airflow v3.1.7
    AuthManagerRefreshTokenExpiredException = None  # type: ignore[assignment,misc]

if AIRFLOW_V_3_1_8_PLUS:
    from airflow.api_fastapi.app import get_cookie_path
else:

    def get_cookie_path() -> str:
        return "/"


# The core route that revokes the Airflow JWT sent with the request, relative to the root path.
LOGOUT_PATH = "/api/v2/auth/logout"
if AIRFLOW_V_3_4_PLUS:
    from airflow.api_fastapi.auth.middlewares.refresh_token import UNAUTHENTICATED_PATHS
else:
    # Unauthenticated endpoints that never read the user, so resolving it there would only cost a
    # JWT decode and a revocation lookup on every health check and UI boot.
    UNAUTHENTICATED_PATHS = frozenset(["/api/v2/monitor/health", "/api/v2/version"])

if AIRFLOW_V_3_4_1_PLUS:
    from airflow.api_fastapi.app import request_cookie_is_secure
else:

    def request_cookie_is_secure(request: Request) -> bool:
        return request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))


if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
    from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

log = logging.getLogger(__name__)

if KeycloakAuthManager._middleware_handles_cookies():
    from airflow.api_fastapi.auth.middlewares.refresh_token import JWTRefreshMiddleware  # pyright: ignore[reportAssignmentType]
else:
    # Backport for v3.3.0-v3.4.0
    class JWTRefreshMiddleware(metaclass=ABCMeta):  # type: ignore[no-redef]
        """Backport of JWTRefreshMiddleware."""

        def __init__(self, app: ASGIApp, skip_path_prefixes: tuple[str, ...] = ()) -> None:
            self.app = app
            self.skip_path_prefixes = skip_path_prefixes

        async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
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
            except (HTTPException, AuthManagerRefreshTokenExpiredException):
                # Receive a HTTPException when the Airflow token is expired
                # Receive a AuthManagerRefreshTokenExpiredException when the potential underlying refresh
                # token used by the auth manager is expired
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
        def build_cookie_headers(
            request: Request, new_token: str, extra_cookies: dict[str, str]
        ) -> tuple[str, list[str]]:
            """
            Return the ``Set-Cookie`` value of the token and those clearing stale token cookies.

            Any cookies with a nil value ("") will be cleared with Max-Age=0

            :param request: FastAPI Request
            :param new_token: New Airflow JWT token
            :param extra_cookies: Additional cookie values to set
            """
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
        @abstractmethod
        def get_extra_cookies(request: Request, new_user: BaseUser | None) -> dict[str, str]:
            """
            Get extra cookies from the request context to set in a new token response.

            Setting a cookie to "" will cause it to be cleared by build_cookie_headers(). A cookie with the
            same name and path set by the route itself takes precedence over the one returned here.
            """
            ...

        @staticmethod
        @abstractmethod
        async def generate_new_user_token(request: Request, new_user: BaseUser | None) -> str:
            """Generate a new Airflow JWT token from the user model."""
            ...

        @staticmethod
        @abstractmethod
        async def refresh_user(request: Request) -> tuple[BaseUser | None, BaseUser | None]:
            """
            Refresh the logged in user using the current JWT Token.

            If the user is not authenticated, return ``None, None``

            :param request: FastAPI Request
            """
            ...


class KeycloakJWTMiddleware(JWTRefreshMiddleware):
    """
    Attach the Keycloak JWT tokens to the user.

    Gets the Keycloak JWT tokens from the request cookies
    and attaches them to the user. If the token is expired,
    attempt to refresh it using the refresh token.
    """

    @staticmethod
    def get_extra_cookies(request: Request, new_user: BaseUser | None) -> dict[str, str]:
        if new_user and not KeycloakJWTMiddleware._is_logout(request):  # Don't issue a new token on logout
            user = cast("KeycloakAuthManagerUser", new_user)
            return {
                COOKIE_NAME_ACCESS_TOKEN: user.access_token,
                COOKIE_NAME_REFRESH_TOKEN: user.refresh_token or "",
            }
        return {COOKIE_NAME_ACCESS_TOKEN: "", COOKIE_NAME_REFRESH_TOKEN: ""}

    @staticmethod
    async def refresh_user(request: Request):
        user = None
        jwt_token = request.cookies.get(COOKIE_NAME_JWT_TOKEN, "")
        access_token = request.cookies.get(COOKIE_NAME_ACCESS_TOKEN)
        if not jwt_token:
            # User is not logged into Airflow.
            return None, None
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="User is not logged into Keycloak."
            )
        try:
            refresh_token = request.cookies.get(COOKIE_NAME_REFRESH_TOKEN)
            auth_manager = cast("KeycloakAuthManager", get_auth_manager())
            base_user = cast(
                "KeycloakAuthManagerUser",
                await auth_manager.get_user_from_token(jwt_token, should_return_user=True),
            )
            user = KeycloakAuthManager._attach_keycloak_tokens(base_user, access_token, refresh_token)
        except ExpiredSignatureError:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Expired")
        except InvalidTokenError:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid JWT token")
        if KeycloakJWTMiddleware._is_logout(request):
            # Last request that carries the refresh token cookie.
            if user.refresh_token:
                await run_in_threadpool(KeycloakJWTMiddleware._end_keycloak_session, user.refresh_token)
            # On logout return the user as new_user without refreshing to allow the Keycloak
            # Cookies to be cleared by get_extra_cookies().
            return user, user
        return await run_in_threadpool(get_auth_manager().refresh_user, user=user), user

    @staticmethod
    async def generate_new_user_token(request, new_user):
        is_logout = KeycloakJWTMiddleware._is_logout(request)
        if not new_user or is_logout:
            # Clear JWTs on logout / Invalid user.
            if not is_logout:
                KeycloakJWTMiddleware._drop_cookie(request, COOKIE_NAME_JWT_TOKEN)
            return ""
        return get_auth_manager().generate_jwt(new_user)

    @staticmethod
    def _end_keycloak_session(refresh_token: str) -> None:
        """End the Keycloak session of ``refresh_token``."""
        try:
            KeycloakAuthManager.get_keycloak_client().logout(refresh_token)
        except KeycloakError as e:
            log.warning(
                "Could not end the Keycloak session on logout (%s, response code %s)",
                type(e).__name__,
                e.response_code,
            )

    @staticmethod
    def _is_logout(request: Request) -> bool:
        """Determine whether the request is for the core logout route."""
        scope = request.scope
        return scope["path"].removeprefix(scope.get("root_path", "")) == LOGOUT_PATH

    @staticmethod
    def _drop_cookie(request: Request, name: str) -> None:
        """Remove a cookie from the request so that the inner middlewares and the route do not receive it."""
        prefix = f"{name}=".encode("latin-1")
        headers = []
        for key, value in request.scope["headers"]:
            if key != b"cookie":
                headers.append((key, value))
                continue
            kept = b";".join(part for part in value.split(b";") if not part.lstrip().startswith(prefix))
            if kept.strip():
                headers.append((key, kept.strip()))
        request.scope["headers"] = headers
