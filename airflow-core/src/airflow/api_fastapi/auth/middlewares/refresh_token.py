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

from fastapi import HTTPException, Request, Response
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import MutableHeaders

from airflow.api_fastapi.app import get_auth_manager, get_cookie_path
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.core_api.security import (
    USER_INJECTED_BY_TRUSTED_MIDDLEWARE,
    resolve_user_from_token,
)
from airflow.configuration import conf

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

# Unauthenticated endpoints that never read the user, so resolving it there would only cost a
# JWT decode and a revocation lookup on every health check and UI boot.
UNAUTHENTICATED_PATHS = frozenset(["/api/v2/monitor/health", "/api/v2/version"])


class JWTRefreshMiddleware:
    """
    Middleware to handle JWT token refresh.

    This middleware:
    1. Extracts JWT token from cookies and build the user from the token
    2. Calls ``refresh_user`` method from auth manager with the user
    3. If ``refresh_user`` returns a user, generate a JWT token based upon this user and send it in the
       response as cookie, unless the response already sets that cookie itself (e.g. a login or logout
       route)

    Requests to ``UNAUTHENTICATED_PATHS`` or under ``skip_path_prefixes`` (paths relative to the
    root path) are passed through untouched.
    """

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

        new_token = None
        try:
            new_user, current_user = await self._refresh_user(current_token)
            if user := (new_user or current_user):
                # Stamp the trust sentinel alongside the user so `get_user()`
                # can distinguish this trusted assignment from a stray write
                # by unrelated middleware.
                request.state.user = user
                request.state.user_authenticated_via = USER_INJECTED_BY_TRUSTED_MIDDLEWARE
            if new_user:
                # If we created a new user, serialize it and set it as a cookie
                new_token = get_auth_manager().generate_jwt(new_user)
        except (HTTPException, AuthManagerRefreshTokenExpiredException):
            # Receive a HTTPException when the Airflow token is expired
            # Receive a AuthManagerRefreshTokenExpiredException when the potential underlying refresh
            # token used by the auth manager is expired
            new_token = ""

        token_cookie, cleanup_cookies = (
            self._build_cookie_headers(request, new_token) if new_token is not None else (None, [])
        )
        response_started = False

        async def send_with_cookies(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                headers = MutableHeaders(scope=message)
                cookie_values = cleanup_cookies
                # The token set by the route itself wins: appending ours after it would override it.
                if token_cookie and not any(
                    value.startswith(f"{COOKIE_NAME_JWT_TOKEN}=") for value in headers.getlist("set-cookie")
                ):
                    cookie_values = [token_cookie, *cleanup_cookies]
                for value in cookie_values:
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
    def _build_cookie_headers(request: Request, new_token: str) -> tuple[str, list[str]]:
        """Return the ``Set-Cookie`` value of the token and those clearing stale token cookies."""
        cookie_path = get_cookie_path()
        secure = request.base_url.scheme == "https" or bool(conf.get("api", "ssl_cert", fallback=""))
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
        token_cookie, *cleanup_cookies = (
            value.decode("latin-1") for key, value in response.raw_headers if key == b"set-cookie"
        )
        return token_cookie, cleanup_cookies

    @staticmethod
    async def _refresh_user(current_token: str) -> tuple[BaseUser | None, BaseUser | None]:
        user = await resolve_user_from_token(current_token)
        # Auth managers may do blocking I/O here (e.g. calling an identity provider)
        return await run_in_threadpool(get_auth_manager().refresh_user, user=user), user
