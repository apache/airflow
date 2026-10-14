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

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.models.resource_details import TeamDetails
from airflow.api_fastapi.core_api.security import (
    USER_INJECTED_BY_TRUSTED_MIDDLEWARE,
    resolve_user_from_token,
)

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser

# Airflow authorizes against a small set of methods, so map the request's HTTP method onto
# one of them. This lets an auth manager grant a team read-only access to its plugin if it
# distinguishes methods; managers that only check membership can ignore it.
_HTTP_METHOD_TO_RESOURCE_METHOD: dict[str, ResourceMethod] = {
    "GET": "GET",
    "HEAD": "GET",
    "OPTIONS": "GET",
    "POST": "POST",
    "PUT": "PUT",
    "PATCH": "PUT",
    "DELETE": "DELETE",
}


class TeamAuthorizationMiddleware(BaseHTTPMiddleware):
    """
    Restrict a team-scoped plugin's FastAPI app to users authorized for that team.

    Plugin apps are mounted on the shared API server, which applies no authorization of
    its own to them, so without this every authenticated user can reach a team's plugin
    endpoints. This wraps only the owning team's sub-app, leaving global plugins and core
    routes untouched.
    """

    def __init__(self, app, team_name: str) -> None:
        super().__init__(app)
        self.team_name = team_name

    async def dispatch(self, request: Request, call_next):
        try:
            user = await self._resolve_user(request)
        except HTTPException as exception:
            # Unauthenticated or invalid token. Mirror the core API's error shape rather
            # than letting the exception escape, since a plugin sub-app does not
            # necessarily install Airflow's exception handlers.
            return JSONResponse(status_code=exception.status_code, content={"detail": exception.detail})

        authorized = await run_in_threadpool(
            get_auth_manager().is_authorized_team,
            method=_HTTP_METHOD_TO_RESOURCE_METHOD.get(request.method.upper(), "GET"),
            user=user,
            details=TeamDetails(name=self.team_name),
        )
        if not authorized:
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={"detail": f"You are not authorized to access team {self.team_name!r}."},
            )

        return await call_next(request)

    async def _resolve_user(self, request: Request) -> BaseUser:
        """
        Build the requesting user, mirroring the core API's ``get_user`` dependency.

        FastAPI dependencies are not available inside middleware, so the token is read
        from the request directly: a bearer header for API clients, otherwise the session
        cookie used by the UI.
        """
        user: BaseUser | None = getattr(request.state, "user", None)
        if user and getattr(request.state, "user_authenticated_via", None) is (
            USER_INJECTED_BY_TRUSTED_MIDDLEWARE
        ):
            return user

        authorization = request.headers.get("Authorization")
        if authorization and authorization.lower().startswith("bearer "):
            token_str: str | None = authorization[len("bearer ") :].strip()
        else:
            token_str = request.cookies.get(COOKIE_NAME_JWT_TOKEN)

        return await resolve_user_from_token(token_str)
