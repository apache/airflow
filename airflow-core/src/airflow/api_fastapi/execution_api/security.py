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

"""
Execution API security: JWT validation, token scopes, and route-level access control.

Token types (``TokenType``):

``"execution"``
    Default scope, accepted by all endpoints. Short-lived, automatically
    refreshed by ``JWTReissueMiddleware``.

``"workload"``
    Restricted scope, only accepted on routes that opt in via
    ``Security(require_auth, scopes=["token:workload"])``.

Tokens without a ``scope`` claim default to ``"execution"`` for backwards
compatibility (``claims.setdefault("scope", "execution")``).

Enforcement flow:
    1. ``JWTBearer.__call__`` validates the JWT once per request (crypto +
       signature verification), caching the result on the ASGI request scope.
       Subsequent FastAPI dependency resolutions and Cadwyn replays return
       the cache.
    2. ``require_auth`` is the Security dependency on routers. It receives
       the token from ``JWTBearer`` and enforces:
       - Token type against the route's ``allowed_token_types`` (precomputed
         by ``ExecutionAPIRoute`` from ``token:*`` Security scopes).
       - ``ti:self`` scope — checks that the JWT ``sub`` matches the
         ``{task_instance_id}`` path parameter.
    3. ``ExecutionAPIRoute`` precomputes ``allowed_token_types`` from
       ``token:*`` Security scopes at route registration time. Routes
       without explicit ``token:*`` scopes default to execution-only.

Why ``ExecutionAPIRoute`` is needed:
    FastAPI resolves router-level ``Security()`` dependencies from outermost
    to innermost. A ``token:workload`` scope on an inner endpoint would need
    to *relax* the outer router's default execution-only restriction, but
    ``SecurityScopes`` only accumulate additively — an outer dependency
    cannot see scopes declared by inner ones. ``ExecutionAPIRoute`` solves
    this by inspecting the **merged** dependency list at route registration
    time (after ``include_router`` has combined all parent and child
    dependencies) and precomputing the full ``allowed_token_types`` set.
    ``require_auth`` then reads this precomputed set from the matched route
    at request time, avoiding the ordering problem entirely.

    Any router whose routes need non-default token type policies must use
    ``route_class=ExecutionAPIRoute``. Routers that only need the default
    (execution-only) can use the standard route class — ``require_auth``
    falls back to ``{"execution"}`` when the attribute is absent.
"""

# Disable future annotations in this file to work around https://github.com/fastapi/fastapi/issues/13056
# ruff: noqa: I002

from typing import Any, Literal, get_args

import structlog
from fastapi import Depends, HTTPException, Request, status
from fastapi.params import Security as SecurityParam
from fastapi.routing import APIRoute
from fastapi.security import HTTPBearer, SecurityScopes

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.deps import DepContainer

log = structlog.get_logger(logger_name=__name__)

TokenType = Literal["execution", "workload"]

VALID_TOKEN_TYPES: frozenset[str] = frozenset(get_args(TokenType))

_REQUEST_SCOPE_TOKEN_KEY = "ti_token"


class JWTBearer(HTTPBearer):
    """
    Validates JWT tokens for the Execution API.

    Performs cryptographic validation once per request and caches the result
    on the ASGI request scope. Subsequent resolutions (FastAPI dependency
    dedup or Cadwyn replays) return the cached token.

    This dependency handles ONLY crypto validation and token construction.
    All route-specific authorization (token type, ti:self) is handled by
    ``require_auth``.
    """

    def __init__(self, required_claims: dict[str, Any] | None = None):
        super().__init__(auto_error=False)
        self.required_claims = required_claims or {}

    async def __call__(  # type: ignore[override]
        self,
        request: Request,
        services=DepContainer,
    ) -> TIToken | None:
        # Return cached token (handles both FastAPI dependency dedup and Cadwyn replays).
        if cached := request.scope.get(_REQUEST_SCOPE_TOKEN_KEY):
            return cached

        # First resolution — full cryptographic validation.
        creds = await super().__call__(request)
        if not creds:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing auth token")

        validator: JWTValidator = await services.aget(JWTValidator)

        try:
            claims = await validator.avalidated_claims(creds.credentials, dict(self.required_claims))
        except Exception as err:
            log.warning("Failed to validate JWT", exc_info=True, token=creds.credentials)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Invalid auth token: {err}")

        claims.setdefault("scope", "execution")

        token = TIToken(id=claims["sub"], claims=claims)
        request.scope[_REQUEST_SCOPE_TOKEN_KEY] = token
        return token


_jwt_bearer = JWTBearer()


async def require_auth(
    security_scopes: SecurityScopes,
    request: Request,
    token: TIToken = Depends(_jwt_bearer),
) -> TIToken:
    """
    Security dependency that enforces token type and ``ti:self`` scope.

    Used via ``Security(require_auth)`` on routers. ``SecurityScopes`` are
    accumulated by FastAPI from all parent ``Security()`` declarations.

    Token type enforcement reads ``route.allowed_token_types`` (precomputed
    by ``ExecutionAPIRoute``) or defaults to ``{"execution"}``.
    """
    token_scope = token.claims.get("scope", "execution")

    if token_scope not in VALID_TOKEN_TYPES:
        log.warning("Invalid token scope in claims", token_scope=token_scope, path=request.url.path)
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Invalid token scope: {token_scope}",
        )

    route = request.scope.get("route")
    allowed_token_types = getattr(route, "allowed_token_types", frozenset({"execution"}))

    if token_scope not in allowed_token_types:
        log.warning(
            "Token type not allowed for endpoint",
            token_scope=token_scope,
            allowed_types=sorted(allowed_token_types),
            path=request.url.path,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Token type '{token_scope}' not allowed for this endpoint. "
            f"Allowed types: {', '.join(sorted(allowed_token_types))}",
        )

    if "ti:self" in security_scopes.scopes:
        ti_self_id = str(request.path_params["task_instance_id"])
        if str(token.id) != ti_self_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Token subject does not match task instance ID",
            )

    return token


CurrentTIToken: TIToken = Depends(require_auth)


class ExecutionAPIRoute(APIRoute):
    """
    Custom route class that precomputes allowed token types from Security scopes.

    Scopes prefixed with ``token:`` (e.g., ``token:execution``, ``token:workload``)
    are extracted at route registration time and stored as ``allowed_token_types``.
    If no ``token:*`` scopes are declared, defaults to ``{"execution"}``.

    ``require_auth`` reads ``route.allowed_token_types`` at request time.
    """

    allowed_token_types: frozenset[str]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        all_scopes: set[str] = set()
        for dep in self.dependencies:
            if isinstance(dep, SecurityParam):
                all_scopes.update(dep.scopes or [])

        token_scopes = {s.removeprefix("token:") for s in all_scopes if s.startswith("token:")}

        if token_scopes and not token_scopes <= VALID_TOKEN_TYPES:
            invalid = token_scopes - VALID_TOKEN_TYPES
            raise ValueError(f"Invalid token types in Security scopes: {invalid}")

        self.allowed_token_types = frozenset(token_scopes) if token_scopes else frozenset({"execution"})


async def get_team_name_dep(token=CurrentTIToken) -> str | None:
    """Return the team name associated to the task (if any)."""
    from airflow.configuration import conf

    if not conf.getboolean("core", "multi_team"):
        return None

    from sqlalchemy import select

    from airflow.models import DagModel, TaskInstance
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.team import Team
    from airflow.utils.session import create_session_async

    stmt = (
        select(Team.name)
        .select_from(TaskInstance)
        .join(DagModel, DagModel.dag_id == TaskInstance.dag_id)
        .join(DagBundleModel, DagBundleModel.name == DagModel.bundle_name)
        .join(DagBundleModel.teams)
        .where(TaskInstance.id == token.id)
    )
    async with create_session_async() as session:
        return await session.scalar(stmt)
