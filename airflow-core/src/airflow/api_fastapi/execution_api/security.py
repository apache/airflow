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

``"dag_processor_session"``
    Issued to a Dag processor session by trusted provisioning, never by the
    processor itself. ``sub`` identifies the session and the ``dag_bundles``
    claim lists the bundles it may act for. Only accepted by Job registration,
    which exchanges it for a ``dag_processor`` token.

``"dag_processor"``
    Issued by Job registration for one Job of a session (``job_id`` claim), and
    valid only while that Job is open: completing the Job, or registering
    another Job for the session, ends it. It never outlives the session token
    it was exchanged for. Accepted for Job lifecycle, parsing-token exchange,
    and Connection/Variable reads needed by bundle preparation. These reads
    select a granted bundle with the ``Airflow-Dag-Bundle`` header. Routes
    declaring ``job:unchecked`` (Job completion) check the Job themselves.

``"dag_parse"``
    Exchanged by the manager for one file-parsing attempt. The subject is the
    attempt ID; signed claims identify its processor session, Job, bundle and
    relative file location. Runtime requests use the signed bundle for team
    resolution. This credential cannot manage Jobs or exchange more tokens.
    Closing or replacing its Job ends access; it never outlives the Job token.

Dag processor and parsing tokens are not refreshed by ``JWTReissueMiddleware``.

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
       - Mutating task requests — checks that the attempt UUID still exists
         in the live TI table. Already-admitted requests may finish after archival.
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

from collections.abc import Callable
from typing import Annotated, Any, ParamSpec, TypeVar, get_args
from uuid import UUID

import structlog
import svcs
from fastapi import Depends, Header, HTTPException, Request, Response, Security, status
from fastapi.params import Security as SecurityParam
from fastapi.routing import APIRoute
from fastapi.security import HTTPBearer, SecurityScopes
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api.datamodels.token import (
    DagParseClaims,
    DagParseToken,
    DagProcessorClaims,
    DagProcessorSessionToken,
    DagProcessorToken,
    ExecutionToken,
    TaskTokenScope,
    TIToken,
    TokenScope,
)
from airflow.api_fastapi.execution_api.deps import DepContainer
from airflow.jobs.job import Job
from airflow.models.callback import Callback
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.utils.session import create_session_async

log = structlog.get_logger(logger_name=__name__)

VALID_TOKEN_TYPES: frozenset[str] = frozenset(get_args(TokenScope))
_TOKEN_MODELS: dict[str, type[ExecutionToken]] = {
    **dict.fromkeys(get_args(TaskTokenScope), TIToken),
    "dag_processor_session": DagProcessorSessionToken,
    "dag_processor": DagProcessorToken,
    "dag_parse": DagParseToken,
}

_REQUEST_SCOPE_TOKEN_KEY = "ti_token"
_REQUEST_SCOPE_LIVE_ATTEMPT_KEY = "live_attempt_checked"
_REQUEST_SCOPE_JOB_KEY = "dag_processor_job_id"
_IN_PROCESS_NON_TI_CALLER = "airflow_in_process_non_ti_caller"
_SKIP_AUTO_TI_ATTEMPT_LIVE = "skip_auto_ti_attempt_live"
_P = ParamSpec("_P")
_R = TypeVar("_R")

JOB_UNCHECKED_SCOPE = "job:unchecked"


def skip_auto_ti_attempt_live(endpoint: Callable[_P, _R]) -> Callable[_P, _R]:
    """Mark a route that checks attempt liveness itself, so ``require_auth`` skips its automatic check."""
    setattr(endpoint, _SKIP_AUTO_TI_ATTEMPT_LIVE, True)
    return endpoint


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
    ) -> ExecutionToken | None:
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
        except Exception:
            log.warning("Failed to validate JWT", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid auth token")

        claims.setdefault("scope", "execution")

        try:
            scope = claims["scope"]
            token_type = (
                _TOKEN_MODELS.get(scope, ExecutionToken) if isinstance(scope, str) else ExecutionToken
            )
            token = token_type.model_validate({"id": claims.get("sub"), "claims": claims})
        except ValidationError as err:
            log.warning("JWT claims did not match Execution API principal schema", exc_info=True)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Invalid auth token: {err}")

        request.scope[_REQUEST_SCOPE_TOKEN_KEY] = token
        return token


_jwt_bearer = JWTBearer()


async def require_auth(
    security_scopes: SecurityScopes,
    request: Request,
    token: ExecutionToken = Depends(_jwt_bearer),
) -> ExecutionToken:
    """
    Enforce token type, self scopes, and live attempt identity for mutations.

    Used via ``Security(require_auth)`` on routers. ``SecurityScopes`` are
    accumulated by FastAPI from all parent ``Security()`` declarations.

    Token type enforcement reads ``route.allowed_token_types`` (precomputed
    by ``ExecutionAPIRoute``) or defaults to ``{"execution"}``.
    """
    token_scope = token.claims.scope

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

    if isinstance(token.claims, (DagProcessorClaims, DagParseClaims)) and getattr(
        route, "requires_open_job", True
    ):
        await _require_open_dag_processor_job(request, token.id, token.claims)

    if "ti:self" in security_scopes.scopes:
        ti_self_id = str(request.path_params["task_instance_id"])
        if str(token.id) != ti_self_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Token subject does not match task instance ID",
            )
    elif "ct:self" in security_scopes.scopes:
        ct_self_id = str(request.path_params["connection_test_id"])
        if str(token.id) != ct_self_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Token subject does not match connection test ID",
            )
    elif "cb:self" in security_scopes.scopes:
        cb_self_id = str(request.path_params["callback_id"])
        if str(token.id) != cb_self_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Token subject does not match callback ID",
            )

    if (
        request.method not in {"GET", "HEAD", "OPTIONS"}
        and token_scope in {"execution", "workload"}
        and "connection_test_id" not in request.path_params
        and not request.scope.get(_IN_PROCESS_NON_TI_CALLER)
        and not request.scope.get(_REQUEST_SCOPE_LIVE_ATTEMPT_KEY)
        and not getattr(route, _SKIP_AUTO_TI_ATTEMPT_LIVE, False)
    ):
        # The versions package imports routes, which depend on this module.
        from airflow.api_fastapi.execution_api.versions.v2026_10_30 import IdentifyArchivedTaskStateUpdates

        if IdentifyArchivedTaskStateUpdates.is_applied:
            await _require_live_attempt(token, allow_callback="task_instance_id" not in request.path_params)
            request.scope[_REQUEST_SCOPE_LIVE_ATTEMPT_KEY] = True

    return token


async def _require_live_attempt(token: ExecutionToken, *, allow_callback: bool) -> None:
    """
    Reject mutations from an attempt whose UUID is no longer in the working set.

    This is an admission check, not a lock: archival may race with an already
    admitted request. Use a fresh session so a prior transaction's snapshot
    cannot keep an archived UUID visible. Historical attempts never grant access.
    """
    async with create_session_async() as session:
        attempt = (
            await session.execute(
                select(TaskInstance.working_set)
                .where(TaskInstance.id == token.id)
                .execution_options(include_all_attempts=True)
            )
        ).one_or_none()
        if attempt is not None and attempt.working_set:
            return
        # Callback token exchange issues an execution token with the callback UUID.
        if (
            attempt is None
            and allow_callback
            and await session.scalar(select(Callback.id).where(Callback.id == token.id))
        ):
            return
        archived = attempt is not None
    raise HTTPException(
        status_code=status.HTTP_410_GONE if archived else status.HTTP_404_NOT_FOUND,
        detail={
            "reason": "not_found",
            "message": (
                "Task Instance not found in the working set; its attempt has been archived"
                if archived
                else "Task Instance not found"
            ),
        },
    )


async def _require_open_dag_processor_job(
    request: Request, token_id: UUID, claims: DagProcessorClaims | DagParseClaims
) -> None:
    """Refuse processor or parsing access after the Job ends or is replaced."""
    if request.scope.get(_REQUEST_SCOPE_JOB_KEY):
        return

    async with create_session_async() as session:
        session_id = claims.session_id if isinstance(claims, DagParseClaims) else token_id
        job_id = await session.scalar(
            select(Job.id).where(
                Job.id == claims.job_id, Job.session_id == session_id, Job.end_date.is_(None)
            )
        )
    if job_id is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "job_closed",
                "message": "The Job this token was issued for has completed or been replaced",
            },
        )
    request.scope[_REQUEST_SCOPE_JOB_KEY] = job_id


CurrentExecutionToken: ExecutionToken = Depends(require_auth)


def require_task_token(token: ExecutionToken = CurrentExecutionToken) -> TIToken:
    if not isinstance(token, TIToken):
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Task token required")
    return token


def require_dag_processor_session_token(
    token: ExecutionToken = CurrentExecutionToken,
) -> DagProcessorSessionToken:
    if not isinstance(token, DagProcessorSessionToken):
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Dag processor session token required")
    return token


def require_dag_processor_token(token: ExecutionToken = CurrentExecutionToken) -> DagProcessorToken:
    if not isinstance(token, DagProcessorToken):
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Dag processor token required")
    return token


CurrentTIToken: TIToken = Depends(require_task_token)
CurrentDagProcessorSessionToken: DagProcessorSessionToken = Depends(require_dag_processor_session_token)
CurrentDagProcessorToken: DagProcessorToken = Depends(require_dag_processor_token)

DAG_BUNDLE_HEADER = "Airflow-Dag-Bundle"

ExecutionOrDagParseToken = Security(require_auth, scopes=["token:execution", "token:dag_parse"])
ExecutionOrProcessorSecretsToken = Security(
    require_auth, scopes=["token:execution", "token:dag_processor", "token:dag_parse"]
)
"""Bundle preparation needs Connection and Variable reads before a file can be discovered."""


async def get_selected_dag_bundle(
    bundle_name: Annotated[str | None, Header(alias=DAG_BUNDLE_HEADER)] = None,
    token: ExecutionToken = CurrentExecutionToken,
) -> str | None:
    """Select a granted management bundle or use the immutable bundle of a parsing attempt."""
    if not isinstance(token, (DagProcessorToken, DagParseToken)):
        return None
    if isinstance(token, DagParseToken):
        signed_bundle = next(iter(token.claims.dag_bundles))
        if bundle_name is not None and bundle_name != signed_bundle:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN, detail="Bundle header conflicts with parsing token"
            )
        return signed_bundle
    if not bundle_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"A dag_processor token must name its Dag bundle in the {DAG_BUNDLE_HEADER} header",
        )
    if bundle_name not in token.claims.dag_bundles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Token is not granted Dag bundle {bundle_name!r}",
        )
    return bundle_name


SelectedDagBundle = Depends(get_selected_dag_bundle)


async def require_dag_in_granted_bundle(
    request: Request, token: ExecutionToken = CurrentExecutionToken
) -> None:
    """
    Limit a parsing request to Dags in the bundle its token grants.

    The Dag comes from the ``dag_id`` path or query parameter. A Dag without a ``DagModel`` row yet, as on its
    first parse, belongs to no other bundle, so it is let through to answer like it would for an execution token.
    """
    if not isinstance(token, (DagProcessorToken, DagParseToken)):
        return

    dag_id = request.path_params.get("dag_id") or request.query_params.get("dag_id")
    if not dag_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="A Dag must be named")
    async with create_session_async() as session:
        bundle_name = await session.scalar(select(DagModel.bundle_name).where(DagModel.dag_id == dag_id))
    if bundle_name is not None and bundle_name not in token.claims.dag_bundles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token is not granted the Dag bundle of this Dag",
        )


DagInGrantedBundle = Depends(require_dag_in_granted_bundle)


def issue_execution_token(services: svcs.Container, response: Response, sub: str) -> None:
    """Mint an ``execution``-scoped token and set it on the ``Refreshed-API-Token`` header."""
    generator: JWTGenerator = services.get(JWTGenerator)
    response.headers["Refreshed-API-Token"] = generator.generate(extras={"sub": sub, "scope": "execution"})


class ExecutionAPIRoute(APIRoute):
    """
    Custom route class that precomputes allowed token types from Security scopes.

    Scopes prefixed with ``token:`` (e.g., ``token:execution``, ``token:workload``)
    are extracted at route registration time and stored as ``allowed_token_types``.
    If no ``token:*`` scopes are declared, defaults to ``{"execution"}``.

    ``require_auth`` reads ``route.allowed_token_types`` at request time, and
    ``route.requires_open_job``, which is ``False`` only when the route
    declares the ``job:unchecked`` scope.
    """

    allowed_token_types: frozenset[str]
    skip_auto_ti_attempt_live: bool

    requires_open_job: bool

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.skip_auto_ti_attempt_live = bool(getattr(self.endpoint, _SKIP_AUTO_TI_ATTEMPT_LIVE, False))

        all_scopes: set[str] = set()
        for dep in self.dependencies:
            if isinstance(dep, SecurityParam):
                all_scopes.update(dep.scopes or [])

        token_scopes = {s.removeprefix("token:") for s in all_scopes if s.startswith("token:")}

        if token_scopes and not token_scopes <= VALID_TOKEN_TYPES:
            invalid = token_scopes - VALID_TOKEN_TYPES
            raise ValueError(f"Invalid token types in Security scopes: {invalid}")

        self.allowed_token_types = frozenset(token_scopes) if token_scopes else frozenset({"execution"})
        self.requires_open_job = JOB_UNCHECKED_SCOPE not in all_scopes


async def get_team_name_dep(
    token: ExecutionToken = CurrentExecutionToken, dag_bundle: str | None = SelectedDagBundle
) -> str | None:
    """Return the team of a task or the selected bundle of a processor or parsing attempt."""
    from airflow.configuration import conf

    if not conf.getboolean("core", "multi_team"):
        return None

    async with create_session_async() as session:
        if isinstance(token, (DagProcessorToken, DagParseToken)):
            return await session.scalar(_team_name_for_bundle_stmt(dag_bundle))
        return await session.scalar(_team_name_for_ti_stmt(token.id))


def get_team_name_for_ti(ti_id, session) -> str | None:
    """
    Return the team name associated to the task (if any), using a sync session.

    Sync counterpart to :func:`get_team_name_dep` for callers that already hold a
    SQLAlchemy session (e.g., the ``ti_run`` endpoint). No-op when multi-team is disabled.
    """
    from airflow.configuration import conf

    if not conf.getboolean("core", "multi_team"):
        return None
    return session.scalar(_team_name_for_ti_stmt(ti_id))


def _team_name_for_ti_stmt(ti_id):
    """Build the select statement resolving ``TaskInstance.id -> Team.name``."""
    return (
        select(Team.name)
        .select_from(TaskInstance)
        .join(DagModel, DagModel.dag_id == TaskInstance.dag_id)
        .join(DagBundleModel, DagBundleModel.name == DagModel.bundle_name)
        .join(DagBundleModel.teams)
        .where(TaskInstance.id == ti_id)
    )


def _team_name_for_bundle_stmt(bundle_name):
    """Build the select statement resolving ``DagBundleModel.name -> Team.name``."""
    return select(Team.name).join(DagBundleModel.teams).where(DagBundleModel.name == bundle_name)


def _team_name_for_dag_stmt(dag_id):
    """Build the select statement resolving ``DagModel.dag_id -> Team.name``."""
    return (
        select(Team.name)
        .select_from(DagModel)
        .join(DagBundleModel, DagBundleModel.name == DagModel.bundle_name)
        .join(DagBundleModel.teams)
        .where(DagModel.dag_id == dag_id)
    )
