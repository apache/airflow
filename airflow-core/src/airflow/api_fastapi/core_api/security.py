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

import posixpath
from collections.abc import Callable, Coroutine
from contextlib import suppress
from json import JSONDecodeError
from typing import TYPE_CHECKING, Annotated, Any, cast
from urllib.parse import ParseResult, unquote, urljoin, urlparse

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, OAuth2PasswordBearer
from itsdangerous import BadSignature, URLSafeSerializer
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import (
    COOKIE_NAME_JWT_TOKEN,
    BaseAuthManager,
)
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.auth.managers.models.batch_apis import (
    IsAuthorizedConnectionRequest,
    IsAuthorizedDagRequest,
    IsAuthorizedPoolRequest,
    IsAuthorizedVariableRequest,
)
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    AssetAliasDetails,
    AssetDetails,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.base import OrmClause
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkAction,
    BulkActionOnExistence,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.connections import ConnectionBody
from airflow.api_fastapi.core_api.datamodels.dag_run import BulkDAGRunBody, BulkDAGRunClearBody
from airflow.api_fastapi.core_api.datamodels.pools import PoolBody
from airflow.api_fastapi.core_api.datamodels.variables import VariableBody
from airflow.configuration import conf
from airflow.models import Connection, Pool, Variable
from airflow.models.backfill import Backfill
from airflow.models.dag import DagModel, DagRun, DagTag
from airflow.models.dag_version import DagVersion
from airflow.models.dagwarning import DagWarning
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.team import Team
from airflow.models.xcom import XComModel

if TYPE_CHECKING:
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod


def auth_manager_from_app(request: Request) -> BaseAuthManager:
    """
    FastAPI dependency resolver that returns the shared AuthManager instance from app.state.

    This ensures that all API routes using AuthManager via dependency injection receive the same
    singleton instance that was initialized at app startup.
    """
    return request.app.state.auth_manager


AuthManagerDep = Annotated[BaseAuthManager, Depends(auth_manager_from_app)]

auth_description = (
    "To authenticate Airflow API requests, clients must include a JWT (JSON Web Token) in "
    "the Authorization header of each request. This token is used to verify the identity of "
    "the client and ensure that they have the appropriate permissions to access the "
    "requested resources. "
    "You can use the endpoint ``POST /auth/token`` in order to generate a JWT token. "
    "Upon successful authentication, the server will issue a JWT token that contains the necessary "
    "information (such as user identity and scope) to authenticate subsequent requests. "
    "To learn more about Airflow public API authentication, please read https://airflow.apache.org/docs/apache-airflow/stable/security/api.html."
)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token", description=auth_description, auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)

MAP_BULK_ACTION_TO_AUTH_METHOD: dict[BulkAction, ResourceMethod] = {
    BulkAction.CREATE: "POST",
    BulkAction.DELETE: "DELETE",
    BulkAction.UPDATE: "PUT",
}


async def resolve_user_from_token(token_str: str | None) -> BaseUser:
    if not token_str:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    try:
        return await get_auth_manager().get_user_from_token(token_str)
    except ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token Expired")
    except InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid JWT token")


# Sentinel marker that designates a `request.state.user` value as having come from a
# trusted, in-tree authentication code path (currently only `JWTRefreshMiddleware`).
# `get_user()` only honours `request.state.user` when this sentinel is also present
# at `request.state.user_authenticated_via`. This is defense-in-depth against an
# accidental `request.state.user = ...` assignment in unrelated middleware (a typo,
# a third-party plugin, a fixture leaked into production); it does NOT defend against
# a malicious in-process plugin that imports the sentinel and sets it itself, since
# plugins are trusted code in Airflow's security model — the goal is to keep an
# accidental write from silently bypassing JWT validation.
USER_INJECTED_BY_TRUSTED_MIDDLEWARE = object()


async def get_user(
    request: Request,
    oauth_token: str | None = Depends(oauth2_scheme),
    bearer_credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> BaseUser:
    # A user might have been already built by a trusted in-tree middleware (currently
    # only `JWTRefreshMiddleware`); if so, it is stored in `request.state.user` AND
    # `request.state.user_authenticated_via` is set to the trust sentinel above.
    # Honour the cached user only when both are present, so a stray `state.user`
    # assignment from unrelated middleware can't bypass JWT validation.
    user: BaseUser | None = getattr(request.state, "user", None)
    trust_marker = getattr(request.state, "user_authenticated_via", None)
    if user and trust_marker is USER_INJECTED_BY_TRUSTED_MIDDLEWARE:
        return user

    token_str: str | None
    if bearer_credentials and bearer_credentials.scheme.lower() == "bearer":
        token_str = bearer_credentials.credentials
    elif oauth_token:
        token_str = oauth_token
    else:
        token_str = request.cookies.get(COOKIE_NAME_JWT_TOKEN)

    return await resolve_user_from_token(token_str)


GetUserDep = Annotated[BaseUser, Depends(get_user)]


def requires_access_dag(
    method: ResourceMethod,
    access_entity: DagAccessEntity | None = None,
    param_dag_id: str | None = None,
) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        # Required for the closure to capture the dag_id but still be able to mutate it.
        # Prevent from using a nonlocal statement causing test failures.
        dag_id = param_dag_id
        if dag_id is None:
            dag_id = request.path_params.get("dag_id") or request.query_params.get("dag_id")
            dag_id = dag_id if dag_id != "~" else None

        team_name = DagModel.get_team_name(dag_id) if dag_id else None

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_dag(
                method=method,
                access_entity=access_entity,
                details=DagDetails(id=dag_id, team_name=team_name),
                user=user,
            )
        )

    return inner


def requires_access_dag_from_file_token(
    method: ResourceMethod,
) -> Callable[[str, Request, BaseUser, Session], None]:
    """
    Authorize the caller against the DAGs referenced by a signed ``file_token``.

    For ``file_token`` based endpoints (such as ``reparse``), the token is resolved to its referenced file, and authorization is performed against exactly the DAGs defined in that file, never against a request parameter.
    """

    def inner(
        file_token: str,
        request: Request,
        user: GetUserDep,
        session: SessionDep,
    ) -> None:
        try:
            payload = URLSafeSerializer(request.app.state.secret_key).loads(file_token)
        except BadSignature:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found")

        dag_ids = list(
            session.scalars(
                select(DagModel.dag_id).where(
                    DagModel.bundle_name == payload["bundle_name"],
                    DagModel.relative_fileloc == payload["relative_fileloc"],
                )
            )
        )
        if not dag_ids:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "File not found")

        dag_id_to_team = DagModel.get_dag_id_to_team_name_mapping(dag_ids, session=session)
        requests: list[IsAuthorizedDagRequest] = [
            {"method": method, "details": DagDetails(id=dag_id, team_name=dag_id_to_team.get(dag_id))}
            for dag_id in dag_ids
        ]
        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_dag(requests, user=user),
        )

    return inner


class PermittedDagFilter(OrmClause[set[str]]):
    """A parameter that filters the permitted dags for the user."""

    def to_orm(self, select: Select) -> Select:
        # self.value may be None (OrmClause holds Optional), ensure we pass an Iterable to in_
        return select.where(DagModel.dag_id.in_(self.value or set()))


class PermittedDagRunFilter(PermittedDagFilter):
    """A parameter that filters the permitted dag runs for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagRun.dag_id.in_(self.value or set()))


class PermittedDagWarningFilter(PermittedDagFilter):
    """A parameter that filters the permitted dag warnings for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagWarning.dag_id.in_(self.value or set()))


class PermittedEventLogFilter(PermittedDagFilter):
    """A parameter that filters the permitted even logs for the user."""

    def to_orm(self, select: Select) -> Select:
        # Event Logs not related to Dags have dag_id as None and are always returned.
        # return select.where(Log.dag_id.in_(self.value or set()) or Log.dag_id.is_(None))
        return select.where(or_(Log.dag_id.in_(self.value or set()), Log.dag_id.is_(None)))


class PermittedTIFilter(PermittedDagFilter):
    """A parameter that filters the permitted task instances for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(TI.dag_id.in_(self.value or set()))


class PermittedXComFilter(PermittedDagFilter):
    """A parameter that filters the permitted XComs for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(XComModel.dag_id.in_(self.value or set()))


class PermittedTagFilter(PermittedDagFilter):
    """A parameter that filters the permitted dag tags for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagTag.dag_id.in_(self.value or set()))


class PermittedDagVersionFilter(PermittedDagFilter):
    """A parameter that filters the permitted dag versions for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagVersion.dag_id.in_(self.value or set()))


class PermittedBackfillFilter(PermittedDagFilter):
    """A parameter that filters the permitted backfills for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(Backfill.dag_id.in_(self.value or set()))


def permitted_dag_filter_factory(
    method: ResourceMethod, filter_class=PermittedDagFilter
) -> Callable[[BaseUser, BaseAuthManager], PermittedDagFilter]:
    """
    Create a callable for Depends in FastAPI that returns a filter of the permitted dags for the user.

    :param method: whether filter readable or writable.
    :return: The callable that can be used as Depends in FastAPI.
    """

    def depends_permitted_dags_filter(
        user: GetUserDep,
        auth_manager: AuthManagerDep,
    ) -> PermittedDagFilter:
        authorized_dags: set[str] = auth_manager.get_authorized_dag_ids(user=user, method=method)
        return filter_class(authorized_dags)

    return depends_permitted_dags_filter


EditableDagsFilterDep = Annotated[PermittedDagFilter, Depends(permitted_dag_filter_factory("PUT"))]
ReadableDagsFilterDep = Annotated[PermittedDagFilter, Depends(permitted_dag_filter_factory("GET"))]
ReadableDagRunsFilterDep = Annotated[
    PermittedDagRunFilter, Depends(permitted_dag_filter_factory("GET", PermittedDagRunFilter))
]
ReadableDagWarningsFilterDep = Annotated[
    PermittedDagWarningFilter, Depends(permitted_dag_filter_factory("GET", PermittedDagWarningFilter))
]
ReadableTIFilterDep = Annotated[
    PermittedTIFilter, Depends(permitted_dag_filter_factory("GET", PermittedTIFilter))
]
ReadableEventLogsFilterDep = Annotated[
    PermittedTIFilter, Depends(permitted_dag_filter_factory("GET", PermittedEventLogFilter))
]
ReadableXComFilterDep = Annotated[
    PermittedXComFilter, Depends(permitted_dag_filter_factory("GET", PermittedXComFilter))
]

ReadableTagsFilterDep = Annotated[
    PermittedTagFilter, Depends(permitted_dag_filter_factory("GET", PermittedTagFilter))
]
ReadableDagVersionsFilterDep = Annotated[
    PermittedDagVersionFilter, Depends(permitted_dag_filter_factory("GET", PermittedDagVersionFilter))
]
ReadableBackfillsFilterDep = Annotated[
    PermittedBackfillFilter, Depends(permitted_dag_filter_factory("GET", PermittedBackfillFilter))
]


def requires_access_backfill(
    method: ResourceMethod,
) -> Callable[[Request, BaseUser, Session], Coroutine[Any, Any, None]]:
    """Wrap ``requires_access_dag`` and extract the dag_id from the backfill_id."""

    async def inner(
        request: Request,
        user: GetUserDep,
        session: SessionDep,
    ) -> None:
        dag_id = None

        # Try to retrieve the dag_id from the backfill_id path param
        backfill_id_raw = request.path_params.get("backfill_id")
        try:
            backfill_id = int(backfill_id_raw) if backfill_id_raw is not None else None
        except ValueError:
            backfill_id = None

        if backfill_id is not None:
            backfill = session.scalars(select(Backfill).where(Backfill.id == backfill_id)).one_or_none()
            dag_id = backfill.dag_id if backfill else None

        # Try to retrieve the dag_id from the request body (POST backfill)
        if dag_id is None:
            # Not a json body, ignore
            with suppress(JSONDecodeError):
                body = await request.json()
                if isinstance(body, dict):
                    dag_id = body.get("dag_id")
            if dag_id is not None and not isinstance(dag_id, str):
                # Fail closed: reject non-string dag_id before authz decision.
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'dag_id' must be a string",
                )

        requires_access_dag(method, DagAccessEntity.RUN, dag_id)(
            request,
            user,
        )

    return inner


def requires_access_event_log(
    method: ResourceMethod,
) -> Callable[[Request, BaseUser, Session], Coroutine[Any, Any, None]]:
    """Wrap ``requires_access_dag`` and extract the dag_id from the event_log_id."""

    async def inner(
        request: Request,
        user: GetUserDep,
        session: SessionDep,
    ) -> None:
        dag_id = None

        event_log_id_raw = request.path_params.get("event_log_id")
        if event_log_id_raw is not None:
            try:
                event_log_id = int(event_log_id_raw)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'event_log_id' must be an integer",
                )
            dag_id = session.scalar(select(Log.dag_id).where(Log.id == event_log_id))

        requires_access_dag(method, DagAccessEntity.AUDIT_LOG, dag_id)(
            request,
            user,
        )

    return inner


class PermittedPoolFilter(OrmClause[set[str]]):
    """A parameter that filters the permitted pools for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(Pool.pool.in_(self.value or set()))


def permitted_pool_filter_factory(
    method: ResourceMethod,
) -> Callable[[BaseUser, BaseAuthManager], PermittedPoolFilter]:
    """
    Create a callable for Depends in FastAPI that returns a filter of the permitted pools for the user.

    :param method: whether filter readable or writable.
    """

    def depends_permitted_pools_filter(
        user: GetUserDep,
        auth_manager: AuthManagerDep,
    ) -> PermittedPoolFilter:
        authorized_pools: set[str] = auth_manager.get_authorized_pools(user=user, method=method)
        return PermittedPoolFilter(authorized_pools)

    return depends_permitted_pools_filter


ReadablePoolsFilterDep = Annotated[PermittedPoolFilter, Depends(permitted_pool_filter_factory("GET"))]


def requires_access_pool(method: ResourceMethod) -> Callable[[Request, BaseUser], Coroutine[Any, Any, None]]:
    async def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        pool_name = request.path_params.get("pool_name")
        for team_name in await _collect_teams_to_check(method, request, pool_name, Pool.get_team_name):

            def _callback(tn: str | None = team_name) -> bool:
                return get_auth_manager().is_authorized_pool(
                    method=method, details=PoolDetails(name=pool_name, team_name=tn), user=user
                )

            _requires_access(is_authorized_callback=_callback)

    return inner


def requires_access_pool_bulk() -> Callable[[BulkBody[PoolBody], BaseUser], None]:
    def inner(
        request: BulkBody[PoolBody],
        user: GetUserDep,
    ) -> None:
        multi_team = conf.getboolean("core", "multi_team")
        # Build the list of pool names provided as part of the request that may correspond to
        # an existing resource (UPDATE / DELETE, or CREATE+OVERWRITE which may turn into a PUT).
        existing_pool_names = [
            cast("str", entity) if action.action == BulkAction.DELETE else cast("PoolBody", entity).pool
            for action in request.actions
            for entity in action.entities
            if _bulk_action_needs_existing_team_lookup(action)
        ]
        # For each pool, find its associated team (if it exists)
        pool_name_to_team = Pool.get_name_to_team_name_mapping(existing_pool_names)

        requests: list[IsAuthorizedPoolRequest] = []
        for action in request.actions:
            methods = _get_resource_methods_from_bulk_request(action)
            for pool in action.entities:
                pool_name = (
                    cast("str", pool) if action.action == BulkAction.DELETE else cast("PoolBody", pool).pool
                )
                for method in methods:
                    req: IsAuthorizedPoolRequest = {
                        "method": method,
                        "details": PoolDetails(
                            name=pool_name,
                            team_name=pool_name_to_team.get(pool_name),
                        ),
                    }
                    requests.append(req)
                # Authorize the destination team_name when the entity body requests a team change.
                if multi_team and _bulk_action_sets_team(action):
                    dest_team = cast("PoolBody", pool).team_name
                    if dest_team is not None and dest_team != pool_name_to_team.get(pool_name):
                        for method in methods:
                            requests.append(
                                {
                                    "method": method,
                                    "details": PoolDetails(name=pool_name, team_name=dest_team),
                                }
                            )

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_pool(
                requests=requests,
                user=user,
            )
        )

    return inner


class PermittedConnectionFilter(OrmClause[set[str]]):
    """A parameter that filters the permitted connections for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(Connection.conn_id.in_(self.value or set()))


def permitted_connection_filter_factory(
    method: ResourceMethod,
) -> Callable[[BaseUser, BaseAuthManager], PermittedConnectionFilter]:
    """
    Create a callable for Depends in FastAPI that returns a filter of the permitted connections for the user.

    :param method: whether filter readable or writable.
    """

    def depends_permitted_connections_filter(
        user: GetUserDep,
        auth_manager: AuthManagerDep,
    ) -> PermittedConnectionFilter:
        authorized_connections: set[str] = auth_manager.get_authorized_connections(user=user, method=method)
        return PermittedConnectionFilter(authorized_connections)

    return depends_permitted_connections_filter


ReadableConnectionsFilterDep = Annotated[
    PermittedConnectionFilter, Depends(permitted_connection_filter_factory("GET"))
]


def requires_access_connection(
    method: ResourceMethod,
) -> Callable[[Request, BaseUser], Coroutine[Any, Any, None]]:
    async def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        connection_id = request.path_params.get("connection_id")
        for team_name in await _collect_teams_to_check(
            method, request, connection_id, Connection.get_team_name
        ):

            def _callback(tn: str | None = team_name) -> bool:
                return get_auth_manager().is_authorized_connection(
                    method=method,
                    details=ConnectionDetails(conn_id=connection_id, team_name=tn),
                    user=user,
                )

            _requires_access(is_authorized_callback=_callback)

    return inner


def requires_access_connection_bulk() -> Callable[[BulkBody[ConnectionBody], BaseUser], None]:
    def inner(
        request: BulkBody[ConnectionBody],
        user: GetUserDep,
    ) -> None:
        multi_team = conf.getboolean("core", "multi_team")
        # Build the list of ``conn_id`` provided as part of the request that may correspond to
        # an existing resource (UPDATE / DELETE, or CREATE+OVERWRITE which may turn into a PUT).
        existing_connection_ids = [
            cast("str", entity)
            if action.action == BulkAction.DELETE
            else cast("ConnectionBody", entity).connection_id
            for action in request.actions
            for entity in action.entities
            if _bulk_action_needs_existing_team_lookup(action)
        ]
        # For each connection, find its associated team (if it exists)
        conn_id_to_team = Connection.get_conn_id_to_team_name_mapping(existing_connection_ids)

        requests: list[IsAuthorizedConnectionRequest] = []
        for action in request.actions:
            methods = _get_resource_methods_from_bulk_request(action)
            for connection in action.entities:
                connection_id = (
                    cast("str", connection)
                    if action.action == BulkAction.DELETE
                    else cast("ConnectionBody", connection).connection_id
                )
                for method in methods:
                    req: IsAuthorizedConnectionRequest = {
                        "method": method,
                        "details": ConnectionDetails(
                            conn_id=connection_id,
                            team_name=conn_id_to_team.get(connection_id),
                        ),
                    }
                    requests.append(req)
                # Authorize the destination team_name when the entity body requests a team change.
                if multi_team and _bulk_action_sets_team(action):
                    dest_team = cast("ConnectionBody", connection).team_name
                    if dest_team is not None and dest_team != conn_id_to_team.get(connection_id):
                        for method in methods:
                            requests.append(
                                {
                                    "method": method,
                                    "details": ConnectionDetails(conn_id=connection_id, team_name=dest_team),
                                }
                            )

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_connection(
                requests=requests,
                user=user,
            )
        )

    return inner


def requires_access_configuration(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        section: str | None = request.query_params.get("section") or request.path_params.get("section")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_configuration(
                method=method,
                details=ConfigurationDetails(section=section),
                user=user,
            )
        )

    return inner


class PermittedTeamFilter(OrmClause[set[str]]):
    """A parameter that filters the permitted teams for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(Team.name.in_(self.value or set()))


def permitted_team_filter_factory() -> Callable[[BaseUser, BaseAuthManager], PermittedTeamFilter]:
    """Create a callable for Depends in FastAPI that returns a filter of the permitted teams for the user."""

    def depends_permitted_teams_filter(
        user: GetUserDep,
        auth_manager: AuthManagerDep,
    ) -> PermittedTeamFilter:
        authorized_teams: set[str] = auth_manager.get_authorized_teams(user=user, method="GET")
        return PermittedTeamFilter(authorized_teams)

    return depends_permitted_teams_filter


ReadableTeamsFilterDep = Annotated[PermittedTeamFilter, Depends(permitted_team_filter_factory())]


class PermittedVariableFilter(OrmClause[set[str]]):
    """A parameter that filters the permitted variables for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(Variable.key.in_(self.value or set()))


def permitted_variable_filter_factory(
    method: ResourceMethod,
) -> Callable[[BaseUser, BaseAuthManager], PermittedVariableFilter]:
    """
    Create a callable for Depends in FastAPI that returns a filter of the permitted variables for the user.

    :param method: whether filter readable or writable.
    """

    def depends_permitted_variables_filter(
        user: GetUserDep,
        auth_manager: AuthManagerDep,
    ) -> PermittedVariableFilter:
        authorized_variables: set[str] = auth_manager.get_authorized_variables(user=user, method=method)
        return PermittedVariableFilter(authorized_variables)

    return depends_permitted_variables_filter


ReadableVariablesFilterDep = Annotated[
    PermittedVariableFilter, Depends(permitted_variable_filter_factory("GET"))
]


def requires_access_variable(
    method: ResourceMethod,
) -> Callable[[Request, BaseUser], Coroutine[Any, Any, None]]:
    async def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        variable_key: str | None = request.path_params.get("variable_key")
        for team_name in await _collect_teams_to_check(method, request, variable_key, Variable.get_team_name):

            def _callback(tn: str | None = team_name) -> bool:
                return get_auth_manager().is_authorized_variable(
                    method=method, details=VariableDetails(key=variable_key, team_name=tn), user=user
                )

            _requires_access(is_authorized_callback=_callback)

    return inner


def requires_access_variable_bulk() -> Callable[[BulkBody[VariableBody], BaseUser], None]:
    def inner(
        request: BulkBody[VariableBody],
        user: GetUserDep,
    ) -> None:
        multi_team = conf.getboolean("core", "multi_team")
        # Build the list of variable keys provided as part of the request that may correspond to
        # an existing resource (UPDATE / DELETE, or CREATE+OVERWRITE which may turn into a PUT).
        existing_variable_keys = [
            cast("str", entity) if action.action == BulkAction.DELETE else cast("VariableBody", entity).key
            for action in request.actions
            for entity in action.entities
            if _bulk_action_needs_existing_team_lookup(action)
        ]
        # For each variable, find its associated team (if it exists)
        var_key_to_team = Variable.get_key_to_team_name_mapping(existing_variable_keys)

        requests: list[IsAuthorizedVariableRequest] = []
        for action in request.actions:
            methods = _get_resource_methods_from_bulk_request(action)
            for variable in action.entities:
                variable_key = (
                    cast("str", variable)
                    if action.action == BulkAction.DELETE
                    else cast("VariableBody", variable).key
                )
                for method in methods:
                    req: IsAuthorizedVariableRequest = {
                        "method": method,
                        "details": VariableDetails(
                            key=variable_key,
                            team_name=var_key_to_team.get(variable_key),
                        ),
                    }
                    requests.append(req)
                # Authorize the destination team_name when the entity body requests a team change.
                if multi_team and _bulk_action_sets_team(action):
                    dest_team = cast("VariableBody", variable).team_name
                    if dest_team is not None and dest_team != var_key_to_team.get(variable_key):
                        for method in methods:
                            requests.append(
                                {
                                    "method": method,
                                    "details": VariableDetails(key=variable_key, team_name=dest_team),
                                }
                            )

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_variable(
                requests=requests,
                user=user,
            )
        )

    return inner


def _build_dag_run_access_requests(
    entity_methods: list[tuple[str, ResourceMethod]],
) -> list[IsAuthorizedDagRequest]:
    """
    Build per-entity DagRun authorization requests for a batched access check.

    ``entity_methods`` is a list of ``(dag_id, method)`` pairs with unresolvable
    entries (no dag_id or the ``~`` wildcard) already filtered out by the caller.
    Teams for all Dags are resolved in a single batched query and shared across each
    Dag's requests.
    """
    if not entity_methods:
        return []
    resolved_dag_ids = list({dag_id for dag_id, _ in entity_methods})
    dag_id_to_team = DagModel.get_dag_id_to_team_name_mapping(resolved_dag_ids)
    return [
        {
            "method": method,
            "access_entity": DagAccessEntity.RUN,
            "details": DagDetails(id=dag_id, team_name=dag_id_to_team.get(dag_id)),
        }
        for dag_id, method in entity_methods
    ]


def requires_access_dag_run_bulk() -> Callable[[BulkBody[BulkDAGRunBody], BaseUser, str], None]:
    def inner(
        request: BulkBody[BulkDAGRunBody],
        user: GetUserDep,
        dag_id: str,
    ) -> None:
        entity_methods: list[tuple[str, ResourceMethod]] = []
        for action in request.actions:
            methods = _get_resource_methods_from_bulk_request(action)
            for entity in action.entities:
                if isinstance(entity, str):
                    entity_dag_id: str | None = dag_id
                else:
                    entity_dag_id = entity.dag_id or dag_id
                # Entities that can't be resolved are surfaced as 400 in the service's BulkResponse.
                if not entity_dag_id or entity_dag_id == "~":
                    continue
                for method in methods:
                    entity_methods.append((entity_dag_id, method))

        requests = _build_dag_run_access_requests(entity_methods)
        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_dag(
                requests=requests,
                user=user,
            )
        )

    return inner


def requires_access_dag_run_clear_bulk() -> Callable[[BulkDAGRunClearBody, BaseUser, str], None]:
    def inner(
        body: BulkDAGRunClearBody,
        user: GetUserDep,
        dag_id: str,
    ) -> None:
        entity_methods: list[tuple[str, ResourceMethod]] = []
        for run in body.dag_runs:
            entity_dag_id = run.dag_id or dag_id
            if not entity_dag_id or entity_dag_id == "~":
                continue
            entity_methods.append((entity_dag_id, "PUT"))

        if not body.dag_runs and body.has_partition_selectors:
            if dag_id and dag_id != "~":
                entity_methods.append((dag_id, "PUT"))

        requests = _build_dag_run_access_requests(entity_methods)
        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_dag(
                requests=requests,
                user=user,
            )
        )

    return inner


def requires_access_asset(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        asset_id = request.path_params.get("asset_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_asset(
                method=method, details=AssetDetails(id=asset_id), user=user
            ),
        )

    return inner


def requires_access_view(access_view: AccessView) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_view(
                access_view=access_view, user=user
            ),
        )

    return inner


def requires_access_asset_alias(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        asset_alias_id: str | None = request.path_params.get("asset_alias_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_asset_alias(
                method=method, details=AssetAliasDetails(id=asset_alias_id), user=user
            ),
        )

    return inner


def requires_authenticated() -> Callable:
    """Just ensure the user is authenticated - no need to check any specific permissions."""

    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        pass

    return inner


async def _collect_teams_to_check(
    method: ResourceMethod,
    request: Request,
    resource_id: str | None,
    get_existing_team: Callable[[str], str | None],
) -> set[str | None]:
    """Collect validated team names from existing resource (DB) and/or request body."""
    if not conf.getboolean("core", "multi_team"):
        return {None}
    teams: set[str | None] = set()
    if method != "POST":
        teams.add(get_existing_team(resource_id) if resource_id else None)
    if method in ("POST", "PUT"):
        try:
            body = await request.json()
        except JSONDecodeError:
            # Fail closed: reject unparsable bodies before any authz decision.
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Request body is not valid JSON",
            )
        raw = body.get("team_name") if isinstance(body, dict) else None
        if raw is not None and not isinstance(raw, str):
            # Fail closed: reject non-string team_name before authz / DB lookup.
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="'team_name' must be a string",
            )
        if raw and not Team.get_name_if_exists(raw):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Team {raw!r} does not exist",
            )
        teams.add(raw)
    return teams


def _requires_access(
    *,
    is_authorized_callback: Callable[[], bool],
) -> None:
    if not is_authorized_callback():
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Forbidden")


def is_safe_url(target_url: str, request: Request | None = None) -> bool:
    """
    Check that the URL is safe.

    Needs to belong to the same domain as base_url, use HTTP or HTTPS (no JavaScript/data schemes),
    is a valid normalized path.
    """
    parsed_bases: tuple[tuple[str, ParseResult], ...] = ()

    # Check if the target URL matches either the configured base URL, or the URL used to make the request
    if request is not None:
        url = str(request.base_url)
        parsed_bases += ((url, urlparse(url)),)
    if base_url := conf.get("api", "base_url", fallback=None):
        parsed_bases += ((base_url, urlparse(base_url)),)

    if not parsed_bases:
        # Can't enforce any security check.
        return True

    # According to WHATWG for http/https /// is interpreted as // whereas urllib doesnt
    # this leads to an inconsistency where python returns a target url with /// as a valid url
    # The same thing also happens with \ where under WHATWG \ are translated to /
    target_url = unquote(target_url).strip()
    if target_url.startswith(("//", "/\\", "\\/", "\\\\")):
        return False
    for base_url, parsed_base in parsed_bases:
        parsed_target = urlparse(urljoin(base_url, unquote(target_url)))  # Resolves relative URLs

        base_path = parsed_base.path or "/"
        target_path = parsed_target.path or "/"

        # Normalize as POSIX paths (URL paths) and ensure target is under base.
        norm_base = posixpath.normpath(base_path)
        norm_target = posixpath.normpath(target_path)

        if norm_base != "/":
            norm_base_with_slash = norm_base if norm_base.endswith("/") else norm_base + "/"
            if norm_target != norm_base and not norm_target.startswith(norm_base_with_slash):
                continue

        if parsed_target.scheme in {"http", "https"} and parsed_target.netloc == parsed_base.netloc:
            return True
    return False


def _get_resource_methods_from_bulk_request(
    action: BulkCreateAction | BulkUpdateAction | BulkDeleteAction,
) -> list[ResourceMethod]:
    resource_methods: list[ResourceMethod] = [MAP_BULK_ACTION_TO_AUTH_METHOD[action.action]]
    # If ``action_on_existence`` == ``overwrite``, we need to check the user has ``PUT`` access as well.
    # With ``action_on_existence`` == ``overwrite``, a create request is actually an update request if the
    # resource already exists, hence adding this check.
    if action.action == BulkAction.CREATE and action.action_on_existence == BulkActionOnExistence.OVERWRITE:
        resource_methods.append("PUT")
    return resource_methods


def _bulk_action_needs_existing_team_lookup(
    action: BulkCreateAction | BulkUpdateAction | BulkDeleteAction,
) -> bool:
    # UPDATE / DELETE always operate on existing resources, so we need the existing team for authz.
    # CREATE with action_on_existence=OVERWRITE may turn into a PUT against an existing resource that
    # belongs to a team; if we omit it from the lookup, the PUT authz check runs with team_name=None
    # and bypasses the per-team membership check that the single-item PUT endpoint enforces.
    if action.action != BulkAction.CREATE:
        return True
    return action.action_on_existence == BulkActionOnExistence.OVERWRITE


def _bulk_action_sets_team(
    action: BulkCreateAction | BulkUpdateAction | BulkDeleteAction,
) -> bool:
    """Return True if this action can write a team_name (UPDATE, or CREATE that carries a body)."""
    return action.action in (BulkAction.UPDATE, BulkAction.CREATE)
