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

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, cast
from urllib.parse import ParseResult, unquote, urljoin, urlparse

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, OAuth2PasswordBearer
from jwt import ExpiredSignatureError, InvalidTokenError
from pydantic import NonNegativeInt

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.auth.managers.models.batch_apis import (
    IsAuthorizedConnectionRequest,
    IsAuthorizedPoolRequest,
    IsAuthorizedVariableRequest,
)
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    AssetAliasDetails,
    AssetDetails,
    BackfillDetails,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
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
from airflow.api_fastapi.core_api.datamodels.pools import PoolBody
from airflow.api_fastapi.core_api.datamodels.variables import VariableBody
from airflow.configuration import conf
from airflow.models import Connection, Pool, Variable
from airflow.models.dag import DagModel, DagRun, DagTag
from airflow.models.dagwarning import DagWarning
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.xcom import XComModel

if TYPE_CHECKING:
    from fastapi.security import HTTPAuthorizationCredentials
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod

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


async def get_user(
    request: Request,
    oauth_token: str | None = Depends(oauth2_scheme),
    bearer_credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> BaseUser:
    # A user might have been already built by a middleware, if so, it is stored in `request.state.user`
    user: BaseUser | None = getattr(request.state, "user", None)
    if user:
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
    method: ResourceMethod, access_entity: DagAccessEntity | None = None
) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        dag_id: str | None = request.path_params.get("dag_id")
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


class PermittedDagFilter(OrmClause[set[str]]):
    """A parameter that filters the permitted dags for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagModel.dag_id.in_(self.value))


class PermittedDagRunFilter(PermittedDagFilter):
    """A parameter that filters the permitted dag runs for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagRun.dag_id.in_(self.value))


class PermittedDagWarningFilter(PermittedDagFilter):
    """A parameter that filters the permitted dag warnings for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagWarning.dag_id.in_(self.value))


class PermittedTIFilter(PermittedDagFilter):
    """A parameter that filters the permitted task instances for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(TI.dag_id.in_(self.value))


class PermittedXComFilter(PermittedDagFilter):
    """A parameter that filters the permitted XComs for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(XComModel.dag_id.in_(self.value))


class PermittedTagFilter(PermittedDagFilter):
    """A parameter that filters the permitted dag tags for the user."""

    def to_orm(self, select: Select) -> Select:
        return select.where(DagTag.dag_id.in_(self.value))


def permitted_dag_filter_factory(
    method: ResourceMethod, filter_class=PermittedDagFilter
) -> Callable[[Request, BaseUser], PermittedDagFilter]:
    """
    Create a callable for Depends in FastAPI that returns a filter of the permitted dags for the user.

    :param method: whether filter readable or writable.
    :return: The callable that can be used as Depends in FastAPI.
    """

    def depends_permitted_dags_filter(
        request: Request,
        user: GetUserDep,
    ) -> PermittedDagFilter:
        auth_manager: BaseAuthManager = request.app.state.auth_manager
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
ReadableXComFilterDep = Annotated[
    PermittedXComFilter, Depends(permitted_dag_filter_factory("GET", PermittedXComFilter))
]

ReadableTagsFilterDep = Annotated[
    PermittedTagFilter, Depends(permitted_dag_filter_factory("GET", PermittedTagFilter))
]


def requires_access_backfill(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        backfill_id: NonNegativeInt | None = request.path_params.get("backfill_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_backfill(
                method=method, details=BackfillDetails(id=backfill_id), user=user
            ),
        )

    return inner


def requires_access_pool(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        pool_name = request.path_params.get("pool_name")
        team_name = Pool.get_team_name(pool_name) if pool_name else None

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_pool(
                method=method, details=PoolDetails(name=pool_name, team_name=team_name), user=user
            )
        )

    return inner


def requires_access_pool_bulk() -> Callable[[BulkBody[PoolBody], BaseUser], None]:
    def inner(
        request: BulkBody[PoolBody],
        user: GetUserDep,
    ) -> None:
        requests: list[IsAuthorizedPoolRequest] = []
        for action in request.actions:
            methods = _get_resource_methods_from_bulk_request(action)
            for pool in action.entities:
                pool_name = (
                    cast("str", pool) if action.action == BulkAction.DELETE else cast("PoolBody", pool).pool
                )
                # For each pool, build a `IsAuthorizedPoolRequest`
                # The list of `IsAuthorizedPoolRequest` will then be sent using `batch_is_authorized_pool`
                # Each `IsAuthorizedPoolRequest` is similar to calling `is_authorized_pool`
                for method in methods:
                    req: IsAuthorizedPoolRequest = {
                        "method": method,
                        "details": PoolDetails(
                            name=pool_name,
                        ),
                    }
                    requests.append(req)

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_pool(
                requests=requests,
                user=user,
            )
        )

    return inner


def requires_access_connection(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        connection_id = request.path_params.get("connection_id")
        team_name = Connection.get_team_name(connection_id) if connection_id else None

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_connection(
                method=method,
                details=ConnectionDetails(conn_id=connection_id, team_name=team_name),
                user=user,
            )
        )

    return inner


def requires_access_connection_bulk() -> Callable[[BulkBody[ConnectionBody], BaseUser], None]:
    def inner(
        request: BulkBody[ConnectionBody],
        user: GetUserDep,
    ) -> None:
        requests: list[IsAuthorizedConnectionRequest] = []
        for action in request.actions:
            methods = _get_resource_methods_from_bulk_request(action)
            for connection in action.entities:
                connection_id = (
                    cast("str", connection)
                    if action.action == BulkAction.DELETE
                    else cast("ConnectionBody", connection).connection_id
                )
                # For each pool, build a `IsAuthorizedConnectionRequest`
                # The list of `IsAuthorizedConnectionRequest` will then be sent using `batch_is_authorized_connection`
                # Each `IsAuthorizedConnectionRequest` is similar to calling `is_authorized_connection`
                for method in methods:
                    req: IsAuthorizedConnectionRequest = {
                        "method": method,
                        "details": ConnectionDetails(
                            conn_id=connection_id,
                        ),
                    }
                    requests.append(req)

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


def requires_access_variable(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        variable_key: str | None = request.path_params.get("variable_key")
        team_name = Variable.get_team_name(variable_key) if variable_key else None

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_variable(
                method=method, details=VariableDetails(key=variable_key, team_name=team_name), user=user
            ),
        )

    return inner


def requires_access_variable_bulk() -> Callable[[BulkBody[VariableBody], BaseUser], None]:
    def inner(
        request: BulkBody[VariableBody],
        user: GetUserDep,
    ) -> None:
        requests: list[IsAuthorizedVariableRequest] = []
        for action in request.actions:
            methods = _get_resource_methods_from_bulk_request(action)
            for variable in action.entities:
                variable_key = (
                    cast("str", variable)
                    if action.action == BulkAction.DELETE
                    else cast("VariableBody", variable).key
                )
                # For each variable, build a `IsAuthorizedVariableRequest`
                # The list of `IsAuthorizedVariableRequest` will then be sent using `batch_is_authorized_variable`
                # Each `IsAuthorizedVariableRequest` is similar to calling `is_authorized_variable`
                for method in methods:
                    req: IsAuthorizedVariableRequest = {
                        "method": method,
                        "details": VariableDetails(
                            key=variable_key,
                        ),
                    }
                    requests.append(req)

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().batch_is_authorized_variable(
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

    for base_url, parsed_base in parsed_bases:
        parsed_target = urlparse(urljoin(base_url, unquote(target_url)))  # Resolves relative URLs

        target_path = Path(parsed_target.path).resolve()

        if target_path and parsed_base.path and not target_path.is_relative_to(parsed_base.path):
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
