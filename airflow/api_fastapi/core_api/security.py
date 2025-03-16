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

from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Callable
from urllib.parse import urljoin, urlparse

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from jwt import ExpiredSignatureError, InvalidTokenError

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
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
from airflow.configuration import conf
from airflow.models.dag import DagModel, DagRun, DagTag
from airflow.models.dagwarning import DagWarning
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.xcom import XCom

if TYPE_CHECKING:
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def get_user(token_str: Annotated[str, Depends(oauth2_scheme)]) -> BaseUser:
    try:
        return await get_auth_manager().get_user_from_token(token_str)
    except ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token Expired")
    except InvalidTokenError:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Invalid JWT token")


GetUserDep = Annotated[BaseUser, Depends(get_user)]


async def get_user_with_exception_handling(request: Request) -> BaseUser | None:
    # Currently the UI does not support JWT authentication, this method defines a fallback if no token is provided by the UI.
    # We can remove this method when issue https://github.com/apache/airflow/issues/44884 is done.
    token_str = None

    # TODO remove try-except when authentication integrated everywhere, safeguard for non integrated clients and endpoints
    try:
        token_str = await oauth2_scheme(request)
    except HTTPException as e:
        if e.status_code == status.HTTP_401_UNAUTHORIZED:
            return None

    if not token_str:  # Handle None or empty token
        return None
    return await get_user(token_str)


def requires_access_dag(
    method: ResourceMethod, access_entity: DagAccessEntity | None = None
) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        dag_id: str | None = request.path_params.get("dag_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_dag(
                method=method, access_entity=access_entity, details=DagDetails(id=dag_id), user=user
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
        return select.where(XCom.dag_id.in_(self.value))


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


def requires_access_backfill(method: ResourceMethod) -> Callable:
    def inner(
        request: Request,
        user: Annotated[BaseUser | None, Depends(get_user)] = None,
    ) -> None:
        backfill_id: str | None = request.path_params.get("backfill_id")

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

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_pool(
                method=method, details=PoolDetails(name=pool_name), user=user
            )
        )

    return inner


def requires_access_connection(method: ResourceMethod) -> Callable[[Request, BaseUser], None]:
    def inner(
        request: Request,
        user: GetUserDep,
    ) -> None:
        connection_id = request.path_params.get("connection_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_connection(
                method=method, details=ConnectionDetails(conn_id=connection_id), user=user
            )
        )

    return inner


def requires_access_configuration(method: ResourceMethod) -> Callable[[Request, BaseUser | None], None]:
    def inner(
        request: Request,
        user: Annotated[BaseUser | None, Depends(get_user)] = None,
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


def requires_access_variable(method: ResourceMethod) -> Callable[[Request, BaseUser | None], None]:
    def inner(
        request: Request,
        user: Annotated[BaseUser | None, Depends(get_user)] = None,
    ) -> None:
        variable_key: str | None = request.path_params.get("variable_key")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_variable(
                method=method, details=VariableDetails(key=variable_key), user=user
            ),
        )

    return inner


def requires_access_asset(method: ResourceMethod) -> Callable:
    def inner(
        request: Request,
        user: Annotated[BaseUser | None, Depends(get_user)] = None,
    ) -> None:
        asset_id = request.path_params.get("asset_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_asset(
                method=method, details=AssetDetails(id=asset_id), user=user
            ),
        )

    return inner


def requires_access_view(access_view: AccessView) -> Callable[[Request, BaseUser | None], None]:
    def inner(
        request: Request,
        user: Annotated[BaseUser | None, Depends(get_user)] = None,
    ) -> None:
        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_view(
                access_view=access_view, user=user
            ),
        )

    return inner


def requires_access_asset_alias(method: ResourceMethod) -> Callable:
    def inner(
        request: Request,
        user: Annotated[BaseUser | None, Depends(get_user)] = None,
    ) -> None:
        asset_alias_id: str | None = request.path_params.get("asset_alias_id")

        _requires_access(
            is_authorized_callback=lambda: get_auth_manager().is_authorized_asset_alias(
                method=method, details=AssetAliasDetails(id=asset_alias_id), user=user
            ),
        )

    return inner


def _requires_access(
    *,
    is_authorized_callback: Callable[[], bool],
) -> None:
    if not is_authorized_callback():
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Forbidden")


def is_safe_url(target_url: str) -> bool:
    """
    Check that the URL is safe.

    Needs to belong to the same domain as base_url, use HTTP or HTTPS (no JavaScript/data schemes),
    is a valid normalized path.
    """
    base_url = conf.get("api", "base_url")

    parsed_base = urlparse(base_url)
    parsed_target = urlparse(urljoin(base_url, target_url))  # Resolves relative URLs

    target_path = Path(parsed_target.path).resolve()

    if target_path and parsed_base.path and not target_path.is_relative_to(parsed_base.path):
        return False

    return parsed_target.scheme in {"http", "https"} and parsed_target.netloc == parsed_base.netloc
