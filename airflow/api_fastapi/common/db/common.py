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
Database helpers for Airflow REST API.

:meta private:
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal, overload

from sqlalchemy.ext.asyncio import AsyncSession

from airflow.utils.db import get_query_count, get_query_count_async
from airflow.utils.session import NEW_SESSION, create_session, create_session_async, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.common.parameters import BaseParam


def get_session() -> Session:
    """
    Dependency for providing a session.

    For non route function please use the :class:`airflow.utils.session.provide_session` decorator.

    Example usage:

    .. code:: python

        @router.get("/your_path")
        def your_route(session: Annotated[Session, Depends(get_session)]):
            pass
    """
    with create_session(scoped=False) as session:
        yield session


def apply_filters_to_select(*, query: Select, filters: Sequence[BaseParam | None] | None = None) -> Select:
    if filters is None:
        return query
    for f in filters:
        if f is None:
            continue
        query = f.to_orm(query)

    return query


async def get_async_session() -> AsyncSession:
    """
    Dependency for providing a session.

    Example usage:

    .. code:: python

        @router.get("/your_path")
        def your_route(session: Annotated[AsyncSession, Depends(get_async_session)]):
            pass
    """
    async with create_session_async() as session:
        yield session


@overload
async def paginated_select_async(
    *,
    query: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: AsyncSession,
    return_total_entries: Literal[True] = True,
) -> tuple[Select, int]: ...


@overload
async def paginated_select_async(
    *,
    query: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: AsyncSession,
    return_total_entries: Literal[False],
) -> tuple[Select, None]: ...


async def paginated_select_async(
    *,
    query: Select,
    filters: Sequence[BaseParam | None] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: AsyncSession,
    return_total_entries: bool = True,
) -> tuple[Select, int | None]:
    query = apply_filters_to_select(
        base_select=query,
        filters=filters,
    )

    total_entries = None
    if return_total_entries:
        total_entries = await get_query_count_async(query, session=session)

    # TODO: Re-enable when permissions are handled. Readable / writable entities,
    # for instance:
    # readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)
    # dags_select = dags_select.where(DagModel.dag_id.in_(readable_dags))

    query = apply_filters_to_select(
        base_select=query,
        filters=[order_by, offset, limit],
    )

    return query, total_entries


@overload
def paginated_select(
    *,
    query: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: Literal[True] = True,
) -> tuple[Select, int]: ...


@overload
def paginated_select(
    *,
    query: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: Literal[False],
) -> tuple[Select, None]: ...


@provide_session
def paginated_select(
    *,
    query: Select,
    filters: Sequence[BaseParam] | None = None,
    order_by: BaseParam | None = None,
    offset: BaseParam | None = None,
    limit: BaseParam | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: bool = True,
) -> tuple[Select, int | None]:
    query = apply_filters_to_select(
        query=query,
        filters=filters,
    )

    total_entries = None
    if return_total_entries:
        total_entries = get_query_count(query, session=session)

    # TODO: Re-enable when permissions are handled. Readable / writable entities,
    # for instance:
    # readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)
    # dags_select = dags_select.where(DagModel.dag_id.in_(readable_dags))

    query = apply_filters_to_select(query=query, filters=[order_by, offset, limit])

    return query, total_entries
