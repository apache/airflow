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
from typing import TYPE_CHECKING, Annotated, Literal, overload

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from airflow.utils.db import get_query_count, get_query_count_async
from airflow.utils.session import NEW_SESSION, create_session, create_session_async, provide_session

if TYPE_CHECKING:
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.core_api.base import OrmClause


def _get_session() -> Session:
    with create_session(scoped=False) as session:
        yield session


SessionDep = Annotated[Session, Depends(_get_session)]


def apply_filters_to_select(
    *, statement: Select, filters: Sequence[OrmClause | None] | None = None
) -> Select:
    if filters is None:
        return statement
    for f in filters:
        if f is None:
            continue
        statement = f.to_orm(statement)

    return statement


async def _get_async_session() -> AsyncSession:
    async with create_session_async() as session:
        yield session


AsyncSessionDep = Annotated[AsyncSession, Depends(_get_async_session)]


@overload
async def paginated_select_async(
    *,
    statement: Select,
    filters: Sequence[OrmClause] | None = None,
    order_by: OrmClause | None = None,
    offset: OrmClause | None = None,
    limit: OrmClause | None = None,
    session: AsyncSession,
    return_total_entries: Literal[True] = True,
) -> tuple[Select, int]: ...


@overload
async def paginated_select_async(
    *,
    statement: Select,
    filters: Sequence[OrmClause] | None = None,
    order_by: OrmClause | None = None,
    offset: OrmClause | None = None,
    limit: OrmClause | None = None,
    session: AsyncSession,
    return_total_entries: Literal[False],
) -> tuple[Select, None]: ...


async def paginated_select_async(
    *,
    statement: Select,
    filters: Sequence[OrmClause | None] | None = None,
    order_by: OrmClause | None = None,
    offset: OrmClause | None = None,
    limit: OrmClause | None = None,
    session: AsyncSession,
    return_total_entries: bool = True,
) -> tuple[Select, int | None]:
    statement = apply_filters_to_select(
        statement=statement,
        filters=filters,
    )

    total_entries = None
    if return_total_entries:
        total_entries = await get_query_count_async(statement, session=session)

    # TODO: Re-enable when permissions are handled. Readable / writable entities,
    # for instance:
    # readable_dags = get_auth_manager().get_authorized_dag_ids(user=g.user)
    # dags_select = dags_select.where(DagModel.dag_id.in_(readable_dags))

    statement = apply_filters_to_select(
        statement=statement,
        filters=[order_by, offset, limit],
    )

    return statement, total_entries


@overload
def paginated_select(
    *,
    statement: Select,
    filters: Sequence[OrmClause] | None = None,
    order_by: OrmClause | None = None,
    offset: OrmClause | None = None,
    limit: OrmClause | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: Literal[True] = True,
) -> tuple[Select, int]: ...


@overload
def paginated_select(
    *,
    statement: Select,
    filters: Sequence[OrmClause] | None = None,
    order_by: OrmClause | None = None,
    offset: OrmClause | None = None,
    limit: OrmClause | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: Literal[False],
) -> tuple[Select, None]: ...


@provide_session
def paginated_select(
    *,
    statement: Select,
    filters: Sequence[OrmClause] | None = None,
    order_by: OrmClause | None = None,
    offset: OrmClause | None = None,
    limit: OrmClause | None = None,
    session: Session = NEW_SESSION,
    return_total_entries: bool = True,
) -> tuple[Select, int | None]:
    statement = apply_filters_to_select(
        statement=statement,
        filters=filters,
    )

    total_entries = None
    if return_total_entries:
        total_entries = get_query_count(statement, session=session)

    # TODO: Re-enable when permissions are handled. Readable / writable entities,
    # for instance:
    # readable_dags = get_auth_manager().get_authorized_dag_ids(user=g.user)
    # dags_select = dags_select.where(DagModel.dag_id.in_(readable_dags))

    statement = apply_filters_to_select(statement=statement, filters=[order_by, offset, limit])

    return statement, total_entries
