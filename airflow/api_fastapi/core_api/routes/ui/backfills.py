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

from typing import Annotated

from fastapi import Depends, status
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from airflow.api_fastapi.common.db.common import get_async_session, paginated_select_async
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.backfills import BackfillCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import (
    create_openapi_http_exception_doc,
)
from airflow.models.backfill import Backfill

backfills_router = AirflowRouter(tags=["Backfill"], prefix="/backfills")


@backfills_router.get(
    path="",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
async def list_backfills(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id"], Backfill).dynamic_depends()),
    ],
    session: Annotated[AsyncSession, Depends(get_async_session)],
    dag_id: str | None = None,
) -> BackfillCollectionResponse:
    print("dag_id")
    conditions = [Backfill.completed_at.is_(None)]  # Active dag
    if dag_id:
        conditions.append(Backfill.dag_id == dag_id)
    print(dag_id, str(conditions))

    select_stmt, total_entries = await paginated_select_async(
        statement=select(Backfill).where(and_(*conditions)),
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    print(select_stmt)
    backfills = await session.scalars(select_stmt)
    return BackfillCollectionResponse(
        backfills=backfills,
        total_entries=total_entries,
    )
