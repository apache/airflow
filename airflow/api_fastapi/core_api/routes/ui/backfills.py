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
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.backfills import BackfillCollectionResponse
from airflow.api_fastapi.core_api.openapi.exceptions import (
    create_openapi_http_exception_doc,
)
from airflow.api_fastapi.core_api.security import requires_access_backfill, requires_access_dag
from airflow.models.backfill import Backfill

backfills_router = AirflowRouter(tags=["Backfill"], prefix="/backfills")


@backfills_router.get(
    path="",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_backfill(method="GET")),
        Depends(requires_access_dag(method="GET")),
    ],
)
def list_backfills(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id"], Backfill).dynamic_depends()),
    ],
    session: SessionDep,
    dag_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(Backfill.dag_id, str | None))],
    active: Annotated[
        FilterParam[bool | None],
        Depends(filter_param_factory(Backfill.completed_at, bool | None, FilterOptionEnum.IS_NONE, "active")),
    ],
) -> BackfillCollectionResponse:
    select_stmt, total_entries = paginated_select(
        statement=select(Backfill),
        filters=[dag_id, active],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    backfills = session.scalars(select_stmt)
    return BackfillCollectionResponse(
        backfills=backfills,
        total_entries=total_entries,
    )
