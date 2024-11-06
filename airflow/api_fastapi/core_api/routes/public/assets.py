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

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.db.common import get_session, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryDagIdsFilter,
    QueryLimit,
    QueryOffset,
    QueryUriPatternSearch,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.assets import AssetCollectionResponse, AssetResponse
from airflow.assets import Asset
from airflow.models.asset import AssetModel

assets_router = AirflowRouter(tags=["Asset"], prefix="/assets")


@assets_router.get(
    "/",
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
async def get_assets(
    limit: QueryLimit,
    offset: QueryOffset,
    uri_pattern: QueryUriPatternSearch,
    dag_ids: QueryDagIdsFilter,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "uri", "created_at", "updated_at"], Asset).dynamic_depends()),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> AssetCollectionResponse:
    """Get assets."""
    assets_select, total_entries = paginated_select(
        select(AssetModel),
        filters=[dag_ids, uri_pattern],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    assets = session.scalars(assets_select).all()
    return AssetCollectionResponse(
        assets=[AssetResponse.model_validate(x, from_attributes=True) for x in assets],
        total_entries=total_entries,
    )
