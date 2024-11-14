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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload, subqueryload

from airflow.api_fastapi.common.db.common import get_session, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryAssetDagIdPatternSearch,
    QueryAssetIdFilter,
    QueryLimit,
    QueryOffset,
    QuerySourceDagIdFilter,
    QuerySourceMapIndexFilter,
    QuerySourceRunIdFilter,
    QuerySourceTaskIdFilter,
    QueryUriPatternSearch,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import (
    AssetCollectionResponse,
    AssetEventCollectionResponse,
    AssetEventResponse,
    AssetResponse,
    CreateAssetEventsBody,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.assets import Asset
from airflow.assets.manager import asset_manager
from airflow.models.asset import AssetEvent, AssetModel
from airflow.utils import timezone

assets_router = AirflowRouter(tags=["Asset"], prefix="/assets")


@assets_router.get(
    "/",
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
def get_assets(
    limit: QueryLimit,
    offset: QueryOffset,
    uri_pattern: QueryUriPatternSearch,
    dag_ids: QueryAssetDagIdPatternSearch,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "uri", "created_at", "updated_at"], AssetModel).dynamic_depends()),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> AssetCollectionResponse:
    """Get assets."""
    assets_select, total_entries = paginated_select(
        select(AssetModel),
        filters=[uri_pattern, dag_ids],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    assets = session.scalars(
        assets_select.options(
            subqueryload(AssetModel.consuming_dags), subqueryload(AssetModel.producing_tasks)
        )
    ).all()
    return AssetCollectionResponse(
        assets=[AssetResponse.model_validate(asset, from_attributes=True) for asset in assets],
        total_entries=total_entries,
    )


@assets_router.get(
    "/events",
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
def get_asset_events(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "source_task_id",
                    "source_dag_id",
                    "source_run_id",
                    "source_map_index",
                    "timestamp",
                ],
                AssetEvent,
            ).dynamic_depends("timestamp")
        ),
    ],
    asset_id: QueryAssetIdFilter,
    source_dag_id: QuerySourceDagIdFilter,
    source_task_id: QuerySourceTaskIdFilter,
    source_run_id: QuerySourceRunIdFilter,
    source_map_index: QuerySourceMapIndexFilter,
    session: Annotated[Session, Depends(get_session)],
) -> AssetEventCollectionResponse:
    """Get asset events."""
    assets_event_select, total_entries = paginated_select(
        select(AssetEvent),
        filters=[asset_id, source_dag_id, source_task_id, source_run_id, source_map_index],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    assets_event_select = assets_event_select.options(subqueryload(AssetEvent.created_dagruns))
    assets_events = session.scalars(assets_event_select).all()

    return AssetEventCollectionResponse(
        asset_events=[
            AssetEventResponse.model_validate(asset, from_attributes=True) for asset in assets_events
        ],
        total_entries=total_entries,
    )


@assets_router.post(
    "/events",
    responses=create_openapi_http_exception_doc([404]),
)
def create_asset_events(
    body: CreateAssetEventsBody,
    session: Annotated[Session, Depends(get_session)],
) -> AssetEventResponse:
    """Create asset events."""
    asset = session.scalar(select(AssetModel).where(AssetModel.uri == body.uri).limit(1))
    if not asset:
        raise HTTPException(404, f"Asset with uri: `{body.uri}` was not found")
    timestamp = timezone.utcnow()

    assets_event = asset_manager.register_asset_change(
        asset=Asset(uri=body.uri),
        timestamp=timestamp,
        extra=body.extra,
        session=session,
    )

    if not assets_event:
        raise HTTPException(404, f"Asset with uri: `{body.uri}` was not found")
    return AssetEventResponse.model_validate(assets_event, from_attributes=True)


@assets_router.get(
    "/{uri:path}",
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
def get_asset(
    uri: str,
    session: Annotated[Session, Depends(get_session)],
) -> AssetResponse:
    """Get an asset."""
    asset = session.scalar(
        select(AssetModel)
        .where(AssetModel.uri == uri)
        .options(joinedload(AssetModel.consuming_dags), joinedload(AssetModel.producing_tasks))
    )

    if asset is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Asset with uri: `{uri}` was not found")

    return AssetResponse.model_validate(asset, from_attributes=True)
