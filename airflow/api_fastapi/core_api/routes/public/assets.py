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

from datetime import datetime
from typing import Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.orm import joinedload, subqueryload

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    OptionalDateTimeQuery,
    QueryAssetAliasNamePatternSearch,
    QueryAssetDagIdPatternSearch,
    QueryAssetNamePatternSearch,
    QueryLimit,
    QueryOffset,
    QueryUriPatternSearch,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import (
    AssetAliasCollectionResponse,
    AssetCollectionResponse,
    AssetEventCollectionResponse,
    AssetEventResponse,
    AssetResponse,
    CreateAssetEventsBody,
    QueuedEventCollectionResponse,
    QueuedEventResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.assets.manager import asset_manager
from airflow.models.asset import AssetAliasModel, AssetDagRunQueue, AssetEvent, AssetModel
from airflow.utils import timezone

assets_router = AirflowRouter(tags=["Asset"])


def _generate_queued_event_where_clause(
    *,
    dag_id: str | None = None,
    uri: str | None = None,
    before: datetime | str | None = None,
) -> list:
    """Get AssetDagRunQueue where clause."""
    where_clause = []
    if dag_id is not None:
        where_clause.append(AssetDagRunQueue.target_dag_id == dag_id)
    if uri is not None:
        where_clause.append(
            AssetDagRunQueue.asset_id.in_(
                select(AssetModel.id).where(AssetModel.uri == uri),
            ),
        )
    if before is not None:
        where_clause.append(AssetDagRunQueue.created_at < before)
    return where_clause


@assets_router.get(
    "/assets",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_assets(
    limit: QueryLimit,
    offset: QueryOffset,
    name_pattern: QueryAssetNamePatternSearch,
    uri_pattern: QueryUriPatternSearch,
    dag_ids: QueryAssetDagIdPatternSearch,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "name", "uri", "created_at", "updated_at"], AssetModel).dynamic_depends()),
    ],
    session: SessionDep,
) -> AssetCollectionResponse:
    """Get assets."""
    assets_select, total_entries = paginated_select(
        statement=select(AssetModel),
        filters=[name_pattern, uri_pattern, dag_ids],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    assets = session.scalars(
        assets_select.options(
            subqueryload(AssetModel.consuming_dags), subqueryload(AssetModel.producing_tasks)
        )
    )
    return AssetCollectionResponse(
        assets=assets,
        total_entries=total_entries,
    )


@assets_router.get(
    "/assets/aliases",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_asset_aliases(
    limit: QueryLimit,
    offset: QueryOffset,
    name_pattern: QueryAssetAliasNamePatternSearch,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "name"], AssetAliasModel).dynamic_depends()),
    ],
    session: SessionDep,
) -> AssetAliasCollectionResponse:
    """Get asset aliases."""
    asset_aliases_select, total_entries = paginated_select(
        statement=select(AssetAliasModel),
        filters=[name_pattern],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    return AssetAliasCollectionResponse(
        asset_aliases=session.scalars(asset_aliases_select),
        total_entries=total_entries,
    )


@assets_router.get(
    "/assets/events",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
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
    asset_id: Annotated[
        FilterParam[int | None], Depends(filter_param_factory(AssetEvent.asset_id, int | None))
    ],
    source_dag_id: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(AssetEvent.source_dag_id, str | None))
    ],
    source_task_id: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(AssetEvent.source_task_id, str | None))
    ],
    source_run_id: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(AssetEvent.source_run_id, str | None))
    ],
    source_map_index: Annotated[
        FilterParam[int | None], Depends(filter_param_factory(AssetEvent.source_map_index, int | None))
    ],
    session: SessionDep,
) -> AssetEventCollectionResponse:
    """Get asset events."""
    assets_event_select, total_entries = paginated_select(
        statement=select(AssetEvent),
        filters=[asset_id, source_dag_id, source_task_id, source_run_id, source_map_index],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    assets_event_select = assets_event_select.options(subqueryload(AssetEvent.created_dagruns))
    assets_events = session.scalars(assets_event_select)

    return AssetEventCollectionResponse(
        asset_events=assets_events,
        total_entries=total_entries,
    )


@assets_router.post(
    "/assets/events",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def create_asset_event(
    body: CreateAssetEventsBody,
    session: SessionDep,
) -> AssetEventResponse:
    """Create asset events."""
    asset_model = session.scalar(select(AssetModel).where(AssetModel.uri == body.uri).limit(1))
    if not asset_model:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Asset with uri: `{body.uri}` was not found")
    timestamp = timezone.utcnow()

    assets_event = asset_manager.register_asset_change(
        asset=asset_model.to_public(),
        timestamp=timestamp,
        extra=body.extra,
        session=session,
    )

    if not assets_event:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Asset with uri: `{body.uri}` was not found")
    return assets_event


@assets_router.get(
    "/assets/queuedEvents/{uri:path}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_asset_queued_events(
    uri: str,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventCollectionResponse:
    """Get queued asset events for an asset."""
    print(f"uri: {uri}")
    where_clause = _generate_queued_event_where_clause(uri=uri, before=before)
    query = (
        select(AssetDagRunQueue, AssetModel.uri)
        .join(AssetModel, AssetDagRunQueue.asset_id == AssetModel.id)
        .where(*where_clause)
    )

    dag_asset_queued_events_select, total_entries = paginated_select(statement=query)
    adrqs = session.execute(dag_asset_queued_events_select).all()

    if not adrqs:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Queue event with uri: `{uri}` was not found")

    queued_events = [
        QueuedEventResponse(created_at=adrq.created_at, dag_id=adrq.target_dag_id, uri=uri)
        for adrq, uri in adrqs
    ]

    return QueuedEventCollectionResponse(
        queued_events=queued_events,
        total_entries=total_entries,
    )


@assets_router.get(
    "/assets/{uri:path}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_asset(
    uri: str,
    session: SessionDep,
) -> AssetResponse:
    """Get an asset."""
    asset = session.scalar(
        select(AssetModel)
        .where(AssetModel.uri == uri)
        .options(joinedload(AssetModel.consuming_dags), joinedload(AssetModel.producing_tasks))
    )

    if asset is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Asset with uri: `{uri}` was not found")

    return AssetResponse.model_validate(asset)


@assets_router.get(
    "/dags/{dag_id}/assets/queuedEvents",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_dag_asset_queued_events(
    dag_id: str,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventCollectionResponse:
    """Get queued asset events for a DAG."""
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, before=before)
    query = (
        select(AssetDagRunQueue, AssetModel.uri)
        .join(AssetModel, AssetDagRunQueue.asset_id == AssetModel.id)
        .where(*where_clause)
    )

    dag_asset_queued_events_select, total_entries = paginated_select(statement=query)
    adrqs = session.execute(dag_asset_queued_events_select).all()
    if not adrqs:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Queue event with dag_id: `{dag_id}` was not found")

    queued_events = [
        QueuedEventResponse(created_at=adrq.created_at, dag_id=adrq.target_dag_id, uri=uri)
        for adrq, uri in adrqs
    ]

    return QueuedEventCollectionResponse(
        queued_events=queued_events,
        total_entries=total_entries,
    )


@assets_router.get(
    "/dags/{dag_id}/assets/queuedEvents/{uri:path}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_dag_asset_queued_event(
    dag_id: str,
    uri: str,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventResponse:
    """Get a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, uri=uri, before=before)
    query = (
        select(AssetDagRunQueue)
        .join(AssetModel, AssetDagRunQueue.asset_id == AssetModel.id)
        .where(*where_clause)
    )
    adrq = session.scalar(query)
    if not adrq:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Queued event with dag_id: `{dag_id}` and asset uri: `{uri}` was not found",
        )

    return QueuedEventResponse(created_at=adrq.created_at, dag_id=adrq.target_dag_id, uri=uri)


@assets_router.delete(
    "/assets/queuedEvents/{uri:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def delete_asset_queued_events(
    uri: str,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    """Delete queued asset events for an asset."""
    where_clause = _generate_queued_event_where_clause(uri=uri, before=before)
    delete_stmt = delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")
    result = session.execute(delete_stmt)
    if result.rowcount == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Queue event with uri: `{uri}` was not found")


@assets_router.delete(
    "/dags/{dag_id}/assets/queuedEvents",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def delete_dag_asset_queued_events(
    dag_id: str,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, before=before)

    delete_statement = delete(AssetDagRunQueue).where(*where_clause)
    result = session.execute(delete_statement)

    if result.rowcount == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Queue event with dag_id: `{dag_id}` was not found")


@assets_router.delete(
    "/dags/{dag_id}/assets/queuedEvents/{uri:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def delete_dag_asset_queued_event(
    dag_id: str,
    uri: str,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    """Delete a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(dag_id=dag_id, before=before, uri=uri)
    delete_statement = (
        delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")
    )
    result = session.execute(delete_statement)
    if result.rowcount == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Queued event with dag_id: `{dag_id}` and asset uri: `{uri}` was not found",
        )
