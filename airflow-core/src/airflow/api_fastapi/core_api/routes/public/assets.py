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
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.orm import joinedload, subqueryload

from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    BaseParam,
    FilterParam,
    OptionalDateTimeQuery,
    QueryAssetAliasNamePatternSearch,
    QueryAssetDagIdPatternSearch,
    QueryAssetNamePatternSearch,
    QueryLimit,
    QueryOffset,
    QueryUriPatternSearch,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import (
    AssetAliasCollectionResponse,
    AssetAliasResponse,
    AssetCollectionResponse,
    AssetEventCollectionResponse,
    AssetEventResponse,
    AssetResponse,
    CreateAssetEventsBody,
    QueuedEventCollectionResponse,
    QueuedEventResponse,
)
from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    ReadableDagsFilterDep,
    requires_access_asset,
    requires_access_asset_alias,
    requires_access_dag,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.assets.manager import asset_manager
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    TaskOutletAssetReference,
)
from airflow.models.dag import DAG
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from sqlalchemy.sql import Select

assets_router = AirflowRouter(tags=["Asset"])


def _generate_queued_event_where_clause(
    *,
    asset_id: int | None = None,
    dag_id: str | None = None,
    before: datetime | str | None = None,
    permitted_dag_ids: set[str] | None = None,
) -> list:
    """Get AssetDagRunQueue where clause."""
    where_clause = []
    if dag_id is not None:
        where_clause.append(AssetDagRunQueue.target_dag_id == dag_id)
    if asset_id is not None:
        where_clause.append(AssetDagRunQueue.asset_id == asset_id)
    if before is not None:
        where_clause.append(AssetDagRunQueue.created_at < before)
    if permitted_dag_ids is not None:
        where_clause.append(AssetDagRunQueue.target_dag_id.in_(permitted_dag_ids))
    return where_clause


class OnlyActiveFilter(BaseParam[bool]):
    """Filter on asset activeness."""

    def to_orm(self, select: Select) -> Select:
        if self.value and self.skip_none:
            return select.where(AssetModel.active.has())
        return select

    @classmethod
    def depends(cls, only_active: bool = True) -> OnlyActiveFilter:
        return cls().set_value(only_active)


@assets_router.get(
    "/assets",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
        Depends(requires_access_asset_alias(method="GET")),
    ],
)
def get_assets(
    limit: QueryLimit,
    offset: QueryOffset,
    name_pattern: QueryAssetNamePatternSearch,
    uri_pattern: QueryUriPatternSearch,
    dag_ids: QueryAssetDagIdPatternSearch,
    only_active: Annotated[OnlyActiveFilter, Depends(OnlyActiveFilter.depends)],
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "name", "uri", "created_at", "updated_at"], AssetModel).dynamic_depends()),
    ],
    session: SessionDep,
) -> AssetCollectionResponse:
    """Get assets."""
    assets_select, total_entries = paginated_select(
        statement=select(AssetModel),
        filters=[only_active, name_pattern, uri_pattern, dag_ids],
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
    dependencies=[Depends(requires_access_asset_alias(method="GET"))],
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
    "/assets/aliases/{asset_alias_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset_alias(method="GET"))],
)
def get_asset_alias(asset_alias_id: int, session: SessionDep):
    """Get an asset alias."""
    alias = session.scalar(select(AssetAliasModel).where(AssetAliasModel.id == asset_alias_id))
    if alias is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Asset Alias with ID: `{asset_alias_id}` was not found",
        )
    return AssetAliasResponse.model_validate(alias)


@assets_router.get(
    "/assets/events",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
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
    timestamp_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("timestamp", AssetEvent))],
    session: SessionDep,
) -> AssetEventCollectionResponse:
    """Get asset events."""
    assets_event_select, total_entries = paginated_select(
        statement=select(AssetEvent),
        filters=[asset_id, source_dag_id, source_task_id, source_run_id, source_map_index, timestamp_range],
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
    dependencies=[Depends(requires_access_asset(method="POST")), Depends(action_logging())],
)
def create_asset_event(
    body: CreateAssetEventsBody,
    session: SessionDep,
) -> AssetEventResponse:
    """Create asset events."""
    asset_model = session.scalar(select(AssetModel).where(AssetModel.id == body.asset_id).limit(1))
    if not asset_model:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Asset with ID: `{body.asset_id}` was not found")
    timestamp = timezone.utcnow()

    assets_event = asset_manager.register_asset_change(
        asset=asset_model,
        timestamp=timestamp,
        extra=body.extra,
        session=session,
    )

    if not assets_event:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Asset with ID: `{body.asset_id}` was not found")
    return AssetEventResponse.model_validate(assets_event)


@assets_router.post(
    "/assets/{asset_id}/materialize",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]),
    dependencies=[Depends(requires_access_asset(method="POST")), Depends(action_logging())],
)
def materialize_asset(
    asset_id: int,
    dag_bag: DagBagDep,
    session: SessionDep,
) -> DAGRunResponse:
    """Materialize an asset by triggering a DAG run that produces it."""
    dag_id_it = iter(
        session.scalars(
            select(TaskOutletAssetReference.dag_id)
            .where(TaskOutletAssetReference.asset_id == asset_id)
            .group_by(TaskOutletAssetReference.dag_id)
            .limit(2)
        )
    )

    if (dag_id := next(dag_id_it, None)) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No DAG materializes asset with ID: {asset_id}")
    if next(dag_id_it, None) is not None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"More than one DAG materializes asset with ID: {asset_id}",
        )

    dag: DAG | None
    if not (dag := dag_bag.get_dag(dag_id)):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"DAG with ID `{dag_id}` was not found")

    return dag.create_dagrun(
        run_id=dag.timetable.generate_run_id(
            run_type=DagRunType.MANUAL,
            run_after=(run_after := timezone.coerce_datetime(timezone.utcnow())),
            data_interval=None,
        ),
        run_after=run_after,
        run_type=DagRunType.MANUAL,
        triggered_by=DagRunTriggeredByType.REST_API,
        state=DagRunState.QUEUED,
        session=session,
    )


@assets_router.get(
    "/assets/{asset_id}/queuedEvents",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_asset_queued_events(
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventCollectionResponse:
    """Get queued asset events for an asset."""
    where_clause = _generate_queued_event_where_clause(
        asset_id=asset_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    query = select(AssetDagRunQueue).where(*where_clause)

    dag_asset_queued_events_select, total_entries = paginated_select(statement=query)
    adrqs = session.scalars(dag_asset_queued_events_select).all()

    if not adrqs:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Queue event with asset_id: `{asset_id}` was not found",
        )

    queued_events = [
        QueuedEventResponse(created_at=adrq.created_at, dag_id=adrq.target_dag_id, asset_id=adrq.asset_id)
        for adrq in adrqs
    ]

    return QueuedEventCollectionResponse(
        queued_events=queued_events,
        total_entries=total_entries,
    )


@assets_router.get(
    "/assets/{asset_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
        Depends(requires_access_asset_alias(method="GET")),
    ],
)
def get_asset(
    asset_id: int,
    session: SessionDep,
) -> AssetResponse:
    """Get an asset."""
    asset = session.scalar(
        select(AssetModel)
        .where(AssetModel.id == asset_id)
        .options(joinedload(AssetModel.consuming_dags), joinedload(AssetModel.producing_tasks))
    )

    if asset is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Asset with ID: `{asset_id}` was not found")

    return AssetResponse.model_validate(asset)


@assets_router.get(
    "/dags/{dag_id}/assets/queuedEvents",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
def get_dag_asset_queued_events(
    dag_id: str,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventCollectionResponse:
    """Get queued asset events for a DAG."""
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    query = select(AssetDagRunQueue).where(*where_clause)

    dag_asset_queued_events_select, total_entries = paginated_select(statement=query)
    adrqs = session.scalars(dag_asset_queued_events_select).all()
    if not adrqs:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Queue event with dag_id: `{dag_id}` was not found")

    queued_events = [
        QueuedEventResponse(created_at=adrq.created_at, dag_id=adrq.target_dag_id, asset_id=adrq.asset_id)
        for adrq in adrqs
    ]

    return QueuedEventCollectionResponse(
        queued_events=queued_events,
        total_entries=total_entries,
    )


@assets_router.get(
    "/dags/{dag_id}/assets/{asset_id}/queuedEvents",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
def get_dag_asset_queued_event(
    dag_id: str,
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
) -> QueuedEventResponse:
    """Get a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, asset_id=asset_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    query = select(AssetDagRunQueue).where(*where_clause)
    adrq = session.scalar(query)
    if not adrq:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Queued event with dag_id: `{dag_id}` and asset_id: `{asset_id}` was not found",
        )

    return QueuedEventResponse(created_at=adrq.created_at, dag_id=adrq.target_dag_id, asset_id=asset_id)


@assets_router.delete(
    "/assets/{asset_id}/queuedEvents",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_asset(method="DELETE")),
        Depends(requires_access_dag(method="GET")),
        Depends(action_logging()),
    ],
)
def delete_asset_queued_events(
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    """Delete queued asset events for an asset."""
    where_clause = _generate_queued_event_where_clause(
        asset_id=asset_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )
    delete_stmt = delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")
    result = session.execute(delete_stmt)
    if result.rowcount == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Queue event with asset_id: `{asset_id}` was not found",
        )


@assets_router.delete(
    "/dags/{dag_id}/assets/queuedEvents",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_access_asset(method="DELETE")),
        Depends(requires_access_dag(method="GET")),
        Depends(action_logging()),
    ],
)
def delete_dag_asset_queued_events(
    dag_id: str,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, before=before, permitted_dag_ids=readable_dags_filter.value
    )

    delete_statement = delete(AssetDagRunQueue).where(*where_clause)
    result = session.execute(delete_statement)

    if result.rowcount == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Queue event with dag_id: `{dag_id}` was not found")


@assets_router.delete(
    "/dags/{dag_id}/assets/{asset_id}/queuedEvents",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_access_asset(method="DELETE")),
        Depends(requires_access_dag(method="GET")),
        Depends(action_logging()),
    ],
)
def delete_dag_asset_queued_event(
    dag_id: str,
    asset_id: int,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    before: OptionalDateTimeQuery = None,
):
    """Delete a queued asset event for a DAG."""
    where_clause = _generate_queued_event_where_clause(
        dag_id=dag_id, before=before, asset_id=asset_id, permitted_dag_ids=readable_dags_filter.value
    )
    delete_statement = (
        delete(AssetDagRunQueue).where(*where_clause).execution_options(synchronize_session="fetch")
    )
    result = session.execute(delete_statement)
    if result.rowcount == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail=f"Queued event with dag_id: `{dag_id}` and asset_id: `{asset_id}` was not found",
        )
