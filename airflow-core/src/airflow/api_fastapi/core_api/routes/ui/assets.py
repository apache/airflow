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

from typing import TYPE_CHECKING, Annotated, Any, cast, get_args

import structlog
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import ColumnElement, and_, case, exists, func, or_, select, true, union
from sqlalchemy.orm import aliased, selectinload

from airflow.api_fastapi.common.asset_expression import redact_asset_expression
from airflow.api_fastapi.common.db.assets import (
    generate_assets_with_last_event_query,
    resolve_triggering_event_refs,
)
from airflow.api_fastapi.common.db.common import SessionDep, apply_filters_to_select, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryAssetDagIdPatternSearch,
    QueryAssetGroupPatternSearch,
    QueryAssetGroupPrefixPatternSearch,
    QueryAssetNamePatternSearch,
    QueryAssetNamePrefixPatternSearch,
    QueryHasEventsFilter,
    QueryLimit,
    QueryOffset,
    QueryUriExactMatch,
    QueryUriPatternSearch,
    QueryUriPrefixPatternSearch,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
)
from airflow.api_fastapi.common.partition_helpers import load_partitioned_timetable
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import (
    AssetCollectionResponse,
    AssetResponse,
    DagRunAssetReference,
)
from airflow.api_fastapi.core_api.datamodels.ui.assets import (
    AssetPartitionCollectionResponse,
    AssetPartitionProducerRunResponse,
    AssetPartitionResponse,
    AssetPartitionStatus,
    AssetPartitionStatusCounts,
    NextRunAssetEventResponse,
    NextRunAssetsResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.routes.public.assets import OnlyActiveFilter
from airflow.api_fastapi.core_api.security import (
    ReadableAssetsFilterDep,
    ReadableDagsFilterDep,
    requires_access_asset,
    requires_access_asset_alias,
    requires_access_dag,
)
from airflow.models import DagModel
from airflow.models.asset import (
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
    TaskOutletAssetReference,
)
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from sqlalchemy import Subquery

    from airflow.partition_mappers.base import RollupMapper

log = structlog.get_logger(logger_name=__name__)

assets_router = AirflowRouter(tags=["Asset"])


@assets_router.get(
    "/assets",
    dependencies=[
        Depends(requires_access_asset(method="GET")),
        Depends(requires_access_asset_alias(method="GET")),
    ],
    operation_id="get_assets_ui",
)
def get_assets(
    limit: QueryLimit,
    offset: QueryOffset,
    name_pattern: QueryAssetNamePatternSearch,
    name_prefix_pattern: QueryAssetNamePrefixPatternSearch,
    uri: QueryUriExactMatch,
    uri_pattern: QueryUriPatternSearch,
    uri_prefix_pattern: QueryUriPrefixPatternSearch,
    group_pattern: QueryAssetGroupPatternSearch,
    group_prefix_pattern: QueryAssetGroupPrefixPatternSearch,
    dag_ids: QueryAssetDagIdPatternSearch,
    has_events: QueryHasEventsFilter,
    only_active: Annotated[OnlyActiveFilter, Depends(OnlyActiveFilter.depends)],
    last_asset_event_timestamp_range: Annotated[
        RangeFilter,
        Depends(
            datetime_range_filter_factory(
                "last_asset_event_timestamp", AssetEvent, attribute_name="timestamp"
            )
        ),
    ],
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "name", "uri", "group", "created_at", "updated_at"],
                AssetModel,
                {"last_asset_event_timestamp": AssetEvent.timestamp},
            ).dynamic_depends(default="-last_asset_event_timestamp")
        ),
    ],
    readable_assets_filter: ReadableAssetsFilterDep,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
) -> AssetCollectionResponse:
    """Get assets. Like the public endpoint, but also supports sorting by group and last asset event timestamp."""
    assets_select, total_entries = paginated_select(
        statement=generate_assets_with_last_event_query(),
        filters=[
            only_active,
            name_pattern,
            name_prefix_pattern,
            uri,
            uri_pattern,
            uri_prefix_pattern,
            group_pattern,
            group_prefix_pattern,
            dag_ids,
            has_events,
            last_asset_event_timestamp_range,
            readable_assets_filter,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    assets = [
        AssetResponse.from_asset_row(
            asset,
            last_asset_event_id,
            last_asset_event_timestamp,
            readable_dag_ids=readable_dags_filter.value,
        )
        for asset, last_asset_event_id, last_asset_event_timestamp in session.execute(assets_select)
    ]

    return AssetCollectionResponse(assets=assets, total_entries=total_entries)


@assets_router.get(
    "/next_run_assets/{dag_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
def next_run_assets(
    dag_id: str,
    readable_assets_filter: ReadableAssetsFilterDep,
    session: SessionDep,
) -> NextRunAssetsResponse:
    dag_model = DagModel.get_dagmodel(dag_id, session=session)
    if dag_model is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    asset_expression = redact_asset_expression(
        dag_model.asset_expression, readable_asset_ids=readable_assets_filter.value or set()
    )

    latest_run = dag_model.get_last_dagrun(session=session)
    event_filter = (
        AssetEvent.timestamp >= latest_run.logical_date if latest_run and latest_run.logical_date else true()
    )

    pending_partition_count: int | None = None

    queued_expr: ColumnElement[int]
    if is_partitioned := dag_model.timetable_partitioned:
        # Count pending APDRs directly without joining DagScheduleAssetReference /
        # AssetModel — neither table appears in the WHERE clause, so the joins
        # would collapse a cartesian product (N declared assets × M pending APDRs)
        # before `count()` folded it back. `next_run_assets` runs on every home
        # and Dags-list render, so this matters.
        pending_partition_count = session.scalar(
            select(func.count())
            .select_from(AssetPartitionDagRun)
            .where(
                AssetPartitionDagRun.target_dag_id == dag_id,
                AssetPartitionDagRun.created_dag_run_id.is_(None),
            )
        )
        queued_expr = case(
            (
                exists(
                    select(PartitionedAssetKeyLog.id)
                    .join(
                        AssetPartitionDagRun,
                        PartitionedAssetKeyLog.asset_partition_dag_run_id == AssetPartitionDagRun.id,
                    )
                    .where(
                        PartitionedAssetKeyLog.asset_id == AssetModel.id,
                        PartitionedAssetKeyLog.target_dag_id == dag_id,
                        AssetPartitionDagRun.created_dag_run_id.is_(None),
                    )
                ),
                1,
            ),
            else_=0,
        )
    else:
        queued_expr = func.max(case((AssetDagRunQueue.asset_id.is_not(None), 1), else_=0))

    query = (
        select(
            AssetModel.id,
            AssetModel.uri,
            AssetModel.name,
            func.max(AssetEvent.timestamp).label("last_update"),
            queued_expr.label("queued"),
            AssetActive.name.label("active_name"),
        )
        .join(DagScheduleAssetReference, DagScheduleAssetReference.asset_id == AssetModel.id)
        .join(AssetEvent, and_(AssetEvent.asset_id == AssetModel.id, event_filter), isouter=True)
        .outerjoin(
            AssetActive,
            and_(AssetActive.name == AssetModel.name, AssetActive.uri == AssetModel.uri),
        )
        .where(DagScheduleAssetReference.dag_id == dag_id)
        .group_by(AssetModel.id, AssetModel.uri, AssetModel.name, AssetActive.name)
        .order_by(AssetModel.uri)
    )
    query = readable_assets_filter.to_orm(query)

    # Counted before the readable filter narrows ``query``: the UI derives the
    # schedule's shape (total, and whether to render the multi-asset popover) from
    # this, so that shape stays the same for every caller regardless of which
    # assets they may read.
    scheduling_asset_count = session.scalar(
        select(func.count())
        .select_from(DagScheduleAssetReference)
        .where(DagScheduleAssetReference.dag_id == dag_id)
    )

    if not is_partitioned:
        query = query.join(
            AssetDagRunQueue,
            and_(
                AssetDagRunQueue.asset_id == AssetModel.id,
                AssetDagRunQueue.target_dag_id == DagScheduleAssetReference.dag_id,
            ),
            isouter=True,
        )

    raw_rows = list(session.execute(query))

    if not is_partitioned:
        events = [
            NextRunAssetEventResponse(
                id=row.id,
                name=row.name,
                uri=row.uri,
                last_update=row.last_update if row.queued else None,
                asset_inactive=(row.active_name is None),
            )
            for row in raw_rows
        ]
        model_data: dict[str, Any] = {
            "asset_expression": asset_expression,
            "events": events,
            "scheduling_asset_count": scheduling_asset_count,
        }
        return NextRunAssetsResponse.model_validate(model_data)

    # Partitioned Dags: enrich with per-asset received/required counts and rollup flag.
    # This shows the pending APDR that has waited longest. It is not necessarily the
    # next run: the scheduler (``_create_dagruns_for_partitioned_asset_dags``) visits
    # pending APDRs in rotation and fires whichever is satisfied, whatever its age.
    pending_apdr = session.execute(
        select(AssetPartitionDagRun.id, AssetPartitionDagRun.partition_key)
        .where(
            AssetPartitionDagRun.target_dag_id == dag_id,
            AssetPartitionDagRun.created_dag_run_id.is_(None),
        )
        .order_by(AssetPartitionDagRun.created_at)
        .limit(1)
    ).one_or_none()

    has_rollup_mappers = dag_model.has_rollup_mappers

    if pending_apdr is None:
        # No pending APDR yet — mark rollup assets so the UI can handle them
        # correctly (e.g. skip "Asset Triggered" in favour of the asset name view).
        # Reads from the cached partition_mapper_info so no timetable load is needed.
        events = [
            NextRunAssetEventResponse(
                id=row.id,
                name=row.name,
                uri=row.uri,
                last_update=row.last_update if row.queued else None,
                is_rollup=has_rollup_mappers and dag_model.is_rollup_asset(name=row.name, uri=row.uri),
                asset_inactive=(row.active_name is None),
            )
            for row in raw_rows
        ]
        model_data = {
            "asset_expression": asset_expression,
            "events": events,
            "scheduling_asset_count": scheduling_asset_count,
            "pending_partition_count": pending_partition_count,
        }
        return NextRunAssetsResponse.model_validate(model_data)

    # Collect received upstream partition keys per asset for this partition run.
    # Use a set to deduplicate: multiple events for the same key count as one.
    received_keys_by_asset: dict[int, set[str]] = {}
    for log_row in session.execute(
        select(
            PartitionedAssetKeyLog.asset_id,
            PartitionedAssetKeyLog.source_partition_key,
        ).where(PartitionedAssetKeyLog.asset_partition_dag_run_id == pending_apdr.id)
    ):
        received_keys_by_asset.setdefault(log_row.asset_id, set()).add(log_row.source_partition_key)

    # The timetable is only needed to call ``to_upstream`` for rollup mappers.
    # When the cached info shows no rollup mappers, skip loading it entirely.
    rollup_timetable = load_partitioned_timetable(dag_id, session) if has_rollup_mappers else None

    events = []
    for row in raw_rows:
        received_keys = sorted(received_keys_by_asset.get(row.id, set()))
        required_keys: list[str] = [pending_apdr.partition_key]
        is_rollup = has_rollup_mappers and dag_model.is_rollup_asset(name=row.name, uri=row.uri)
        mapper_failed = False
        if is_rollup and rollup_timetable is not None:
            try:
                mapper = rollup_timetable.get_partition_mapper(name=row.name, uri=row.uri)
                required_keys = sorted(cast("RollupMapper", mapper).to_upstream(pending_apdr.partition_key))
            except Exception:
                # Mirror the scheduler's ``_resolve_asset_partition_status``: a
                # misconfigured rollup mapper marks the asset as not-yet-satisfied
                # and the Dag run is held. Without this branch the UI would fall
                # through to the non-rollup default of 1/1 and silently show
                # "ready" for a run the scheduler will never fire.
                log.warning(
                    "Failed to evaluate rollup mapper; treating asset as not-yet-satisfied",
                    dag_id=dag_id,
                    asset_name=row.name,
                    asset_uri=row.uri,
                    partition_key=pending_apdr.partition_key,
                    exc_info=True,
                )
                mapper_failed = True
        if mapper_failed:
            received_keys = []
            required_keys = []
            received_count = 0
            required_count = 1
            last_update = None
        else:
            received_count = len(received_keys)
            required_count = len(required_keys)
            # Only surface last_update once all required upstream keys have arrived.
            last_update = row.last_update if row.queued and received_count >= required_count else None
        events.append(
            NextRunAssetEventResponse(
                id=row.id,
                name=row.name,
                uri=row.uri,
                last_update=last_update,
                received_count=received_count,
                required_count=required_count,
                received_keys=received_keys,
                required_keys=required_keys,
                is_rollup=is_rollup,
                mapper_error=mapper_failed,
                asset_inactive=(row.active_name is None),
            )
        )

    model_data = {
        "asset_expression": asset_expression,
        "events": events,
        "scheduling_asset_count": scheduling_asset_count,
        "pending_partition_count": pending_partition_count,
    }
    return NextRunAssetsResponse.model_validate(model_data)


def _build_asset_partition_status_subquery(asset_id: int, readable_dag_ids: set[str] | None) -> Subquery:
    """
    Classify every partition key of an asset.

    A producer run belongs to its own ``partition_key``, which is the key its asset
    events get unless the task overrides it. A run that emitted this asset, but never
    under its own key, is known to have overridden the key and is left out. For each
    key, ``E`` is the newest event and ``R`` the newest attempt among the runs of the
    Dags that declare the asset as an outlet:

    - ``R`` queued or running, and no ``E`` since the attempt began: ``in_progress``
    - ``R`` failed, and no ``E`` since the attempt began: ``failed``
    - otherwise, ``E`` exists: ``materialized``
    - otherwise the key is left out

    An attempt begins at ``queued_at``, which is also reset when a task of the run is
    cleared, whether or not the run has finished. A cleared run is therefore the newest
    attempt and counts as not having produced an event yet.

    Runs and events of Dags the caller may not read are left out before anything is
    aggregated, so neither the rows nor the counts reveal them.
    """
    event_filters = [AssetEvent.asset_id == asset_id, AssetEvent.partition_key.is_not(None)]
    run_filters = [
        DagRun.dag_id.in_(
            select(TaskOutletAssetReference.dag_id).where(TaskOutletAssetReference.asset_id == asset_id)
        ),
        DagRun.partition_key.is_not(None),
    ]
    if readable_dag_ids is not None:
        # Events created through the API or by a watcher have no source Dag to authorize on.
        event_filters.append(
            or_(AssetEvent.source_dag_id.in_(readable_dag_ids), AssetEvent.source_dag_id.is_(None))
        )
        run_filters.append(DagRun.dag_id.in_(readable_dag_ids))

    ranked_events = (
        select(
            AssetEvent.partition_key.label("partition_key"),
            AssetEvent.id.label("event_id"),
            AssetEvent.timestamp.label("event_timestamp"),
            func.row_number()
            .over(
                partition_by=AssetEvent.partition_key,
                order_by=(AssetEvent.timestamp.desc(), AssetEvent.id.desc()),
            )
            .label("position"),
        )
        .where(*event_filters)
        .subquery("ranked_events")
    )
    last_events = select(ranked_events).where(ranked_events.c.position == 1).cte("last_events")

    # One pass over the asset's events, rather than a lookup per run.
    source_run = aliased(DagRun)
    emitted_under_own_key = func.sum(case((AssetEvent.partition_key == source_run.partition_key, 1), else_=0))
    key_overriding_runs = (
        select(AssetEvent.source_dag_id.label("dag_id"), AssetEvent.source_run_id.label("run_id"))
        .join(
            source_run,
            and_(
                source_run.dag_id == AssetEvent.source_dag_id,
                source_run.run_id == AssetEvent.source_run_id,
            ),
        )
        .where(AssetEvent.asset_id == asset_id, AssetEvent.partition_key.is_not(None))
        .group_by(AssetEvent.source_dag_id, AssetEvent.source_run_id)
        .having(emitted_under_own_key == 0)
        .cte("key_overriding_runs")
    )
    attempt_began_at = func.coalesce(DagRun.queued_at, DagRun.start_date, DagRun.run_after)
    ranked_runs = (
        select(
            DagRun.partition_key.label("partition_key"),
            DagRun.dag_id.label("run_dag_id"),
            DagRun.run_id.label("run_id"),
            DagRun.state.label("run_state"),
            DagRun.start_date.label("run_start_date"),
            DagRun.end_date.label("run_end_date"),
            attempt_began_at.label("attempt_began_at"),
            func.row_number()
            .over(
                partition_by=DagRun.partition_key,
                order_by=(attempt_began_at.desc(), DagRun.id.desc()),
            )
            .label("position"),
        )
        .outerjoin(
            key_overriding_runs,
            and_(
                key_overriding_runs.c.dag_id == DagRun.dag_id,
                key_overriding_runs.c.run_id == DagRun.run_id,
            ),
        )
        .where(*run_filters, key_overriding_runs.c.run_id.is_(None))
        .subquery("ranked_runs")
    )
    last_runs = select(ranked_runs).where(ranked_runs.c.position == 1).cte("last_runs")
    keys = union(select(last_events.c.partition_key), select(last_runs.c.partition_key)).cte("partition_keys")

    no_event_since_attempt_began = or_(
        last_events.c.event_id.is_(None),
        last_events.c.event_timestamp < last_runs.c.attempt_began_at,
    )
    partition_status = case(
        (
            and_(
                last_runs.c.run_state.in_([DagRunState.QUEUED, DagRunState.RUNNING]),
                no_event_since_attempt_began,
            ),
            "in_progress",
        ),
        (and_(last_runs.c.run_state == DagRunState.FAILED, no_event_since_attempt_began), "failed"),
        (last_events.c.event_id.is_not(None), "materialized"),
        else_=None,
    )
    return (
        select(
            keys.c.partition_key,
            partition_status.label("status"),
            last_events.c.event_id.label("last_event_id"),
            last_events.c.event_timestamp.label("last_event_timestamp"),
            last_runs.c.run_dag_id,
            last_runs.c.run_id,
            last_runs.c.run_state,
            last_runs.c.run_start_date,
            last_runs.c.run_end_date,
        )
        .select_from(keys)
        .outerjoin(last_events, last_events.c.partition_key == keys.c.partition_key)
        .outerjoin(last_runs, last_runs.c.partition_key == keys.c.partition_key)
        .subquery("asset_partitions")
    )


PARTITION_SORT_COLUMNS = {
    "partition_key": "partition_key",
    "status": "status",
    "last_event_timestamp": "last_event_timestamp",
    "last_producer_run": "run_start_date",
}


def _build_partition_order(columns: Any, order_by: str) -> list[ColumnElement]:
    """Order by an allow-listed column, NULLs last in both directions, with the unique key as tiebreak."""
    sort_name = order_by.removeprefix("-")
    column = columns.c[PARTITION_SORT_COLUMNS[sort_name]]
    sort_key = [
        case((column.is_(None), 1), else_=0),
        column.desc() if order_by.startswith("-") else column.asc(),
    ]
    return sort_key if sort_name == "partition_key" else [*sort_key, columns.c.partition_key.asc()]


@assets_router.get(
    "/assets/{asset_id}/partitions",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_asset_partitions(
    asset_id: int,
    limit: QueryLimit,
    offset: QueryOffset,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    partition_status: Annotated[list[AssetPartitionStatus] | None, Query(alias="status")] = None,
    partition_key_pattern: str | None = None,
    order_by: str = "-partition_key",
) -> AssetPartitionCollectionResponse:
    """Return the partition keys of an asset with the status of each."""
    sort_name = order_by.removeprefix("-")
    if sort_name not in PARTITION_SORT_COLUMNS:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Ordering with '{order_by}' is disallowed or the attribute does not exist on the model",
        )
    if session.scalar(select(AssetModel.id).where(AssetModel.id == asset_id)) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Asset with ID: `{asset_id}` was not found")

    readable_dag_ids = readable_dags_filter.value
    partitions = _build_asset_partition_status_subquery(asset_id, readable_dag_ids)
    filters: list[ColumnElement[bool]] = [partitions.c.status.is_not(None)]
    if partition_key_pattern:
        filters.append(partitions.c.partition_key.icontains(partition_key_pattern, autoescape=True))

    selected = set(partition_status or get_args(AssetPartitionStatus))
    filtered = select(partitions).where(*filters).cte("filtered_partitions")
    counts = (
        select(
            *[
                func.coalesce(func.sum(case((filtered.c.status == name, 1), else_=0)), 0).label(
                    f"{name}_count"
                )
                for name in get_args(AssetPartitionStatus)
            ]
        )
        .select_from(filtered)
        .cte("partition_counts")
    )
    page = apply_filters_to_select(
        statement=select(filtered)
        .where(filtered.c.status.in_(selected))
        .order_by(*_build_partition_order(filtered, order_by)),
        filters=[offset, limit],
    ).cte("partition_page")
    # The counts row is kept by the outer join, so an empty page still carries the counts.
    all_rows = session.execute(
        select(*counts.c, *page.c)
        .select_from(counts.outerjoin(page, true()))
        .order_by(*_build_partition_order(page, order_by))
    ).all()
    status_counts = AssetPartitionStatusCounts.model_validate(
        {name: getattr(all_rows[0], f"{name}_count") for name in get_args(AssetPartitionStatus)}
    )
    total_entries = sum(getattr(status_counts, name) for name in selected)
    rows = [row for row in all_rows if row.partition_key is not None]

    consumer_runs: dict[int, list[DagRunAssetReference]] = {}
    if last_event_ids := [row.last_event_id for row in rows if row.last_event_id is not None]:
        events = session.scalars(
            select(AssetEvent)
            .where(AssetEvent.id.in_(last_event_ids))
            .options(selectinload(AssetEvent.created_dagruns))
        ).all()
        triggering_refs = resolve_triggering_event_refs(events, session=session)
        for event in events:
            runs = []
            for run in event.created_dagruns:
                # In this route ``None`` means every Dag is readable.
                if readable_dag_ids is None or run.dag_id in readable_dag_ids:
                    run.triggering = (event.id, run.dag_id, run.run_id) in triggering_refs
                    runs.append(DagRunAssetReference.model_validate(run))
            consumer_runs[event.id] = runs

    return AssetPartitionCollectionResponse(
        partitions=[
            AssetPartitionResponse(
                partition_key=row.partition_key,
                status=row.status,
                last_event_id=row.last_event_id,
                last_event_timestamp=row.last_event_timestamp,
                last_producer_run=(
                    None
                    if row.run_dag_id is None
                    else AssetPartitionProducerRunResponse(
                        dag_id=row.run_dag_id,
                        run_id=row.run_id,
                        state=row.run_state,
                        start_date=row.run_start_date,
                        end_date=row.run_end_date,
                    )
                ),
                consumer_runs=consumer_runs.get(row.last_event_id, []),
            )
            for row in rows
        ],
        total_entries=total_entries,
        status_counts=status_counts,
    )
