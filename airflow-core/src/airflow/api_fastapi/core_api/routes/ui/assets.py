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

from typing import TYPE_CHECKING, cast

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import ColumnElement, and_, case, exists, func, select, true

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.partition_helpers import load_partitioned_timetable
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.assets import (
    NextRunAssetEventResponse,
    NextRunAssetsResponse,
)
from airflow.api_fastapi.core_api.security import requires_access_asset, requires_access_dag
from airflow.models import DagModel
from airflow.models.asset import (
    AssetActive,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
)

if TYPE_CHECKING:
    from airflow.partition_mappers.base import RollupMapper

log = structlog.get_logger(logger_name=__name__)

assets_router = AirflowRouter(tags=["Asset"])


@assets_router.get(
    "/next_run_assets/{dag_id}",
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
def next_run_assets(
    dag_id: str,
    session: SessionDep,
) -> NextRunAssetsResponse:
    dag_model = DagModel.get_dagmodel(dag_id, session=session)
    if dag_model is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

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
        return NextRunAssetsResponse(asset_expression=dag_model.asset_expression, events=events)

    # Partitioned Dags: enrich with per-asset received/required counts and rollup flag.
    # FIFO matches the scheduler's pending-APDR processing order
    # (``_create_dagruns_for_partitioned_asset_dags``), so the "next run" the UI
    # surfaces is the same one the scheduler will fire next.
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
        return NextRunAssetsResponse(
            asset_expression=dag_model.asset_expression,
            events=events,
            pending_partition_count=pending_partition_count,
        )

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

    return NextRunAssetsResponse(
        asset_expression=dag_model.asset_expression,
        events=events,
        pending_partition_count=pending_partition_count,
    )
