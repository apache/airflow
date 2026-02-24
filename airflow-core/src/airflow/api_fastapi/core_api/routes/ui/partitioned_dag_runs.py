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

from fastapi import Depends, HTTPException, status
from sqlalchemy import exists, func, select

from airflow.api_fastapi.common.db.common import SessionDep, apply_filters_to_select
from airflow.api_fastapi.common.parameters import (
    QueryPartitionedDagRunDagIdFilter,
    QueryPartitionedDagRunHasCreatedDagRunIdFilter,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.partitioned_dag_runs import (
    PartitionedDagRunAssetResponse,
    PartitionedDagRunCollectionResponse,
    PartitionedDagRunDetailResponse,
    PartitionedDagRunResponse,
)
from airflow.api_fastapi.core_api.security import requires_access_asset
from airflow.models import DagModel
from airflow.models.asset import (
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
)
from airflow.models.dagrun import DagRun

partitioned_dag_runs_router = AirflowRouter(tags=["PartitionedDagRun"])


def _build_response(row, required_count: int) -> PartitionedDagRunResponse:
    return PartitionedDagRunResponse(
        id=row.id,
        dag_id=row.target_dag_id,
        partition_key=row.partition_key,
        created_at=row.created_at.isoformat() if row.created_at else None,
        total_received=row.total_received or 0,
        total_required=required_count,
        state=row.dag_run_state if row.created_dag_run_id else "pending",
        created_dag_run_id=row.dag_run_id,
    )


@partitioned_dag_runs_router.get(
    "/partitioned_dag_runs",
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_partitioned_dag_runs(
    session: SessionDep,
    dag_id: QueryPartitionedDagRunDagIdFilter,
    has_created_dag_run_id: QueryPartitionedDagRunHasCreatedDagRunIdFilter,
) -> PartitionedDagRunCollectionResponse:
    """Return PartitionedDagRuns. Filter by dag_id and/or has_created_dag_run_id."""
    if dag_id.value is not None:
        # Single query: validate Dag + get required count
        dag_info = session.execute(
            select(
                DagModel.timetable_summary,
                func.count(DagScheduleAssetReference.asset_id).label("required_count"),
            )
            .outerjoin(
                DagScheduleAssetReference,
                (DagScheduleAssetReference.dag_id == DagModel.dag_id)
                & DagScheduleAssetReference.asset_id.in_(
                    select(AssetModel.id).where(AssetModel.active.has())
                ),
            )
            .where(DagModel.dag_id == dag_id.value)
            .group_by(DagModel.dag_id)
        ).one_or_none()

        if dag_info is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id.value} was not found")
        if dag_info.timetable_summary != "Partitioned Asset":
            return PartitionedDagRunCollectionResponse(partitioned_dag_runs=[], total=0)

        required_count = dag_info.required_count

    # Subquery for received count per partition (only count required assets)
    required_assets_subq = (
        select(DagScheduleAssetReference.asset_id)
        .join(AssetModel, AssetModel.id == DagScheduleAssetReference.asset_id)
        .where(
            DagScheduleAssetReference.dag_id == AssetPartitionDagRun.target_dag_id,
            AssetModel.active.has(),
        )
    )
    received_subq = (
        select(func.count(func.distinct(PartitionedAssetKeyLog.asset_id)))
        .where(
            PartitionedAssetKeyLog.asset_partition_dag_run_id == AssetPartitionDagRun.id,
            PartitionedAssetKeyLog.asset_id.in_(required_assets_subq),
        )
        .correlate(AssetPartitionDagRun)
        .scalar_subquery()
    )

    query = select(
        AssetPartitionDagRun.id,
        AssetPartitionDagRun.target_dag_id,
        AssetPartitionDagRun.partition_key,
        AssetPartitionDagRun.created_at,
        AssetPartitionDagRun.created_dag_run_id,
        DagRun.run_id.label("dag_run_id"),
        DagRun.state.label("dag_run_state"),
        received_subq.label("total_received"),
    ).outerjoin(DagRun, AssetPartitionDagRun.created_dag_run_id == DagRun.id)
    query = apply_filters_to_select(statement=query, filters=[dag_id, has_created_dag_run_id])
    query = query.order_by(AssetPartitionDagRun.created_at.desc())

    if not (rows := session.execute(query).all()):
        return PartitionedDagRunCollectionResponse(partitioned_dag_runs=[], total=0)

    if dag_id.value is not None:
        results = [_build_response(row, required_count) for row in rows]
        return PartitionedDagRunCollectionResponse(partitioned_dag_runs=results, total=len(results))

    # No dag_id: need to get required counts and expressions per dag
    dag_ids = list({row.target_dag_id for row in rows})
    dag_rows = session.execute(
        select(
            DagModel.dag_id,
            DagModel.asset_expression,
            func.count(DagScheduleAssetReference.asset_id).label("required_count"),
        )
        .outerjoin(
            DagScheduleAssetReference,
            (DagScheduleAssetReference.dag_id == DagModel.dag_id)
            & DagScheduleAssetReference.asset_id.in_(select(AssetModel.id).where(AssetModel.active.has())),
        )
        .where(DagModel.dag_id.in_(dag_ids))
        .group_by(DagModel.dag_id)
    ).all()

    required_counts = {r.dag_id: r.required_count for r in dag_rows}
    asset_expressions = {r.dag_id: r.asset_expression for r in dag_rows}
    results = [_build_response(row, required_counts.get(row.target_dag_id, 0)) for row in rows]

    return PartitionedDagRunCollectionResponse(
        partitioned_dag_runs=results,
        total=len(results),
        asset_expressions=asset_expressions,
    )


@partitioned_dag_runs_router.get(
    "/pending_partitioned_dag_run/{dag_id}/{partition_key}",
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_pending_partitioned_dag_run(
    dag_id: str,
    partition_key: str,
    session: SessionDep,
) -> PartitionedDagRunDetailResponse:
    """Return full details for pending PartitionedDagRun."""
    partitioned_dag_run = session.execute(
        select(
            AssetPartitionDagRun.id,
            AssetPartitionDagRun.target_dag_id,
            AssetPartitionDagRun.partition_key,
            AssetPartitionDagRun.created_at,
            AssetPartitionDagRun.updated_at,
            DagRun.run_id.label("created_dag_run_id"),
        )
        .outerjoin(DagRun, AssetPartitionDagRun.created_dag_run_id == DagRun.id)
        .where(
            AssetPartitionDagRun.target_dag_id == dag_id,
            AssetPartitionDagRun.partition_key == partition_key,
            AssetPartitionDagRun.created_dag_run_id.is_(None),
        )
    ).one_or_none()

    if partitioned_dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No PartitionedDagRun for dag={dag_id} partition={partition_key}",
        )

    received_subq = (
        select(PartitionedAssetKeyLog.asset_id).where(
            PartitionedAssetKeyLog.asset_partition_dag_run_id == partitioned_dag_run.id
        )
    ).correlate(AssetModel)

    received_expr = exists(received_subq.where(PartitionedAssetKeyLog.asset_id == AssetModel.id))

    asset_expression_subq = (
        select(DagModel.asset_expression).where(DagModel.dag_id == dag_id).scalar_subquery()
    )
    asset_rows = session.execute(
        select(
            AssetModel.id,
            AssetModel.uri,
            AssetModel.name,
            received_expr.label("received"),
            asset_expression_subq.label("asset_expression"),
        )
        .join(DagScheduleAssetReference, DagScheduleAssetReference.asset_id == AssetModel.id)
        .where(DagScheduleAssetReference.dag_id == dag_id, AssetModel.active.has())
        .order_by(received_expr.asc(), AssetModel.uri)
    ).all()

    assets = [
        PartitionedDagRunAssetResponse(
            asset_id=row.id, asset_name=row.name, asset_uri=row.uri, received=row.received
        )
        for row in asset_rows
    ]
    total_received = sum(1 for a in assets if a.received)
    asset_expression = asset_rows[0].asset_expression if asset_rows else None

    return PartitionedDagRunDetailResponse(
        id=partitioned_dag_run.id,
        dag_id=dag_id,
        partition_key=partition_key,
        created_at=partitioned_dag_run.created_at.isoformat() if partitioned_dag_run.created_at else None,
        updated_at=partitioned_dag_run.updated_at.isoformat() if partitioned_dag_run.updated_at else None,
        created_dag_run_id=partitioned_dag_run.created_dag_run_id,
        assets=assets,
        total_required=len(assets),
        total_received=total_received,
        asset_expression=asset_expression,
    )
