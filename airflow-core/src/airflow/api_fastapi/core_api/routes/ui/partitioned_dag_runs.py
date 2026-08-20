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

from typing import TYPE_CHECKING, Any, NamedTuple, TypeAlias, cast

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import and_, select

from airflow.api_fastapi.common.db.common import SessionDep, apply_filters_to_select, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    QueryPartitionedDagRunDagIdFilter,
    QueryPartitionedDagRunHasCreatedDagRunIdFilter,
)
from airflow.api_fastapi.common.partition_helpers import (
    load_partitioned_timetable,
    load_partitioned_timetables,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.partitioned_dag_runs import (
    PartitionedDagRunAssetResponse,
    PartitionedDagRunCollectionResponse,
    PartitionedDagRunDetailResponse,
    PartitionedDagRunResponse,
)
from airflow.api_fastapi.core_api.security import (
    ReadableDagsFilterDep,
    requires_access_asset,
    requires_access_dag,
)
from airflow.models import DagModel
from airflow.models.asset import (
    AssetActive,
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
)
from airflow.models.dagrun import DagRun

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.partition_mappers.base import RollupMapper
    from airflow.timetables.simple import PartitionedAssetTimetable


log = structlog.get_logger(logger_name=__name__)


AssetNameUri: TypeAlias = tuple[str, str]
"""A ``(name, uri)`` pair identifying an asset."""


def _fetch_active_assets_per_dag(
    dag_ids: list[str], session: Session
) -> dict[str, tuple[list[AssetNameUri], dict[int, AssetNameUri]]]:
    """
    Batch-fetch required assets for multiple Dags in a single query.

    Returns ``{dag_id: ([(name, uri), ...], {asset_id: (name, uri)})}``.
    Dags with no references are still included with empty containers
    so callers can index by ``dag_id`` without ``KeyError``.

    Inactive (deactivated) assets are still included so list-route totals stay
    symmetric with the detail-route response; the per-asset ``asset_inactive``
    flag (detail route only) surfaces the freeze state.
    """
    rows = session.execute(
        select(
            DagScheduleAssetReference.dag_id,
            AssetModel.id,
            AssetModel.name,
            AssetModel.uri,
        )
        .join(DagScheduleAssetReference, DagScheduleAssetReference.asset_id == AssetModel.id)
        .where(DagScheduleAssetReference.dag_id.in_(dag_ids))
    ).all()
    result: dict[str, tuple[list[AssetNameUri], dict[int, AssetNameUri]]] = {
        dag_id: ([], {}) for dag_id in dag_ids
    }
    for row in rows:
        info, id_to_info = result[row.dag_id]
        info.append((row.name, row.uri))
        id_to_info[row.id] = (row.name, row.uri)
    return result


class _RollupResolution(NamedTuple):
    """
    Outcome of resolving an asset's upstream-key requirement for one partition key.

    Three states, distinguished so callers can match the scheduler's
    ``_resolve_asset_partition_status`` semantics:

    - ``keys`` is a ``frozenset`` and ``mapper_failed`` is ``False``: rollup
      asset, mapper succeeded — use ``keys`` as the required set.
    - ``keys`` is ``None`` and ``mapper_failed`` is ``False``: not a rollup
      asset — a single received event satisfies it.
    - ``keys`` is ``None`` and ``mapper_failed`` is ``True``: rollup asset
      whose mapper raised — the scheduler treats it as not-yet-satisfied; the
      UI must not credit any received event either, otherwise progress would
      silently show "ready" for a run the scheduler will never fire.
    """

    keys: frozenset[str] | None = None
    mapper_failed: bool = False


def _resolve_rollup_status(
    dag_model: DagModel | None,
    rollup_timetable: PartitionedAssetTimetable | None,
    name: str,
    uri: str,
    partition_key: str,
) -> _RollupResolution:
    """
    Resolve the rollup state for *(name, uri)* under the given partition key.

    The ``dag_model is None`` / ``rollup_timetable is None`` cases short-circuit
    to "not rollup" because there is nothing to evaluate against, not because
    the asset is mis-configured.
    """
    if dag_model is None or rollup_timetable is None or not dag_model.is_rollup_asset(name=name, uri=uri):
        return _RollupResolution()
    try:
        mapper = rollup_timetable.get_partition_mapper(name=name, uri=uri)
        return _RollupResolution(keys=frozenset(cast("RollupMapper", mapper).to_upstream(partition_key)))
    except Exception:
        # Mismatch with the scheduler's rollup contract. The scheduler writes a
        # Log row for the same condition (once per misconfiguration); this path
        # is per-request and lighter.
        log.warning(
            "Failed to evaluate rollup mapper; treating asset as not-yet-satisfied",
            dag_id=dag_model.dag_id,
            asset_name=name,
            asset_uri=uri,
            partition_key=partition_key,
            exc_info=True,
        )
        return _RollupResolution(mapper_failed=True)


def _build_asset_resolutions(
    dag_model: DagModel | None,
    rollup_timetable: PartitionedAssetTimetable | None,
    asset_info: list[AssetNameUri],
    partition_key: str,
) -> dict[AssetNameUri, _RollupResolution]:
    """
    Resolve each ``(name, uri)`` asset once per APDR row.

    Shared between ``_compute_total_required`` and ``_compute_received_count``.
    ``_resolve_rollup_status`` runs ``to_upstream`` for rollup assets, which
    yields up to 60 keys (HourWindow) per mapper call. Without this cache each
    row evaluated the same ``(name, uri)`` twice.
    """
    return {
        (name, uri): _resolve_rollup_status(dag_model, rollup_timetable, name, uri, partition_key)
        for name, uri in asset_info
    }


def _compute_total_required(resolutions: dict[AssetNameUri, _RollupResolution]) -> int:
    """
    Sum required upstream events across all assets, using to_upstream for rollup mappers.

    Non-rollup assets and broken-mapper assets both count as 1: non-rollup needs
    one event to satisfy, broken-mapper counts as 1 unit of "blocked" so the
    asset still contributes to the totals (received side credits 0, keeping the
    progress short of "ready" as the scheduler intends).
    """
    return sum(len(res.keys) if res.keys is not None else 1 for res in resolutions.values())


def _compute_received_count(
    received_by_asset: dict[int, set[str]],
    asset_id_to_info: dict[int, AssetNameUri],
    resolutions: dict[AssetNameUri, _RollupResolution],
) -> int:
    """
    Count received events using rollup-aware deduplication.

    For rollup assets whose mapper succeeded: count distinct upstream keys that
    intersect the required set. For non-rollup assets: any logged event
    satisfies the asset. For rollup assets whose mapper raised: do not credit
    anything received, matching the scheduler's not-yet-satisfied verdict.
    """
    total = 0
    for asset_id, received_keys in received_by_asset.items():
        # Logs may reference assets that have been removed from the Dag's
        # reference list entirely (asset removed from declaration). The skip
        # filter avoids counting those. Inactive-but-still-declared assets stay
        # in ``asset_id_to_info`` so their historical received events keep
        # contributing to the totals.
        if (info := asset_id_to_info.get(asset_id)) is None:
            continue
        res = resolutions[info]
        if res.keys is not None:
            total += len(received_keys & res.keys)
        elif res.mapper_failed:
            # Match scheduler: a broken mapper means the run is held; don't
            # let UI counts march toward "ready" while the scheduler holds.
            continue
        else:
            total += 1 if received_keys else 0
    return total


partitioned_dag_runs_router = AirflowRouter(tags=["PartitionedDagRun"])


def _build_response(row, required_count: int, received_count: int) -> PartitionedDagRunResponse:
    return PartitionedDagRunResponse(
        id=row.id,
        dag_id=row.target_dag_id,
        partition_key=row.partition_key,
        created_at=row.created_at.isoformat() if row.created_at else None,
        total_received=received_count,
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
    limit: QueryLimit,
    offset: QueryOffset,
    readable_dags_filter: ReadableDagsFilterDep,
    dag_id: QueryPartitionedDagRunDagIdFilter,
    has_created_dag_run_id: QueryPartitionedDagRunHasCreatedDagRunIdFilter,
) -> PartitionedDagRunCollectionResponse:
    """Return PartitionedDagRuns. Filter by dag_id and/or has_created_dag_run_id."""
    # The Dag-existence check is intentionally deferred to the empty-results branch
    # below. In the happy path (rows exist), filtering by dag_id already restricts
    # to that Dag, so an extra DagModel lookup just to validate existence wastes a
    # query. We only consult DagModel when we have no rows to report — that's the
    # only case where the distinction (404 vs empty) matters.

    query = select(
        AssetPartitionDagRun.id,
        AssetPartitionDagRun.target_dag_id,
        AssetPartitionDagRun.partition_key,
        AssetPartitionDagRun.created_at,
        AssetPartitionDagRun.created_dag_run_id,
        DagRun.run_id.label("dag_run_id"),
        DagRun.state.label("dag_run_state"),
    ).outerjoin(DagRun, AssetPartitionDagRun.created_dag_run_id == DagRun.id)
    query = apply_filters_to_select(statement=query, filters=[dag_id, has_created_dag_run_id])
    readable_dag_ids = readable_dags_filter.value
    if readable_dag_ids is not None:
        query = query.where(AssetPartitionDagRun.target_dag_id.in_(readable_dag_ids))
    query = query.order_by(AssetPartitionDagRun.created_at.desc(), AssetPartitionDagRun.id.desc())

    query, total_entries = paginated_select(
        statement=query,
        offset=offset,
        limit=limit,
        session=session,
    )

    if not (rows := session.execute(query).all()):
        if dag_id.value is not None and total_entries == 0:
            dag_exists = session.scalar(select(DagModel.dag_id).where(DagModel.dag_id == dag_id.value))
            if dag_exists is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id.value} was not found")
        return PartitionedDagRunCollectionResponse(partitioned_dag_runs=[], total=total_entries)

    # Batch-fetch DagModels (for cached partition_mapper_info), required assets,
    # and APDR log entries in three single queries instead of N per-Dag queries.
    # Timetables are only loaded for Dags that actually have rollup mappers,
    # since that's the only case where ``to_upstream`` evaluation is needed.
    # A SQL count subquery for total_received cannot honour rollup windows
    # without running the mapper, so the rollup-aware Python computation runs
    # uniformly across single-Dag and global views.
    unique_dag_ids = list({row.target_dag_id for row in rows})
    dag_models: dict[str, DagModel] = {
        dm.dag_id: dm
        for dm in session.scalars(select(DagModel).where(DagModel.dag_id.in_(unique_dag_ids))).all()
    }
    assets_by_dag = _fetch_active_assets_per_dag(unique_dag_ids, session)
    # Batch-fetch timetables for only the Dags that need rollup evaluation so the
    # query stays bounded by ``has_rollup_mappers`` rather than the full Dag set.
    rollup_dag_ids = [
        d_id for d_id in unique_dag_ids if (dm := dag_models.get(d_id)) is not None and dm.has_rollup_mappers
    ]
    rollup_timetables_by_dag: dict[str, PartitionedAssetTimetable | None] = {
        d_id: None for d_id in unique_dag_ids
    }
    rollup_timetables_by_dag.update(load_partitioned_timetables(rollup_dag_ids, session))

    apdr_ids = [row.id for row in rows]
    log_by_apdr: dict[int, dict[int, set[str]]] = {}
    for pakl_row in session.execute(
        select(
            PartitionedAssetKeyLog.asset_partition_dag_run_id,
            PartitionedAssetKeyLog.asset_id,
            PartitionedAssetKeyLog.source_partition_key,
        ).where(PartitionedAssetKeyLog.asset_partition_dag_run_id.in_(apdr_ids))
    ).all():
        log_by_apdr.setdefault(pakl_row.asset_partition_dag_run_id, {}).setdefault(
            pakl_row.asset_id, set()
        ).add(pakl_row.source_partition_key)

    results = []
    for row in rows:
        asset_info, asset_id_to_info = assets_by_dag[row.target_dag_id]
        resolutions = _build_asset_resolutions(
            dag_models.get(row.target_dag_id),
            rollup_timetables_by_dag[row.target_dag_id],
            asset_info,
            row.partition_key,
        )
        results.append(
            _build_response(
                row,
                _compute_total_required(resolutions),
                _compute_received_count(log_by_apdr.get(row.id, {}), asset_id_to_info, resolutions),
            )
        )

    asset_expressions: dict[str, dict | None] | None = None
    if dag_id.value is None:
        asset_expressions = {dm.dag_id: dm.asset_expression for dm in dag_models.values()}

    model_data: dict[str, Any] = {
        "partitioned_dag_runs": results,
        "total": total_entries,
        "asset_expressions": asset_expressions,
    }
    return PartitionedDagRunCollectionResponse.model_validate(model_data)


@partitioned_dag_runs_router.get(
    "/pending_partitioned_dag_run/{dag_id}",
    dependencies=[Depends(requires_access_asset(method="GET")), Depends(requires_access_dag(method="GET"))],
)
def get_pending_partitioned_dag_run(
    dag_id: str,
    partition_key: str,
    session: SessionDep,
) -> PartitionedDagRunDetailResponse:
    """Return full details for pending PartitionedDagRun."""
    # partition_key is a query param, not a path segment: it is a free-form key
    # (up to 250 chars) that may itself contain "/", which would otherwise be
    # ambiguous (or mis-routed) as a path segment.
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
        # Duplicate pending rows for the same (dag_id, partition_key) can exist
        # after a crash; mirror _get_or_create_apdr and work on the latest one.
        .order_by(AssetPartitionDagRun.id.desc())
        .limit(1)
    ).first()

    if partitioned_dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No PartitionedDagRun for dag={dag_id} partition={partition_key}",
        )

    # Collect received upstream partition keys per asset for this partition run.
    # Use a set to deduplicate: multiple events for the same key count as one.
    received_keys_by_asset: dict[int, set[str]] = {}
    for row in session.execute(
        select(
            PartitionedAssetKeyLog.asset_id,
            PartitionedAssetKeyLog.source_partition_key,
        ).where(PartitionedAssetKeyLog.asset_partition_dag_run_id == partitioned_dag_run.id)
    ):
        received_keys_by_asset.setdefault(row.asset_id, set()).add(row.source_partition_key)

    dag_model = session.get(DagModel, dag_id)
    asset_rows = session.execute(
        select(
            AssetModel.id,
            AssetModel.uri,
            AssetModel.name,
            AssetActive.name.label("active_name"),
        )
        .join(DagScheduleAssetReference, DagScheduleAssetReference.asset_id == AssetModel.id)
        .outerjoin(
            AssetActive,
            and_(AssetActive.name == AssetModel.name, AssetActive.uri == AssetModel.uri),
        )
        .where(DagScheduleAssetReference.dag_id == dag_id)
        .order_by(AssetModel.uri)
    ).all()

    # Skip the timetable load when no rollup mapper is configured — the cached
    # ``partition_mapper_info`` already tells us whether we will need
    # ``to_upstream`` evaluation, which is the only thing the timetable adds here.
    has_rollup_mappers = dag_model is not None and dag_model.has_rollup_mappers
    rollup_timetable = load_partitioned_timetable(dag_id, session) if has_rollup_mappers else None

    assets = []
    for asset_row in asset_rows:
        received_keys = sorted(received_keys_by_asset.get(asset_row.id, set()))
        required_keys: list[str] = [partition_key]
        is_rollup = (
            has_rollup_mappers
            and dag_model is not None
            and dag_model.is_rollup_asset(name=asset_row.name, uri=asset_row.uri)
        )
        mapper_failed = False
        if is_rollup and rollup_timetable is not None:
            try:
                mapper = rollup_timetable.get_partition_mapper(name=asset_row.name, uri=asset_row.uri)
                required_keys = sorted(cast("RollupMapper", mapper).to_upstream(partition_key))
            except Exception:
                # Mirror the scheduler: a misconfigured rollup mapper holds the
                # run, so the detail view must not show "received" for an asset
                # the scheduler treats as not-yet-satisfied.
                log.warning(
                    "Failed to evaluate rollup mapper; treating asset as not-yet-satisfied",
                    dag_id=dag_id,
                    asset_name=asset_row.name,
                    asset_uri=asset_row.uri,
                    partition_key=partition_key,
                    exc_info=True,
                )
                mapper_failed = True
        if mapper_failed:
            received_keys = []
            required_keys = []
            received_count = 0
            required_count = 1
        elif is_rollup:
            received_count = len(received_keys)
            required_count = len(required_keys)
        else:
            # Match the list route's _compute_received_count: a non-rollup asset is
            # satisfied by any single received event, so credit caps at 1 even if
            # several distinct upstream keys mapped onto this one target key.
            required_count = len(required_keys)
            received_count = 1 if received_keys else 0
        assets.append(
            PartitionedDagRunAssetResponse(
                asset_id=asset_row.id,
                asset_name=asset_row.name,
                asset_uri=asset_row.uri,
                received=received_count >= required_count and required_count > 0,
                received_count=received_count,
                required_count=required_count,
                received_keys=received_keys,
                required_keys=required_keys,
                is_rollup=is_rollup,
                mapper_error=mapper_failed,
                asset_inactive=(asset_row.active_name is None),
            )
        )

    total_received = sum(a.received_count for a in assets)
    total_required = sum(a.required_count for a in assets)
    asset_expression = dag_model.asset_expression if dag_model is not None else None

    model_data: dict[str, Any] = {
        "id": partitioned_dag_run.id,
        "dag_id": dag_id,
        "partition_key": partition_key,
        "created_at": partitioned_dag_run.created_at.isoformat() if partitioned_dag_run.created_at else None,
        "updated_at": partitioned_dag_run.updated_at.isoformat() if partitioned_dag_run.updated_at else None,
        "created_dag_run_id": partitioned_dag_run.created_dag_run_id,
        "assets": assets,
        "total_required": total_required,
        "total_received": total_received,
        "asset_expression": asset_expression,
    }
    return PartitionedDagRunDetailResponse.model_validate(model_data)
