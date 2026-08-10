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

from typing import TYPE_CHECKING

import structlog
from sqlalchemy import select

from airflow.exceptions import DeserializationError
from airflow.models.asset import AssetPartitionDagRun
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.timetables.simple import PartitionedAssetTimetable
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from pendulum import DateTime
    from sqlalchemy.orm import Session

    from airflow.timetables.base import Timetable


log = structlog.get_logger(logger_name=__name__)


def _extract_partitioned_timetable(serdag: SerializedDagModel) -> PartitionedAssetTimetable | None:
    """Return the ``PartitionedAssetTimetable`` carried by *serdag*, or ``None``."""
    try:
        timetable = serdag.dag.timetable
    except (DeserializationError, TypeError):
        # ``DeserializationError`` covers structural serialization failures so
        # a corrupted serialized Dag silently degrades to non-partitioned
        # rather than 500-ing the read-only UI page. ``TypeError`` covers the
        # eager validation in ``RollupMapper.__init__`` (raised during
        # timetable deserialization when an upstream mapper / window pair is
        # incompatible), so a misconfigured rollup Dag also degrades to
        # non-partitioned here rather than 500-ing.
        # ``KeyError`` / ``AttributeError`` / ``ImportError`` are intentionally
        # not caught: refactor bugs that rename an attribute on ``serdag.dag``
        # or break an import path must surface to the caller rather than
        # silently downgrading the route to non-rollup.
        log.warning("Failed to deserialize timetable for Dag", dag_id=serdag.dag_id, exc_info=True)
        return None
    if not timetable.partitioned:
        return None
    if TYPE_CHECKING:
        assert isinstance(timetable, PartitionedAssetTimetable)
    return timetable


def load_partitioned_timetable(dag_id: str, session: Session) -> PartitionedAssetTimetable | None:
    """
    Return the PartitionedAssetTimetable for *dag_id*, or None if absent or not partitioned.

    Callers gate this behind ``DagModel.has_rollup_mappers``, which is only
    populated for ``PartitionedAssetTimetable``. The ``TYPE_CHECKING`` assert
    narrows the type for mypy without a runtime ``isinstance`` cost.
    """
    serdag = SerializedDagModel.get(dag_id=dag_id, session=session)
    if serdag is None:
        return None
    return _extract_partitioned_timetable(serdag)


def load_partitioned_timetables(
    dag_ids: list[str], session: Session
) -> dict[str, PartitionedAssetTimetable | None]:
    """
    Batch-load PartitionedAssetTimetables for *dag_ids* in a single query.

    Routes that already gate per-Dag on ``DagModel.has_rollup_mappers`` should
    use this when iterating over many Dags so ``SerializedDagModel`` is hit
    once instead of once per Dag. Returns a dict keyed by ``dag_id``; entries
    whose timetable failed to deserialize or is not partitioned are ``None``.
    """
    if not dag_ids:
        return {}
    return {
        serdag.dag_id: _extract_partitioned_timetable(serdag)
        for serdag in SerializedDagModel.get_latest_serialized_dags(dag_ids=dag_ids, session=session)
    }


def suggest_partition_key_for_dag(
    *, dag_id: str, timetable: Timetable, now: DateTime, session: Session
) -> str | None:
    """
    Suggest a partition key to pre-fill the manual-trigger form for *dag_id*.

    This is a **guess** used only to pre-fill the UI form field; it is not a
    validation and callers must still accept ``None`` from
    ``validate_partition_key``. Returns ``None`` immediately for a timetable
    that is neither ``partitioned`` nor ``partitioned_at_runtime`` (no query is
    issued in that case). Otherwise, tries in order and returns the first
    non-``None`` result:

    1. The oldest pending ``AssetPartitionDagRun`` for *dag_id*
       (``created_dag_run_id IS NULL``) — an asset event has already arrived
       for this partition and is only waiting for the run to be created. Only
       asset-driven timetables have any rows here. The ordering mirrors the
       scheduler's FIFO claim order in
       ``SchedulerJobRunner._create_partition_dag_runs``, so the suggestion
       names the same partition the scheduler would create next rather than a
       newer one that will not run until the backlog drains.
    2. ``timetable.suggest_partition_key(now)`` — a purely time-based guess
       that needs no history, so even a brand-new Dag with no runs or asset
       events gets a suggestion (asset-driven timetables only; see
       :meth:`~airflow.timetables.base.Timetable.suggest_partition_key`).
    3. The ``partition_key`` of the most recent successful ``DagRun`` for
       *dag_id*. This is the only source available to
       ``partitioned_at_runtime`` timetables, which have no asset and no
       temporal anchor of their own. Only the single most recent successful
       run is consulted — if its ``partition_key`` is ``None`` (an
       unpartitioned run), this returns ``None`` rather than searching
       further back.

    Source 3 can name a partition that has already run, and nothing rejects a
    second ``DagRun`` for the same ``(dag_id, partition_key)`` — re-running a
    partition is a legitimate re-materialization, so this deliberately neither
    de-duplicates nor blocks it. The trigger form's help text warns the user
    that the pre-filled key may already have run.

    ``models.dag.get_last_dagrun`` is intentionally not reused here: it
    filters ``logical_date.is_not(None)`` and excludes ``MANUAL`` runs by
    default, which would drop exactly the partitioned runs this needs.
    """
    if not timetable.partitioned and not timetable.partitioned_at_runtime:
        return None

    pending_key = session.execute(
        select(AssetPartitionDagRun.partition_key)
        .where(
            AssetPartitionDagRun.target_dag_id == dag_id,
            AssetPartitionDagRun.created_dag_run_id.is_(None),
        )
        .order_by(AssetPartitionDagRun.created_at, AssetPartitionDagRun.id)
        .limit(1)
    ).scalar_one_or_none()
    if pending_key is not None:
        return pending_key

    suggested_key = timetable.suggest_partition_key(now)
    if suggested_key is not None:
        return suggested_key

    return session.execute(
        select(DagRun.partition_key)
        .where(DagRun.dag_id == dag_id, DagRun.state == DagRunState.SUCCESS)
        .order_by(DagRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()
