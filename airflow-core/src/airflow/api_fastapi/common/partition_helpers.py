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
    from collections.abc import Callable

    from pendulum import DateTime
    from sqlalchemy.orm import Session

    from airflow.timetables.base import Timetable


log = structlog.get_logger(logger_name=__name__)


def suggest_partition_key_for_dag(
    *,
    dag_id: str,
    timetable: Timetable,
    now: DateTime,
    can_read_runs: Callable[[], bool],
    session: Session,
) -> str | None:
    """
    Return the partition key to suggest for a manual trigger of *dag_id*, or ``None``.

    This is the API-layer entry point for suggestions and a **guess**, not a validation.
    For an asset-driven partitioned Dag or a Dag partitioned at runtime, the first
    non-``None`` of these wins:

    1. The oldest pending ``AssetPartitionDagRun``, in the scheduler's FIFO order. That
       partition may still be waiting for other events, so it is not guaranteed to be satisfied.
       Triggering it manually does not mark it as handled, only the scheduler does that, so the
       scheduler may still create its own run for the same partition later, and this suggestion
       does not change until then.
    2. ``timetable.suggest_partition_key(now)``, a purely time-based guess.
    3. The ``partition_key`` of the most recently created successful ``DagRun``; if that key
       is ``None`` this returns ``None`` rather than searching further back. It can name a
       partition that already ran; re-running a partition is a legitimate re-materialization.

    Any other timetable, including cron partitioned ones, only gets source 2 and never
    touches *session*. Neither does any timetable when *can_read_runs* returns false: sources 1
    and 3 expose data of this Dag's runs, so the caller passes a check of whether the user may
    read them. It is only called for timetables that would use those sources.

    A failure of the timetable call is logged and treated as "no suggestion" from that
    source. Database errors propagate.

    ``models.dag.get_last_dagrun`` is intentionally not reused: it filters
    ``logical_date.is_not(None)`` and excludes ``MANUAL`` runs by default,
    which would drop exactly the partitioned runs source 3 needs.
    """
    # A cron partitioned Dag has a tick-based answer that is always newer than its last run's
    # key, so the database sources would only make it worse.
    uses_database_sources = (
        isinstance(timetable, PartitionedAssetTimetable) or timetable.partitioned_at_runtime
    ) and can_read_runs()

    if uses_database_sources:
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

    try:
        suggested_key = timetable.suggest_partition_key(now)
    except Exception:
        # Only the timetable call is guarded: it may be a plugin class and the suggestion is
        # optional, so it must not turn the Dag details response into a 500, nor keep the
        # last-run source below from running. A failing query must still surface.
        log.warning("Failed to compute suggested partition key", dag_id=dag_id, exc_info=True)
        suggested_key = None
    if suggested_key is not None or not uses_database_sources:
        return suggested_key

    return session.execute(
        select(DagRun.partition_key)
        .where(DagRun.dag_id == dag_id, DagRun.state == DagRunState.SUCCESS)
        .order_by(DagRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()


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
