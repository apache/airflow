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

from collections import defaultdict
from collections.abc import Sequence
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import func, select, tuple_, union_all
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.interfaces import LoaderOption

from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

# Use the hybrid_property for dag_display_name so the CASE WHEN fallback to dag_id
# is applied when dag_display_name is NULL.  Using __table__.c would return raw NULLs
# and crash DagStatsResponse validation (https://github.com/apache/airflow/issues/64247).
dagruns_select_with_state_count = (
    select(  # type: ignore[call-overload]
        DagRun.__table__.c.dag_id,
        DagRun.__table__.c.state,
        DagModel.dag_display_name,
        func.count(DagRun.__table__.c.state).label("count"),
    )
    .join(DagModel, DagRun.__table__.c.dag_id == DagModel.__table__.c.dag_id)
    .group_by(DagRun.__table__.c.dag_id, DagRun.__table__.c.state, DagModel.dag_display_name)
    .order_by(DagRun.__table__.c.dag_id)
)


def eager_load_dag_run_for_list() -> tuple[LoaderOption, ...]:
    """
    Lightweight eager loading for the DagRun list endpoint.

    Only loads the direct relationships needed for serialization (dag_model,
    dag_run_note, created_dag_version).  The dag_versions property — which
    requires iterating every TI and TIH — is populated separately by
    :func:`attach_dag_versions_to_runs` using a single DISTINCT query.
    """
    return (
        joinedload(DagRun.dag_model),
        joinedload(DagRun.dag_run_note),
        joinedload(DagRun.created_dag_version).joinedload(DagVersion.bundle),
    )


def attach_dag_versions_to_runs(dag_runs: Sequence[DagRun], *, session: Session) -> None:
    """
    Prefetch distinct dag_version_ids for each DagRun via a lightweight query.

    Instead of loading all TI and TIH rows (potentially thousands per run)
    through the ORM relationship just to extract distinct dag_version_ids,
    this issues a single query that returns only the distinct
    (dag_id, run_id, dag_version_id) tuples for the given runs.

    The result is attached to each DagRun as ``_prefetched_dag_version_ids``
    (a dict mapping version_id -> DagVersion), which the ``dag_versions``
    property reads as an optimized substitute for traversing TI/TIH
    relationships.  All business logic (bundle_version shortcut, sorting,
    deduplication) remains solely in ``DagRun.dag_versions``.
    """
    if not dag_runs:
        return

    # Only runs without a bundle_version need TI/TIH traversal;
    # runs with bundle_version use created_dag_version directly
    # (handled by the dag_versions property).
    runs_needing_versions = [dr for dr in dag_runs if not dr.bundle_version]
    if not runs_needing_versions:
        return

    run_key_values = [(dr.dag_id, dr.run_id) for dr in runs_needing_versions]

    ti_sub = (
        select(
            TaskInstance.dag_id,
            TaskInstance.run_id,
            TaskInstance.dag_version_id,
        )
        .where(TaskInstance.dag_version_id.isnot(None))
        .where(tuple_(TaskInstance.dag_id, TaskInstance.run_id).in_(run_key_values))
        .distinct()
    )
    tih_sub = (
        select(
            TaskInstanceHistory.dag_id,
            TaskInstanceHistory.run_id,
            TaskInstanceHistory.dag_version_id,
        )
        .where(TaskInstanceHistory.dag_version_id.isnot(None))
        .where(tuple_(TaskInstanceHistory.dag_id, TaskInstanceHistory.run_id).in_(run_key_values))
        .distinct()
    )
    combined = union_all(ti_sub, tih_sub).subquery()
    rows = session.execute(
        select(combined.c.dag_id, combined.c.run_id, combined.c.dag_version_id).distinct()
    ).all()

    all_version_ids = {r.dag_version_id for r in rows}
    versions_by_id: dict[UUID, DagVersion] = {}
    if all_version_ids:
        dv_query = (
            select(DagVersion)
            .where(DagVersion.id.in_(all_version_ids))
            .options(joinedload(DagVersion.bundle))
        )
        versions_by_id = {dv.id: dv for dv in session.scalars(dv_query).unique()}

    versions_per_run: dict[tuple[str, str], dict[UUID, DagVersion]] = defaultdict(dict)
    for row in rows:
        dv = versions_by_id.get(row.dag_version_id)
        if dv:
            versions_per_run[(row.dag_id, row.run_id)][dv.id] = dv

    for dr in runs_needing_versions:
        dr._prefetched_dag_version_ids = versions_per_run.get((dr.dag_id, dr.run_id), {})
