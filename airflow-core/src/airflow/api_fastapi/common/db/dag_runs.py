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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.interfaces import LoaderOption

from airflow.api_fastapi.common.db.dags import eager_load_teams
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun

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
    dag_run_note, created_dag_version, and the owning team).  The dag_versions
    property — which requires iterating every attempt — is populated separately by
    :func:`attach_dag_versions_to_runs` using a single DISTINCT query.
    """
    return (
        joinedload(DagRun.dag_model),
        joinedload(DagRun.dag_run_note),
        joinedload(DagRun.created_dag_version).joinedload(DagVersion.bundle),
        *eager_load_teams(DagRun.dag_model),
    )


def attach_dag_versions_to_runs(dag_runs: Sequence[DagRun], *, session: Session) -> None:
    """
    Batch-prefetch dag versions for many runs in one query (list/grid endpoints).

    Bundle-versioned runs are included: after a ``run_on_latest_version``
    clear they can keep older versions on uncleared TIs or archived attempts,
    so they must not skip this prefetch.
    """
    if not dag_runs:
        return
    per_run = DagRun.fetch_dag_versions_for_runs([(dr.dag_id, dr.run_id) for dr in dag_runs], session=session)
    for dr in dag_runs:
        dr._prefetched_dag_version_ids = per_run.get((dr.dag_id, dr.run_id), {})
