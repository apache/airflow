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
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from airflow.api_fastapi.core_api.datamodels.backfills import BackfillResponse
from airflow.models import DagRun
from airflow.models.backfill import BackfillDagRun

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def enrich_backfill_responses(
    backfills: list[BackfillResponse],
    *,
    session: Session,
) -> list[BackfillResponse]:
    """Populate num_runs and dag_run_state_counts on each backfill response."""
    ids = [b.id for b in backfills]
    if not ids:
        return backfills
    # Single query: get state counts per backfill, derive num_runs by summing counts.
    rows = session.execute(
        select(
            BackfillDagRun.backfill_id,
            DagRun.state,
            func.count().label("count"),
        )
        .join(DagRun, BackfillDagRun.dag_run_id == DagRun.id)
        .where(
            BackfillDagRun.backfill_id.in_(ids),
            DagRun.backfill_id == BackfillDagRun.backfill_id,
            DagRun.state.is_not(None),
        )
        .group_by(BackfillDagRun.backfill_id, DagRun.state)
    ).all()
    counts: dict[int, dict[str, int]] = defaultdict(dict)
    num_runs: dict[int, int] = defaultdict(int)
    for backfill_id, state, count in rows:
        counts[backfill_id][str(state)] = count
        num_runs[backfill_id] += count
    for backfill in backfills:
        backfill.num_runs = num_runs.get(backfill.id, 0)
        backfill.dag_run_state_counts = dict(counts.get(backfill.id, {}))
    return backfills
