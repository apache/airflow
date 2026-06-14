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

import statistics
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, cast

from sqlalchemy import Column, select
from sqlalchemy.orm import Session

from airflow._shared.timezones import timezone as airflow_timezone
from airflow.api_fastapi.core_api.datamodels.ui.dag_schedule_overview import (
    DagScheduleOverviewCollectionResponse,
    DagScheduleOverviewEntry,
)
from airflow.models import DagModel, DagRun
from airflow.utils.state import DagRunState

# Cap the per-dag recent run window to keep the response bounded for very
# chatty deployments. 200 is a comfortable upper bound — most dags in real
# deployments are scheduled hourly or less frequently, and 200 samples per
# dag are more than enough to produce stable mean / median estimates.
DEFAULT_RECENT_RUNS_PER_DAG = 200
# Cap the total number of Dag rows in the response so a deployment with
# thousands of dags still returns quickly. The full per-dag list can be
# paginated / filtered client-side once this baseline view lands.
DEFAULT_MAX_DAGS = 500


def _seconds_since_midnight_utc(value: datetime | None) -> int | None:
    """
    Convert a datetime to integer seconds since 00:00:00 UTC.

    ``DagRun.start_date`` / ``end_date`` are stored as naive UTC datetimes
    in the metadata DB, but the ORM can hand back aware datetimes depending
    on the dialect. Normalize to UTC before slicing off the date component
    so the result is always expressed in UTC.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    delta = value - value.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(delta.total_seconds())


def _round_to_int(value: float | int | None) -> int | None:
    if value is None:
        return None
    return int(round(float(value)))


def _round_to_float(value: float | int | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 3)


class DagScheduleOverviewService:
    """
    Service that builds a 24h clock view of typical Dag run times.

    For each Dag in the deployment we look at recent *successful* runs and
    compute the time-of-day at which they typically start and end. The
    resulting per-Dag row can be rendered as a Gantt-style bar across a
    single 24h axis, which answers the question "across my whole
    deployment, when during the day does each Dag usually run?".
    """

    def __init__(
        self,
        *,
        recent_runs_per_dag: int = DEFAULT_RECENT_RUNS_PER_DAG,
        max_dags: int = DEFAULT_MAX_DAGS,
    ) -> None:
        self.recent_runs_per_dag = recent_runs_per_dag
        self.max_dags = max_dags

    def get_overview(
        self,
        session: Session,
        *,
        run_after_gte: datetime | None = None,
        run_after_lte: datetime | None = None,
        dag_id_pattern: str | None = None,
        dag_display_name_pattern: str | None = None,
    ) -> DagScheduleOverviewCollectionResponse:
        """
        Aggregate per-Dag schedule statistics across the deployment.

        Args:
            session: SQLAlchemy session.
            run_after_gte: Optional inclusive lower bound for ``DagRun.run_after``.
            run_after_lte: Optional inclusive upper bound for ``DagRun.run_after``.
            dag_id_pattern: Optional SQL LIKE pattern to filter ``DagModel.dag_id``.
            dag_display_name_pattern: Optional SQL LIKE pattern to filter
                ``DagModel.dag_display_name``.

        Returns:
            A collection response ordered by ``dag_id`` ascending.
        """
        # Coerce incoming datetimes to UTC-aware values so the comparison is
        # consistent regardless of the caller's local zone.
        run_after_gte = (
            airflow_timezone.coerce_datetime(run_after_gte).astimezone(timezone.utc)
            if run_after_gte is not None
            else None
        )
        run_after_lte = (
            airflow_timezone.coerce_datetime(run_after_lte).astimezone(timezone.utc)
            if run_after_lte is not None
            else None
        )

        # Step 1: pick the DagModels the caller is allowed to see. We use the
        # full DagModel query (not a join with DagRun) so dags that have never
        # been triggered still appear in the response as zero-statistic rows.
        # ``DagModel.dag_display_name`` is a hybrid_property whose expression
        # part is a CASE; cast to ``Column`` for mypy and to drive .ilike().
        dag_id_col = cast("Column[str]", DagModel.dag_id)
        dag_display_name_col = cast("Column[str]", DagModel.dag_display_name)
        dag_stmt = select(dag_id_col, dag_display_name_col)
        if dag_id_pattern:
            dag_stmt = dag_stmt.where(dag_id_col.ilike(dag_id_pattern))
        if dag_display_name_pattern:
            dag_stmt = dag_stmt.where(dag_display_name_col.ilike(dag_display_name_pattern))
        dag_stmt = dag_stmt.order_by(dag_id_col.asc()).limit(self.max_dags)

        dag_rows = session.execute(dag_stmt).all()

        dag_ids: list[str] = [row.dag_id for row in dag_rows]
        display_names: dict[str, str] = {row.dag_id: row.dag_display_name for row in dag_rows}

        if not dag_ids:
            return DagScheduleOverviewCollectionResponse(total_entries=0, entries=[])

        # Step 2: pull the most-recent successful runs for those dags, scoped
        # by the optional run_after window. We fetch the rows as-is and let
        # Python compute mean / median so the SQL stays trivially cross-dialect.
        runs_stmt = select(
            DagRun.dag_id,
            DagRun.logical_date,
            DagRun.start_date,
            DagRun.end_date,
        ).where(
            DagRun.dag_id.in_(dag_ids),
            DagRun.state == DagRunState.SUCCESS,
        )
        if run_after_gte is not None:
            runs_stmt = runs_stmt.where(DagRun.run_after >= run_after_gte)
        if run_after_lte is not None:
            runs_stmt = runs_stmt.where(DagRun.run_after <= run_after_lte)
        runs_stmt = runs_stmt.order_by(DagRun.dag_id.asc(), DagRun.run_after.desc())

        runs = session.execute(runs_stmt).all()

        # Step 3: keep only the most-recent N runs per dag so the per-dag
        # statistics are not biased by a long, dense history. The query is
        # already ordered by (dag_id ASC, run_after DESC) so the first N rows
        # for each dag are the most recent N.
        runs_by_dag: dict[str, list[Any]] = defaultdict(list)
        for row in runs:
            bucket = runs_by_dag[row.dag_id]
            if len(bucket) >= self.recent_runs_per_dag:
                continue
            bucket.append(row)

        entries: list[DagScheduleOverviewEntry] = []
        for dag_id in dag_ids:
            display_name = display_names.get(dag_id) or dag_id
            bucket = runs_by_dag.get(dag_id, [])

            if not bucket:
                entries.append(
                    DagScheduleOverviewEntry(
                        dag_id=dag_id,
                        dag_display_name=display_name,
                        recent_runs_count=0,
                        oldest_logical_date=None,
                        newest_logical_date=None,
                        start_mean_seconds=None,
                        start_median_seconds=None,
                        end_mean_seconds=None,
                        end_median_seconds=None,
                        duration_mean_seconds=None,
                        duration_median_seconds=None,
                    )
                )
                continue

            start_seconds: list[int] = []
            end_seconds: list[int] = []
            durations: list[float] = []
            logical_dates: list[datetime] = []

            for run in bucket:
                ss = _seconds_since_midnight_utc(run.start_date)
                es = _seconds_since_midnight_utc(run.end_date)
                if ss is not None:
                    start_seconds.append(ss)
                if es is not None:
                    end_seconds.append(es)
                if run.start_date is not None and run.end_date is not None:
                    delta = run.end_date - run.start_date
                    durations.append(delta.total_seconds())
                if run.logical_date is not None:
                    logical_dates.append(run.logical_date)

            entries.append(
                DagScheduleOverviewEntry(
                    dag_id=dag_id,
                    dag_display_name=display_name,
                    recent_runs_count=len(bucket),
                    oldest_logical_date=min(logical_dates) if logical_dates else None,
                    newest_logical_date=max(logical_dates) if logical_dates else None,
                    start_mean_seconds=_round_to_int(statistics.fmean(start_seconds))
                    if start_seconds
                    else None,
                    start_median_seconds=_round_to_int(statistics.median(start_seconds))
                    if start_seconds
                    else None,
                    end_mean_seconds=_round_to_int(statistics.fmean(end_seconds)) if end_seconds else None,
                    end_median_seconds=_round_to_int(statistics.median(end_seconds)) if end_seconds else None,
                    duration_mean_seconds=_round_to_float(statistics.fmean(durations)) if durations else None,
                    duration_median_seconds=_round_to_float(statistics.median(durations))
                    if durations
                    else None,
                )
            )

        return DagScheduleOverviewCollectionResponse(total_entries=len(entries), entries=entries)
