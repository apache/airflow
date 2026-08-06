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

from decimal import ROUND_FLOOR, Context
from typing import TYPE_CHECKING, cast

from fastapi import Depends, status
from sqlalchemy import func, select
from sqlalchemy.sql.expression import case, false

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import DateTimeQuery, OptionalDateTimeQuery
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.dashboard import (
    DashboardDagStatsResponse,
    HistoricalMetricDataResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import ReadableDagsFilterDep, requires_access_dag
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

dashboard_router = AirflowRouter(tags=["Dashboard"], prefix="/dashboard")

# Rows a single scan reads. Windows that fit are counted exactly; wider ones report a floor.
EXACT_COUNT_LIMIT = 50_000


_ROUNDING = Context(prec=2, rounding=ROUND_FLOOR)


def _round_down(value: int) -> int:
    """Round to two significant digits, never upwards: that would claim uncounted rows."""
    return int(_ROUNDING.create_decimal(value))


def _compute_state_counts(
    model, filters, *, session: Session, join=None, null_label: str | None = None
) -> tuple[dict[str, int], bool]:
    """
    Per-state counts for the window, and whether they are lower bounds rather than exact.

    A scan that stopped early counted only some of the rows, but each count is still a floor
    on the real value.
    """
    stmt = select(model.state.label("state")).select_from(model)
    if join is not None:
        stmt = stmt.join(join)
    window = stmt.where(*filters).limit(EXACT_COUNT_LIMIT + 1).subquery()
    rows = session.execute(select(window.c.state, func.count().label("cnt")).group_by(window.c.state)).all()
    are_lower_bounds = sum(row.cnt for row in rows) > EXACT_COUNT_LIMIT
    counts: dict[str, int] = {}
    for row in rows:
        label = row.state or null_label
        if label is None:
            continue
        counts[label] = _round_down(row.cnt) if are_lower_bounds else row.cnt
    return counts, are_lower_bounds


@dashboard_router.get(
    "/historical_metrics_data",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN)),
    ],
)
def historical_metrics(
    session: SessionDep,
    start_date: DateTimeQuery,
    readable_dags_filter: ReadableDagsFilterDep,
    end_date: OptionalDateTimeQuery = None,
) -> HistoricalMetricDataResponse:
    """Return cluster activity historical metrics."""
    current_time = timezone.utcnow()
    permitted_dag_ids = cast("set[str]", readable_dags_filter.value)

    end_bound = end_date if end_date is not None else current_time
    dag_run_filters = [
        DagRun.run_after >= start_date,
        DagRun.run_after <= end_bound,
        DagRun.dag_id.in_(permitted_dag_ids),
    ]

    # Judged separately: dag runs often fit when task instances do not.
    dag_run_states, dag_runs_are_lower_bounds = _compute_state_counts(
        DagRun, dag_run_filters, session=session
    )
    task_instance_states, task_instances_are_lower_bounds = _compute_state_counts(
        TaskInstance,
        dag_run_filters,
        session=session,
        join=TaskInstance.dag_run,
        null_label="no_status",
    )

    return HistoricalMetricDataResponse.model_validate(
        {
            "dag_run_states": {
                **{dag_run_state.value: 0 for dag_run_state in DagRunState},
                **dag_run_states,
            },
            "task_instance_states": {
                "no_status": 0,
                **{ti_state.value: 0 for ti_state in TaskInstanceState},
                **task_instance_states,
            },
            "dag_run_counts_are_lower_bounds": dag_runs_are_lower_bounds,
            "task_instance_counts_are_lower_bounds": task_instances_are_lower_bounds,
        }
    )


@dashboard_router.get(
    "/dag_stats",
    dependencies=[Depends(requires_access_dag(method="GET"))],
)
def dag_stats(
    session: SessionDep,
    readable_dags_filter: ReadableDagsFilterDep,
) -> DashboardDagStatsResponse:
    """Return basic Dag stats with counts of Dags in various states."""
    permitted_dag_ids = cast("set[str]", readable_dags_filter.value)

    latest_state = (
        select(DagRun.state)
        .where(DagRun.dag_id == DagModel.dag_id, DagRun.logical_date.is_not(None))
        .order_by(DagRun.logical_date.desc())
        .limit(1)
        .correlate(DagModel)
        .scalar_subquery()
    )
    dag_counts_query = (
        select(
            func.coalesce(func.sum(case((DagModel.is_paused == false(), 1))), 0).label("active"),
            func.coalesce(func.sum(case((latest_state == DagRunState.FAILED, 1))), 0).label("failed"),
        )
        .select_from(DagModel)
        .where(DagModel.is_stale == false())
        .where(DagModel.dag_id.in_(permitted_dag_ids))
    )
    dag_counts = session.execute(dag_counts_query).one()

    active_states_query = (
        select(DagRun.state, func.count(func.distinct(DagRun.dag_id)))
        .join(DagModel, DagModel.dag_id == DagRun.dag_id)
        .where(DagModel.is_stale == false())
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .where(DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED]))
        .group_by(DagRun.state)
    )
    active_counts = {state: count for state, count in session.execute(active_states_query)}

    return DashboardDagStatsResponse(
        active_dag_count=dag_counts.active,
        failed_dag_count=dag_counts.failed,
        running_dag_count=active_counts.get(DagRunState.RUNNING, 0),
        queued_dag_count=active_counts.get(DagRunState.QUEUED, 0),
    )
