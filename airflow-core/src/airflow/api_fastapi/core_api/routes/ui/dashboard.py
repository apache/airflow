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

from typing import cast

from fastapi import Depends, status
from sqlalchemy import func, literal, select, union_all
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

dashboard_router = AirflowRouter(tags=["Dashboard"], prefix="/dashboard")

# Cap for state counts — avoids counting millions of rows.
# The UI shows "N+" when the returned count equals this value.
STATE_COUNT_CAP = 1000


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

    dag_run_filters = [
        func.coalesce(DagRun.start_date, current_time) >= start_date,
        func.coalesce(DagRun.end_date, current_time) <= func.coalesce(end_date, current_time),
        DagRun.dag_id.in_(permitted_dag_ids),
    ]

    # Build one LIMIT-capped subquery per state, then UNION ALL them into a
    # single query.  Every state gets the same treatment: at most STATE_COUNT_CAP
    # rows are read from the index, so even states with millions of rows
    # (typically "success") are counted in single-digit milliseconds.
    # Each branch is wrapped in a subquery so LIMIT works on all backends
    # (SQLite rejects LIMIT inside bare UNION ALL arms).
    def _capped_state_counts(model, states, label_fn, join=None):
        branches = []
        for state in states:
            stmt = select(literal(label_fn(state)).label("state")).select_from(model)
            if join is not None:
                stmt = stmt.join(join)
            branch = (
                stmt.where(*dag_run_filters)
                .where(model.state == state if state else model.state.is_(None))
                .limit(STATE_COUNT_CAP)
                .subquery()
            )
            branches.append(select(branch.c.state))
        capped = union_all(*branches).subquery()
        return session.execute(
            select(capped.c.state, func.count().label("cnt")).group_by(capped.c.state)
        ).all()

    dag_run_state_counts = _capped_state_counts(DagRun, list(DagRunState), lambda s: s.value)
    ti_state_counts = _capped_state_counts(
        TaskInstance,
        [None, *TaskInstanceState],
        lambda s: s.value if s else "no_status",
        join=TaskInstance.dag_run,
    )

    return HistoricalMetricDataResponse.model_validate(
        {
            "dag_run_states": {
                **{dag_run_state.value: 0 for dag_run_state in DagRunState},
                **{row.state: row.cnt for row in dag_run_state_counts},
            },
            "task_instance_states": {
                "no_status": 0,
                **{ti_state.value: 0 for ti_state in TaskInstanceState},
                **{row.state: row.cnt for row in ti_state_counts},
            },
            "state_count_limit": STATE_COUNT_CAP,
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
    latest_dates_subq = (
        select(DagRun.dag_id, func.max(DagRun.logical_date).label("max_logical_date"))
        .where(DagRun.logical_date.is_not(None))
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .group_by(DagRun.dag_id)
        .subquery()
    )

    # Active Dags need another query from DagModel, as a Dag may not have any runs but still be active
    active_count_query = (
        select(func.count())
        .select_from(DagModel)
        .where(DagModel.is_stale == false())
        .where(DagModel.is_paused == false())
        .where(DagModel.dag_id.in_(permitted_dag_ids))
    )
    active_count = session.execute(active_count_query).scalar_one()

    # Other metrics are based on latest DagRun states
    latest_runs_cte = (
        select(
            DagModel.dag_id,
            DagModel.is_paused,
            DagRun.state,
        )
        .join(DagModel, DagRun.dag_id == DagModel.dag_id)
        .join(
            latest_dates_subq,
            (DagRun.dag_id == latest_dates_subq.c.dag_id)
            & (DagRun.logical_date == latest_dates_subq.c.max_logical_date),
        )
        .where(DagModel.is_stale == false())
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .cte()
    )
    combined_runs_query = select(
        func.coalesce(func.sum(case((latest_runs_cte.c.state == DagRunState.FAILED, 1))), 0).label("failed"),
        func.coalesce(func.sum(case((latest_runs_cte.c.state == DagRunState.RUNNING, 1))), 0).label(
            "running"
        ),
        func.coalesce(func.sum(case((latest_runs_cte.c.state == DagRunState.QUEUED, 1))), 0).label("queued"),
    ).select_from(latest_runs_cte)

    counts = session.execute(combined_runs_query).one()

    return DashboardDagStatsResponse(
        active_dag_count=active_count,
        failed_dag_count=counts.failed,
        running_dag_count=counts.running,
        queued_dag_count=counts.queued,
    )
