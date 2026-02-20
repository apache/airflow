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

from datetime import datetime
from typing import cast

from fastapi import Depends, status
from sqlalchemy import and_, func, or_, select
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
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState

dashboard_router = AirflowRouter(tags=["Dashboard"], prefix="/dashboard")


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

    start_date_dt = cast("datetime", start_date)
    end_date_dt = cast("datetime | None", end_date)

    start_date_filter = DagRun.start_date >= start_date
    if current_time >= start_date_dt:
        start_date_filter = or_(start_date_filter, DagRun.start_date.is_(None))

    effective_end_date = end_date_dt or current_time
    end_date_filter = DagRun.end_date <= effective_end_date
    if current_time <= effective_end_date:
        end_date_filter = or_(end_date_filter, DagRun.end_date.is_(None))

    date_filters = and_(start_date_filter, end_date_filter)

    # Dag run types + States
    dag_run_metrics = session.execute(
        select(DagRun.run_type, DagRun.state, func.count(DagRun.run_id))
        .where(date_filters)
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .group_by(DagRun.run_type, DagRun.state)
    ).all()

    # TaskInstances
    task_instance_states = session.execute(
        select(TaskInstance.state, func.count(TaskInstance.run_id))
        .join(TaskInstance.dag_run)
        .where(date_filters)
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .group_by(TaskInstance.state)
    ).all()

    # Aggregate combined DagRun results into separate type and state maps.
    run_types_map: dict[str, int] = {dag_run_type.value: 0 for dag_run_type in DagRunType}
    run_states_map: dict[str, int] = {dag_run_state.value: 0 for dag_run_state in DagRunState}

    for run_type, state, count in dag_run_metrics:
        run_types_map[run_type] = run_types_map.get(run_type, 0) + count
        run_states_map[state] = run_states_map.get(state, 0) + count

    historical_metrics_response = {
        "dag_run_types": run_types_map,
        "dag_run_states": run_states_map,
        "task_instance_states": {
            "no_status": 0,
            **{ti_state.value: 0 for ti_state in TaskInstanceState},
            **{ti_state or "no_status": sum_value for ti_state, sum_value in task_instance_states},
        },
    }

    return HistoricalMetricDataResponse.model_validate(historical_metrics_response)


@dashboard_router.get(
    "/dag_stats",
    dependencies=[Depends(requires_access_dag(method="GET"))],
)
def dag_stats(
    session: SessionDep,
    readable_dags_filter: ReadableDagsFilterDep,
) -> DashboardDagStatsResponse:
    """Return basic DAG stats with counts of DAGs in various states."""
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
