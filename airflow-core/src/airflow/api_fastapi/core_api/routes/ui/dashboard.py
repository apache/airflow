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
    permitted_dag_ids = readable_dags_filter.value
    # DagRuns
    dag_run_types = session.execute(
        select(DagRun.run_type, func.count(DagRun.run_id))
        .where(
            func.coalesce(DagRun.start_date, current_time) >= start_date,
            func.coalesce(DagRun.end_date, current_time) <= func.coalesce(end_date, current_time),
        )
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .group_by(DagRun.run_type)
    ).all()

    dag_run_states = session.execute(
        select(DagRun.state, func.count(DagRun.run_id))
        .where(
            func.coalesce(DagRun.start_date, current_time) >= start_date,
            func.coalesce(DagRun.end_date, current_time) <= func.coalesce(end_date, current_time),
        )
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .group_by(DagRun.state)
    ).all()

    # TaskInstances
    task_instance_states = session.execute(
        select(TaskInstance.state, func.count(TaskInstance.run_id))
        .join(TaskInstance.dag_run)
        .where(
            func.coalesce(DagRun.start_date, current_time) >= start_date,
            func.coalesce(DagRun.end_date, current_time) <= func.coalesce(end_date, current_time),
        )
        .where(DagRun.dag_id.in_(permitted_dag_ids))
        .group_by(TaskInstance.state)
    ).all()

    # Combining historical metrics response as dictionary
    historical_metrics_response = {
        "dag_run_types": {
            **{dag_run_type.value: 0 for dag_run_type in DagRunType},
            **dict(dag_run_types),
        },
        "dag_run_states": {
            **{dag_run_state.value: 0 for dag_run_state in DagRunState},
            **dict(dag_run_states),
        },
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
    permitted_dag_ids = readable_dags_filter.value
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

    counts = session.execute(combined_runs_query).first()

    return DashboardDagStatsResponse(
        active_dag_count=active_count,
        failed_dag_count=counts.failed,
        running_dag_count=counts.running,
        queued_dag_count=counts.queued,
    )
