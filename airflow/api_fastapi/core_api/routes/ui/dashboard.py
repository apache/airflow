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

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.parameters import DateTimeQuery
from airflow.api_fastapi.core_api.datamodels.ui.dashboard import HistoricalMetricDataResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.utils import timezone

dashboard_router = AirflowRouter(tags=["Dashboard"])


@dashboard_router.get(
    "/dashboard/historical_metrics_data",
    include_in_schema=False,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST]),
)
def historical_metrics(
    start_date: DateTimeQuery,
    end_date: DateTimeQuery,
    session: Annotated[Session, Depends(get_session)],
) -> HistoricalMetricDataResponse:
    """Return cluster activity historical metrics."""
    # DagRuns
    dag_run_types = session.execute(
        select(DagRun.run_type, func.count(DagRun.run_id))
        .where(
            DagRun.start_date >= start_date,
            func.coalesce(DagRun.end_date, timezone.utcnow()) <= end_date,
        )
        .group_by(DagRun.run_type)
    ).all()

    dag_run_states = session.execute(
        select(DagRun.state, func.count(DagRun.run_id))
        .where(
            DagRun.start_date >= start_date,
            func.coalesce(DagRun.end_date, timezone.utcnow()) <= end_date,
        )
        .group_by(DagRun.state)
    ).all()

    # TaskInstances
    task_instance_states = session.execute(
        select(TaskInstance.state, func.count(TaskInstance.run_id))
        .join(TaskInstance.dag_run)
        .where(
            DagRun.start_date >= start_date,
            func.coalesce(DagRun.end_date, timezone.utcnow()) <= end_date,
        )
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

    return HistoricalMetricDataResponse.model_validate(historical_metrics_response, from_attributes=True)
