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

from fastapi import Depends

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.dag_schedule_overview import (
    DagScheduleOverviewCollectionResponse,
)
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.dag_schedule_overview import DagScheduleOverviewService

dag_schedule_overview_router = AirflowRouter(prefix="/dag_schedule_overview", tags=["Schedule Overview"])


@dag_schedule_overview_router.get(
    "",
    dependencies=[
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
)
def get_dag_schedule_overview(
    session: SessionDep,
    run_after_gte: datetime | None = None,
    run_after_lte: datetime | None = None,
    dag_id_pattern: str | None = None,
    dag_display_name_pattern: str | None = None,
) -> DagScheduleOverviewCollectionResponse:
    """
    Aggregate per-Dag typical start / end times across the deployment.

    Returns mean and median time-of-day (in UTC, as seconds since midnight)
    at which each Dag typically starts and ends, derived from the most
    recent successful Dag runs. Renders the Gantt-style 24h overview
    without forcing the UI to fetch and crunch data per-Dag.
    """
    service = DagScheduleOverviewService()
    return service.get_overview(
        session=session,
        run_after_gte=run_after_gte,
        run_after_lte=run_after_lte,
        dag_id_pattern=dag_id_pattern,
        dag_display_name_pattern=dag_display_name_pattern,
    )
