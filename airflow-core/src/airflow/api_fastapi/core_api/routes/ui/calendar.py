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

from typing import Annotated, Literal

from fastapi import Depends

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import RangeFilter, datetime_range_filter_factory
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.calendar import CalendarTimeRangeCollectionResponse
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.calendar import CalendarService
from airflow.models.dagrun import DagRun

calendar_router = AirflowRouter(prefix="/calendar", tags=["Calendar"])


@calendar_router.get(
    "/{dag_id}",
    dependencies=[
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.TASK_INSTANCE,
            )
        ),
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
)
def get_calendar(
    dag_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    logical_date: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", DagRun))],
    granularity: Literal["hourly", "daily"] = "daily",
) -> CalendarTimeRangeCollectionResponse:
    """Get calendar data for a DAG including historical and planned DAG runs."""
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)

    calendar_service = CalendarService()

    return calendar_service.get_calendar_data(
        dag_id=dag_id,
        session=session,
        dag=dag,
        logical_date=logical_date,
        granularity=granularity,
    )
