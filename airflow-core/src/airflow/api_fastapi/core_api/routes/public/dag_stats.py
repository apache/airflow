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

from typing import Annotated

from fastapi import Depends, status

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.db.dag_runs import dagruns_select_with_state_count
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_stats import (
    DagStatsCollectionResponse,
    DagStatsResponse,
    DagStatsStateResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import ReadableDagRunsFilterDep, requires_access_dag
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

dag_stats_router = AirflowRouter(tags=["DagStats"], prefix="/dagStats")


@dag_stats_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_dag_stats(
    readable_dag_runs_filter: ReadableDagRunsFilterDep,
    session: SessionDep,
    dag_ids: Annotated[
        FilterParam[list[str]],
        Depends(filter_param_factory(DagRun.dag_id, list[str], FilterOptionEnum.IN, "dag_ids")),
    ],
) -> DagStatsCollectionResponse:
    """Get Dag statistics."""
    dagruns_select, _ = paginated_select(
        statement=dagruns_select_with_state_count,
        filters=[dag_ids, readable_dag_runs_filter],
        session=session,
        return_total_entries=False,
    )
    query_result = session.execute(dagruns_select)

    result_dag_ids = []
    dag_display_names: dict[str, str] = {}
    dag_state_data = {}
    for dag_id, state, dag_display_name, count in query_result:
        dag_state_data[(dag_id, state)] = count
        if dag_id not in result_dag_ids:
            dag_display_names[dag_id] = dag_display_name
            result_dag_ids.append(dag_id)

    dags = [
        DagStatsResponse(
            dag_id=dag_id,
            dag_display_name=dag_display_names[dag_id],
            stats=[
                DagStatsStateResponse(
                    state=state,
                    count=dag_state_data.get((dag_id, state), 0),
                )
                for state in DagRunState
            ],
        )
        for dag_id in result_dag_ids
    ]
    return DagStatsCollectionResponse(dags=dags, total_entries=len(dags))
