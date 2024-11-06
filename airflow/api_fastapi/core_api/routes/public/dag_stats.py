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
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import (
    get_session,
    paginated_select,
)
from airflow.api_fastapi.common.db.dag_runs import dagruns_select_with_state_count
from airflow.api_fastapi.common.parameters import QueryDagIdsFilter
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.dag_stats import (
    DagStatsCollectionResponse,
    DagStatsResponse,
    DagStatsStateResponse,
)
from airflow.utils.state import DagRunState

dag_stats_router = AirflowRouter(tags=["DagStats"], prefix="/dagStats")


@dag_stats_router.get(
    "/",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_401_UNAUTHORIZED,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
async def get_dag_stats(
    session: Annotated[Session, Depends(get_session)],
    dag_ids: QueryDagIdsFilter,
) -> DagStatsCollectionResponse:
    """Get Dag statistics."""
    dagruns_select, _ = paginated_select(
        base_select=dagruns_select_with_state_count,
        filters=[dag_ids],
        session=session,
        return_total_entries=False,
    )
    query_result = session.execute(dagruns_select)

    result_dag_ids = []
    dag_state_data = {}
    for dag_id, state, count in query_result:
        dag_state_data[(dag_id, state)] = count
        if dag_id not in result_dag_ids:
            result_dag_ids.append(dag_id)

    dags = [
        DagStatsResponse(
            dag_id=dag_id,
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
