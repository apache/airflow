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

from fastapi import Depends
from sqlalchemy import func, select
from sqlalchemy.orm import Query, Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import (
    get_session,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.dag_stats import (
    DagStatsCollectionResponse,
    DagStatsResponse,
    DagStatsStateResponse,
)
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

dag_stats_router = AirflowRouter(tags=["DagStats"], prefix="/dagStats")


@dag_stats_router.get(
    "/",
    responses=create_openapi_http_exception_doc([400, 401, 403, 404, 422]),
)
def get_dag_stats(
    session: Annotated[Session, Depends(get_session)],
    dag_ids: str | None = None,
) -> DagStatsCollectionResponse:
    """Get Dag statistics."""
    query: Query = select(
        DagRun.dag_id,
        DagRun.state,
        func.count(DagRun.state),
    )
    if dag_ids:
        query_dag_ids = sorted(set(dag_ids.split(",")))
        query = query.where(DagRun.dag_id.in_(query_dag_ids))
    query = query.group_by(DagRun.dag_id, DagRun.state)
    query_result = session.execute(query)

    result_dag_ids = set()
    dag_state_data = {}
    for dag_id, state, count in query_result:
        dag_state_data[(dag_id, state)] = count
        result_dag_ids.add(dag_id)
    dags: list[DagStatsResponse] = [
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
        for dag_id in sorted(result_dag_ids)
    ]
    return DagStatsCollectionResponse(dags=dags, total_entries=len(dags))
