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
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import (
    get_session,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    QueryDagDisplayNamePatternSearch,
    QueryDagIdPatternSearch,
    QueryLastDagRunStateFilter,
    QueryLimit,
    QueryOffset,
    QueryOnlyActiveFilter,
    QueryOwnersFilter,
    QueryPausedFilter,
    QueryTagsFilter,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.serializers.dag_run import DAGRunResponse
from airflow.api_fastapi.core_api.serializers.dags import DAGResponse
from airflow.api_fastapi.core_api.serializers.ui.dags import (
    DAGWithLatestDagRunsCollectionResponse,
    DAGWithLatestDagRunsResponse,
)
from airflow.models import DagModel, DagRun

dags_router = AirflowRouter(prefix="/dags", tags=["Dags"])


@dags_router.get("/recent_dag_runs", include_in_schema=False, response_model_exclude_none=True)
async def recent_dag_runs(
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_id_pattern: QueryDagIdPatternSearch,
    dag_display_name_pattern: QueryDagDisplayNamePatternSearch,
    only_active: QueryOnlyActiveFilter,
    paused: QueryPausedFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    session: Annotated[Session, Depends(get_session)],
    dag_runs_limit: int = 10,
) -> DAGWithLatestDagRunsCollectionResponse:
    """Get recent DAG runs."""
    recent_runs_subquery = (
        select(
            DagRun.dag_id,
            DagRun.execution_date,
            func.rank()
            .over(
                partition_by=DagRun.dag_id,
                order_by=DagRun.execution_date.desc(),
            )
            .label("rank"),
        )
        .order_by(DagRun.execution_date.desc())
        .subquery()
    )
    dags_with_recent_dag_runs_select = (
        select(
            DagRun,
            DagModel,
            recent_runs_subquery.c.execution_date,
        )
        .join(DagModel, DagModel.dag_id == recent_runs_subquery.c.dag_id)
        .join(
            DagRun,
            and_(
                DagRun.dag_id == DagModel.dag_id,
                DagRun.execution_date == recent_runs_subquery.c.execution_date,
            ),
        )
        .where(recent_runs_subquery.c.rank <= dag_runs_limit)
        .group_by(
            DagModel.dag_id,
            recent_runs_subquery.c.execution_date,
            DagRun.execution_date,
            DagRun.id,
        )
        .order_by(recent_runs_subquery.c.execution_date.desc())
    )
    dags_with_recent_dag_runs_select_filter, _ = paginated_select(
        dags_with_recent_dag_runs_select,
        [only_active, paused, dag_id_pattern, dag_display_name_pattern, tags, owners, last_dag_run_state],
        None,
        offset,
        limit,
    )
    dags_with_recent_dag_runs = session.execute(dags_with_recent_dag_runs_select_filter)
    # aggregate rows by dag_id
    dag_runs_by_dag_id: dict[str, DAGWithLatestDagRunsResponse] = {}

    for row in dags_with_recent_dag_runs:
        dag_run, dag, *_ = row
        dag_id = dag.dag_id
        dag_run_response = DAGRunResponse.model_validate(dag_run, from_attributes=True)
        if dag_id not in dag_runs_by_dag_id:
            dag_response = DAGResponse.model_validate(dag, from_attributes=True)
            dag_runs_by_dag_id[dag_id] = DAGWithLatestDagRunsResponse.model_validate(
                {
                    **dag_response.dict(),
                    "latest_dag_runs": [dag_run_response],
                }
            )
        else:
            dag_runs_by_dag_id[dag_id].latest_dag_runs.append(dag_run_response)

    return DAGWithLatestDagRunsCollectionResponse(
        total_entries=len(dag_runs_by_dag_id),
        dags=list(dag_runs_by_dag_id.values()),
    )
