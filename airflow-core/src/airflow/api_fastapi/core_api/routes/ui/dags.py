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

from fastapi import Depends
from sqlalchemy import and_, func, select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    QueryDagDisplayNamePatternSearch,
    QueryDagIdPatternSearch,
    QueryExcludeStaleFilter,
    QueryLastDagRunStateFilter,
    QueryLimit,
    QueryOffset,
    QueryOwnersFilter,
    QueryPausedFilter,
    QueryTagsFilter,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
from airflow.api_fastapi.core_api.datamodels.dags import DAGResponse
from airflow.api_fastapi.core_api.datamodels.ui.dags import (
    DAGWithLatestDagRunsCollectionResponse,
    DAGWithLatestDagRunsResponse,
)
from airflow.api_fastapi.core_api.security import (
    ReadableDagsFilterDep,
    requires_access_dag,
)
from airflow.models import DagModel, DagRun

dags_router = AirflowRouter(prefix="/dags", tags=["DAG"])


@dags_router.get(
    "/recent_dag_runs",
    response_model_exclude_none=True,
    dependencies=[
        Depends(requires_access_dag(method="GET")),
        Depends(requires_access_dag("GET", DagAccessEntity.RUN)),
    ],
)
def recent_dag_runs(
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_ids: Annotated[
        FilterParam[list[str] | None],
        Depends(filter_param_factory(DagRun.dag_id, list[str] | None, FilterOptionEnum.IN, "dag_ids")),
    ],
    dag_id_pattern: QueryDagIdPatternSearch,
    dag_display_name_pattern: QueryDagDisplayNamePatternSearch,
    exclude_stale: QueryExcludeStaleFilter,
    paused: QueryPausedFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    dag_runs_limit: int = 10,
) -> DAGWithLatestDagRunsCollectionResponse:
    """Get recent DAG runs."""
    recent_runs_subquery = (
        select(
            DagRun.dag_id,
            DagRun.run_after,
            func.rank()
            .over(
                partition_by=DagRun.dag_id,
                order_by=DagRun.run_after.desc(),
            )
            .label("rank"),
        )
        .order_by(DagRun.run_after.desc())
        .subquery()
    )
    dags_with_recent_dag_runs_select = (
        select(
            DagRun,
            DagModel,
            recent_runs_subquery.c.run_after,
        )
        .join(DagModel, DagModel.dag_id == recent_runs_subquery.c.dag_id)
        .join(
            DagRun,
            and_(
                DagRun.dag_id == DagModel.dag_id,
                DagRun.run_after == recent_runs_subquery.c.run_after,
            ),
        )
        .where(recent_runs_subquery.c.rank <= dag_runs_limit)
        .group_by(
            DagModel.dag_id,
            recent_runs_subquery.c.run_after,
            DagRun.run_after,
            DagRun.id,
        )
        .order_by(recent_runs_subquery.c.run_after.desc())
    )
    dags_with_recent_dag_runs_select_filter, _ = paginated_select(
        statement=dags_with_recent_dag_runs_select,
        filters=[
            exclude_stale,
            paused,
            dag_id_pattern,
            dag_ids,
            dag_display_name_pattern,
            tags,
            owners,
            last_dag_run_state,
            readable_dags_filter,
        ],
        order_by=None,
        offset=offset,
        limit=limit,
    )
    dags_with_recent_dag_runs = session.execute(dags_with_recent_dag_runs_select_filter)
    # aggregate rows by dag_id
    dag_runs_by_dag_id: dict[str, DAGWithLatestDagRunsResponse] = {}

    for row in dags_with_recent_dag_runs:
        dag_run, dag, *_ = row
        dag_id = dag.dag_id
        dag_run_response = DAGRunResponse.model_validate(dag_run)
        if dag_id not in dag_runs_by_dag_id:
            dag_response = DAGResponse.model_validate(dag)
            dag_model: DagModel = session.get(DagModel, dag.dag_id)
            dag_runs_by_dag_id[dag_id] = DAGWithLatestDagRunsResponse.model_validate(
                {
                    **dag_response.model_dump(),
                    "asset_expression": dag_model.asset_expression,
                    "latest_dag_runs": [dag_run_response],
                }
            )
        else:
            dag_runs_by_dag_id[dag_id].latest_dag_runs.append(dag_run_response)

    return DAGWithLatestDagRunsCollectionResponse(
        total_entries=len(dag_runs_by_dag_id),
        dags=list(dag_runs_by_dag_id.values()),
    )
