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
from airflow.api_fastapi.common.db.dags import generate_dag_with_latest_run_query
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
    SortParam,
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
    "",
    response_model_exclude_none=True,
    dependencies=[
        Depends(requires_access_dag(method="GET")),
        Depends(requires_access_dag("GET", DagAccessEntity.RUN)),
    ],
    operation_id="get_dags_ui",
)
def get_dags(
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_ids: Annotated[
        FilterParam[list[str] | None],
        Depends(filter_param_factory(DagModel.dag_id, list[str] | None, FilterOptionEnum.IN, "dag_ids")),
    ],
    dag_id_pattern: QueryDagIdPatternSearch,
    dag_display_name_pattern: QueryDagDisplayNamePatternSearch,
    exclude_stale: QueryExcludeStaleFilter,
    paused: QueryPausedFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["dag_id", "dag_display_name", "next_dagrun", "state", "start_date"],
                DagModel,
                {"last_run_state": DagRun.state, "last_run_start_date": DagRun.start_date},
            ).dynamic_depends()
        ),
    ],
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    dag_runs_limit: int = 10,
) -> DAGWithLatestDagRunsCollectionResponse:
    """Get DAGs with recent DagRun."""
    # Fetch DAGs with their latest DagRun and apply filters
    query = generate_dag_with_latest_run_query(
        max_run_filters=[
            last_dag_run_state,
        ],
        order_by=order_by,
    )

    dags_select, total_entries = paginated_select(
        statement=query,
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
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    dags = [dag for dag in session.scalars(dags_select)]

    # Populate the last 'dag_runs_limit' DagRuns for each DAG
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
        .where(DagRun.dag_id.in_([dag.dag_id for dag in dags]))
        .order_by(DagRun.run_after.desc())
        .subquery()
    )

    recent_dag_runs_select = (
        select(
            recent_runs_subquery.c.run_after,
            DagRun,
        )
        .join(
            DagRun,
            and_(
                DagRun.dag_id == recent_runs_subquery.c.dag_id,
                DagRun.run_after == recent_runs_subquery.c.run_after,
            ),
        )
        .where(recent_runs_subquery.c.rank <= dag_runs_limit)
        .group_by(
            recent_runs_subquery.c.run_after,
            DagRun.run_after,
            DagRun.id,
        )
        .order_by(recent_runs_subquery.c.run_after.desc())
    )

    recent_dag_runs = session.execute(recent_dag_runs_select)

    # aggregate rows by dag_id
    dag_runs_by_dag_id: dict[str, DAGWithLatestDagRunsResponse] = {
        dag.dag_id: DAGWithLatestDagRunsResponse.model_validate(
            {
                **DAGResponse.model_validate(dag).model_dump(),
                "asset_expression": dag.asset_expression,
                "latest_dag_runs": [],
            }
        )
        for dag in dags
    }

    for row in recent_dag_runs:
        _, dag_run = row
        dag_id = dag_run.dag_id
        dag_run_response = DAGRunResponse.model_validate(dag_run)
        dag_runs_by_dag_id[dag_id].latest_dag_runs.append(dag_run_response)

    return DAGWithLatestDagRunsCollectionResponse(
        total_entries=total_entries,
        dags=list(dag_runs_by_dag_id.values()),
    )
