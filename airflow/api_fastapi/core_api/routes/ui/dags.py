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
from airflow.api_fastapi.core_api.serializers.ui.dags import (
    RecentDAGRun,
    RecentDAGRunsCollectionResponse,
    RecentDAGRunsResponse,
)
from airflow.models import DagModel, DagRun

dags_router = AirflowRouter(prefix="/dags", tags=["Dags"])


@dags_router.get("/recent_dag_runs", include_in_schema=False)
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
) -> RecentDAGRunsCollectionResponse:
    """Get recent DAG runs."""
    recent_runs_subquery = (
        select(
            DagRun.dag_id,
            func.max(DagRun.execution_date).label("max_execution_date"),
        )
        .group_by(DagRun.dag_id)
        .subquery("last_runs")
    )
    dags_select_with_recent_dag_run = (
        select(
            DagModel.dag_id,
            DagRun.start_date,
            DagRun.end_date,
            DagRun.state,
            DagRun.execution_date,
            DagRun.data_interval_start,
            DagRun.data_interval_end,
        )
        .join(
            DagRun,
            DagModel.dag_id == DagRun.dag_id,
        )
        .join(
            recent_runs_subquery,
            and_(
                recent_runs_subquery.c.dag_id == DagModel.dag_id,
                recent_runs_subquery.c.max_execution_date == DagRun.execution_date,
            ),
        )
    )
    recent_dags_select, total_entries = paginated_select(
        dags_select_with_recent_dag_run,
        [only_active, paused, dag_id_pattern, dag_display_name_pattern, tags, owners, last_dag_run_state],
        None,
        offset,
        limit,
    )

    dag_runs = session.execute(recent_dags_select).all()
    print("dag_runs\n", dag_runs)
    return RecentDAGRunsCollectionResponse(
        total_entries=total_entries,
        recent_dag_runs=[
            RecentDAGRunsResponse(
                dag_id=row.dag_id,
                dag_runs=[
                    RecentDAGRun(
                        dag_id=row.dag_id,
                        start_date=row.start_date,
                        end_date=row.end_date,
                        state=row.state,
                        execution_date=row.execution_date,
                        data_interval_start=row.data_interval_start,
                        data_interval_end=row.data_interval_end,
                    )
                ],
            )
            for row in dag_runs
        ],
    )
