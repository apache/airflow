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

from http.client import HTTPException
from typing import Annotated

from fastapi import Depends, status
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
    QueryAssetDependencyFilter,
    QueryBundleNameFilter,
    QueryBundleVersionFilter,
    QueryDagDisplayNamePatternSearch,
    QueryDagIdPatternSearch,
    QueryExcludeStaleFilter,
    QueryFavoriteFilter,
    QueryHasAssetScheduleFilter,
    QueryHasImportErrorsFilter,
    QueryLastDagRunStateFilter,
    QueryLimit,
    QueryOffset,
    QueryOwnersFilter,
    QueryPausedFilter,
    QueryPendingActionsFilter,
    QueryTagsFilter,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dags import DAGResponse
from airflow.api_fastapi.core_api.datamodels.ui.dag_runs import DAGRunLightResponse
from airflow.api_fastapi.core_api.datamodels.ui.dags import (
    DAGWithLatestDagRunsCollectionResponse,
    DAGWithLatestDagRunsResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
    ReadableDagsFilterDep,
    requires_access_dag,
)
from airflow.models import DagModel, DagRun
from airflow.models.dag_favorite import DagFavorite
from airflow.models.hitl import HITLDetail
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState

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
    has_import_errors: QueryHasImportErrorsFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    bundle_name: QueryBundleNameFilter,
    bundle_version: QueryBundleVersionFilter,
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
    is_favorite: QueryFavoriteFilter,
    has_asset_schedule: QueryHasAssetScheduleFilter,
    asset_dependency: QueryAssetDependencyFilter,
    has_pending_actions: QueryPendingActionsFilter,
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
    user: GetUserDep,
    dag_runs_limit: int = 10,
) -> DAGWithLatestDagRunsCollectionResponse:
    """Get DAGs with recent DagRun."""
    # Fetch DAGs with their latest DagRun and apply filters
    query = generate_dag_with_latest_run_query(
        max_run_filters=[
            last_dag_run_state,
        ],
        order_by=order_by,
        dag_ids=readable_dags_filter.value,
    )

    dags_select, total_entries = paginated_select(
        statement=query,
        filters=[
            exclude_stale,
            paused,
            has_import_errors,
            dag_id_pattern,
            dag_ids,
            dag_display_name_pattern,
            tags,
            owners,
            last_dag_run_state,
            is_favorite,
            has_asset_schedule,
            asset_dependency,
            has_pending_actions,
            readable_dags_filter,
            bundle_name,
            bundle_version,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    dags = [dag for dag in session.scalars(dags_select)]

    # Fetch favorite status for each DAG for the current user
    user_id = str(user.get_id())
    favorites_select = select(DagFavorite.dag_id).where(
        DagFavorite.user_id == user_id, DagFavorite.dag_id.in_([dag.dag_id for dag in dags])
    )
    favorite_dag_ids = set(session.scalars(favorites_select))

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
            DagRun.id,
            DagRun.dag_id,
            DagRun.run_id,
            DagRun.end_date,
            DagRun.logical_date,
            DagRun.run_after,
            DagRun.start_date,
            DagRun.state,
            DagRun.duration.expression,  # type: ignore[attr-defined]
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

    # Fetch pending HITL actions for each Dag if we are not certain whether some of the Dag might contain HITL actions
    pending_actions_by_dag_id: dict[str, list[HITLDetail]] = {dag.dag_id: [] for dag in dags}
    if has_pending_actions.value:
        pending_actions_select = (
            select(
                TaskInstance.dag_id,
                HITLDetail,
            )
            .join(TaskInstance, HITLDetail.ti_id == TaskInstance.id)
            .where(
                HITLDetail.responded_at.is_(None),
                TaskInstance.state == TaskInstanceState.DEFERRED,
            )
            .where(TaskInstance.dag_id.in_([dag.dag_id for dag in dags]))
            .order_by(TaskInstance.dag_id)
        )

        pending_actions = session.execute(pending_actions_select)

        # Group pending actions by dag_id
        for dag_id, hitl_detail in pending_actions:
            pending_actions_by_dag_id[dag_id].append(hitl_detail)

    # aggregate rows by dag_id
    dag_runs_by_dag_id: dict[str, DAGWithLatestDagRunsResponse] = {
        dag.dag_id: DAGWithLatestDagRunsResponse.model_validate(
            {
                **DAGResponse.model_validate(dag).model_dump(),
                "asset_expression": dag.asset_expression,
                "latest_dag_runs": [],
                "pending_actions": pending_actions_by_dag_id[dag.dag_id],
                "is_favorite": dag.dag_id in favorite_dag_ids,
            }
        )
        for dag in dags
    }

    for row in recent_dag_runs:
        dag_run_response = DAGRunLightResponse.model_validate(row)
        dag_id = dag_run_response.dag_id
        dag_runs_by_dag_id[dag_id].latest_dag_runs.append(dag_run_response)

    return DAGWithLatestDagRunsCollectionResponse(
        total_entries=total_entries,
        dags=list(dag_runs_by_dag_id.values()),
    )


@dags_router.get(
    "/{dag_id}/latest_run",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_latest_run_info(dag_id: str, session: SessionDep) -> DAGRunLightResponse | None:
    """Get latest run."""
    if dag_id == "~":
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "`~` was supplied as dag_id, but querying multiple dags is not supported.",
        )

    latest_run_info_select = (
        select(
            DagRun.id,
            DagRun.dag_id,
            DagRun.run_id,
            DagRun.end_date,
            DagRun.logical_date,
            DagRun.run_after,
            DagRun.start_date,
            DagRun.state,
        )
        .where(DagRun.dag_id == dag_id)
        .order_by(DagRun.run_after.desc())
        .limit(1)
    )
    latest_run_info = session.execute(latest_run_info_select).one_or_none()

    return DAGRunLightResponse(**latest_run_info._mapping) if latest_run_info else None
