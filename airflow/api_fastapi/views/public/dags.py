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

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import update
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.db.common import (
    get_session,
    paginated_select,
)
from airflow.api_fastapi.db.dags import dags_select_with_latest_dag_run
from airflow.api_fastapi.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.parameters import (
    QueryDagDisplayNamePatternSearch,
    QueryDagIdPatternSearch,
    QueryDagIdPatternSearchWithNone,
    QueryLastDagRunStateFilter,
    QueryLimit,
    QueryOffset,
    QueryOnlyActiveFilter,
    QueryOwnersFilter,
    QueryPausedFilter,
    QueryTagsFilter,
    SortParam,
)
from airflow.api_fastapi.serializers.dags import DAGCollectionResponse, DAGPatchBody, DAGResponse
from airflow.models import DagModel

dags_router = APIRouter(tags=["DAG"])


@dags_router.get("/dags")
async def get_dags(
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_id_pattern: QueryDagIdPatternSearch,
    dag_display_name_pattern: QueryDagDisplayNamePatternSearch,
    only_active: QueryOnlyActiveFilter,
    paused: QueryPausedFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["dag_id", "dag_display_name", "next_dagrun", "last_run_state", "last_run_start_date"]
            ).depends
        ),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> DAGCollectionResponse:
    """Get all DAGs."""
    dags_select, total_entries = paginated_select(
        dags_select_with_latest_dag_run,
        [only_active, paused, dag_id_pattern, dag_display_name_pattern, tags, owners, last_dag_run_state],
        order_by,
        offset,
        limit,
        session,
    )

    dags = session.scalars(dags_select).all()

    return DAGCollectionResponse(
        dags=[DAGResponse.model_validate(dag, from_attributes=True) for dag in dags],
        total_entries=total_entries,
    )


@dags_router.patch("/dags/{dag_id}", responses=create_openapi_http_exception_doc([400, 401, 403, 404]))
async def patch_dag(
    dag_id: str,
    patch_body: DAGPatchBody,
    session: Annotated[Session, Depends(get_session)],
    update_mask: list[str] | None = Query(None),
) -> DAGResponse:
    """Patch the specific DAG."""
    dag = session.get(DagModel, dag_id)

    if dag is None:
        raise HTTPException(404, f"Dag with id: {dag_id} was not found")

    if update_mask:
        if update_mask != ["is_paused"]:
            raise HTTPException(400, "Only `is_paused` field can be updated through the REST API")

    else:
        update_mask = ["is_paused"]

    for attr_name in update_mask:
        attr_value = getattr(patch_body, attr_name)
        setattr(dag, attr_name, attr_value)

    return DAGResponse.model_validate(dag, from_attributes=True)


@dags_router.patch("/dags", responses=create_openapi_http_exception_doc([400, 401, 403, 404]))
async def patch_dags(
    patch_body: DAGPatchBody,
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_id_pattern: QueryDagIdPatternSearchWithNone,
    only_active: QueryOnlyActiveFilter,
    paused: QueryPausedFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    session: Annotated[Session, Depends(get_session)],
    update_mask: list[str] | None = Query(None),
) -> DAGCollectionResponse:
    """Patch multiple DAGs."""
    if update_mask:
        if update_mask != ["is_paused"]:
            raise HTTPException(400, "Only `is_paused` field can be updated through the REST API")
    else:
        update_mask = ["is_paused"]

    dags_select, total_entries = paginated_select(
        dags_select_with_latest_dag_run,
        [only_active, paused, dag_id_pattern, tags, owners, last_dag_run_state],
        None,
        offset,
        limit,
        session,
    )

    dags = session.scalars(dags_select).all()

    dags_to_update = {dag.dag_id for dag in dags}

    session.execute(
        update(DagModel)
        .where(DagModel.dag_id.in_(dags_to_update))
        .values(is_paused=patch_body.is_paused)
        .execution_options(synchronize_session="fetch")
    )

    return DAGCollectionResponse(
        dags=[DAGResponse.model_validate(dag, from_attributes=True) for dag in dags],
        total_entries=total_entries,
    )
