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
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.db import apply_filters_to_select, get_session, latest_dag_run_per_dag_id_cte
from airflow.api_fastapi.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.parameters import (
    QueryDagDisplayNamePatternSearch,
    QueryDagIdPatternSearch,
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
from airflow.models.dagrun import DagRun
from airflow.utils.db import get_query_count

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
    dags_query = (
        select(DagModel)
        .join(
            latest_dag_run_per_dag_id_cte,
            DagModel.dag_id == latest_dag_run_per_dag_id_cte.c.dag_id,
            isouter=True,
        )
        .join(
            DagRun,
            DagRun.start_date == latest_dag_run_per_dag_id_cte.c.start_date
            and DagRun.dag_id == latest_dag_run_per_dag_id_cte.c.dag_id,
            isouter=True,
        )
    )

    dags_query = apply_filters_to_select(
        dags_query,
        [only_active, paused, dag_id_pattern, dag_display_name_pattern, tags, owners, last_dag_run_state],
    )

    # TODO: Re-enable when permissions are handled.
    # readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)
    # dags_query = dags_query.where(DagModel.dag_id.in_(readable_dags))

    total_entries = get_query_count(dags_query, session=session)

    dags_query = apply_filters_to_select(dags_query, [order_by, offset, limit])

    dags = session.scalars(dags_query).all()

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
    """Update the specific DAG."""
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
