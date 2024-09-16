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
from sqlalchemy import or_, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_connexion.parameters import apply_sorting
from airflow.api_fastapi.db import get_session
from airflow.api_fastapi.serializers.dags import DAGCollectionResponse, DAGModelResponse
from airflow.models import DagModel
from airflow.models.dag import DagTag
from airflow.utils.db import get_query_count

dags_router = APIRouter(tags=["DAG"])


@dags_router.get("/dags")
async def get_dags(
    *,
    limit: int = 100,
    offset: int = 0,
    tags: Annotated[list[str] | None, Query()] = None,
    dag_id_pattern: str | None = None,
    only_active: bool = True,
    paused: bool | None = None,
    order_by: str = "dag_id",
    session: Annotated[Session, Depends(get_session)],
) -> DAGCollectionResponse:
    """Get all DAGs."""
    allowed_sorting_attrs = ["dag_id"]
    dags_query = select(DagModel)
    if only_active:
        dags_query = dags_query.where(DagModel.is_active)
    if paused is not None:
        if paused:
            dags_query = dags_query.where(DagModel.is_paused)
        else:
            dags_query = dags_query.where(~DagModel.is_paused)
    if dag_id_pattern:
        dags_query = dags_query.where(DagModel.dag_id.ilike(f"%{dag_id_pattern}%"))

    # TODO: Re-enable when permissions are handled.
    # readable_dags = get_auth_manager().get_permitted_dag_ids(user=g.user)
    # dags_query = dags_query.where(DagModel.dag_id.in_(readable_dags))

    if tags:
        cond = [DagModel.tags.any(DagTag.name == tag) for tag in tags]
        dags_query = dags_query.where(or_(*cond))

    total_entries = get_query_count(dags_query, session=session)
    dags_query = apply_sorting(dags_query, order_by, {}, allowed_sorting_attrs)
    dags = session.scalars(dags_query.offset(offset).limit(limit)).all()

    try:
        return DAGCollectionResponse(
            dags=[DAGModelResponse.model_validate(dag, from_attributes=True) for dag in dags],
            total_entries=total_entries,
        )
    except ValueError as e:
        raise HTTPException(400, f"DAGCollectionSchema error: {str(e)}")
