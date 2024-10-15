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

from fastapi import Depends, HTTPException, Query, Request, Response
from sqlalchemy import update
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api.common import delete_dag as delete_dag_module
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
from airflow.api_fastapi.serializers.dags import (
    DAGCollectionResponse,
    DAGDetailsResponse,
    DAGPatchBody,
    DAGResponse,
)
from airflow.api_fastapi.views.router import AirflowRouter
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models import DAG, DagModel

dags_router = AirflowRouter(tags=["DAG"], prefix="/dags")


@dags_router.get("/")
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


@dags_router.get("/{dag_id}", responses=create_openapi_http_exception_doc([400, 401, 403, 404, 422]))
async def get_dag(
    dag_id: str, session: Annotated[Session, Depends(get_session)], request: Request
) -> DAGResponse:
    """Get basic information about a DAG."""
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(404, f"Dag with id {dag_id} was not found")

    dag_model: DagModel = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(404, f"Unable to obtain dag with id {dag_id} from session")

    for key, value in dag.__dict__.items():
        if not key.startswith("_") and not hasattr(dag_model, key):
            setattr(dag_model, key, value)

    return DAGResponse.model_validate(dag_model, from_attributes=True)


@dags_router.get("/{dag_id}/details", responses=create_openapi_http_exception_doc([400, 401, 403, 404, 422]))
async def get_dag_details(
    dag_id: str, session: Annotated[Session, Depends(get_session)], request: Request
) -> DAGDetailsResponse:
    """Get details of DAG."""
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(404, f"Dag with id {dag_id} was not found")

    dag_model: DagModel = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(404, f"Unable to obtain dag with id {dag_id} from session")

    for key, value in dag.__dict__.items():
        if not key.startswith("_") and not hasattr(dag_model, key):
            setattr(dag_model, key, value)

    return DAGDetailsResponse.model_validate(dag_model, from_attributes=True)


@dags_router.patch("/{dag_id}", responses=create_openapi_http_exception_doc([400, 401, 403, 404]))
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


@dags_router.patch("/", responses=create_openapi_http_exception_doc([400, 401, 403, 404]))
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


@dags_router.delete("/{dag_id}", responses=create_openapi_http_exception_doc([400, 401, 403, 404, 422]))
async def delete_dag(
    dag_id: str,
    session: Annotated[Session, Depends(get_session)],
) -> Response:
    """Delete the specific DAG."""
    try:
        delete_dag_module.delete_dag(dag_id, session=session)
    except DagNotFound:
        raise HTTPException(404, f"Dag with id: {dag_id} was not found")
    except AirflowException:
        raise HTTPException(409, f"Task instances of dag with id: '{dag_id}' are still running")
    return Response(status_code=204)
