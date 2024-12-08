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

from fastapi import Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import update

from airflow.api.common import delete_dag as delete_dag_module
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    paginated_select,
)
from airflow.api_fastapi.common.db.dags import dags_select_with_latest_dag_run
from airflow.api_fastapi.common.parameters import (
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
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dags import (
    DAGCollectionResponse,
    DAGDetailsResponse,
    DAGPatchBody,
    DAGResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models import DAG, DagModel
from airflow.models.dagrun import DagRun

dags_router = AirflowRouter(tags=["DAG"], prefix="/dags")


@dags_router.get("")
def get_dags(
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
                ["dag_id", "dag_display_name", "next_dagrun", "state", "start_date"],
                DagModel,
                {"last_run_state": DagRun.state, "last_run_start_date": DagRun.start_date},
            ).dynamic_depends()
        ),
    ],
    session: SessionDep,
) -> DAGCollectionResponse:
    """Get all DAGs."""
    dags_select, total_entries = paginated_select(
        statement=dags_select_with_latest_dag_run,
        filters=[
            only_active,
            paused,
            dag_id_pattern,
            dag_display_name_pattern,
            tags,
            owners,
            last_dag_run_state,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    dags = session.scalars(dags_select)

    return DAGCollectionResponse(
        dags=dags,
        total_entries=total_entries,
    )


@dags_router.get(
    "/{dag_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_422_UNPROCESSABLE_ENTITY,
        ]
    ),
)
def get_dag(dag_id: str, session: SessionDep, request: Request) -> DAGResponse:
    """Get basic information about a DAG."""
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    dag_model: DagModel = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Unable to obtain dag with id {dag_id} from session")

    for key, value in dag.__dict__.items():
        if not key.startswith("_") and not hasattr(dag_model, key):
            setattr(dag_model, key, value)

    return dag_model


@dags_router.get(
    "/{dag_id}/details",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_dag_details(dag_id: str, session: SessionDep, request: Request) -> DAGDetailsResponse:
    """Get details of DAG."""
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    dag_model: DagModel = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Unable to obtain dag with id {dag_id} from session")

    for key, value in dag.__dict__.items():
        if not key.startswith("_") and not hasattr(dag_model, key):
            setattr(dag_model, key, value)

    return dag_model


@dags_router.patch(
    "/{dag_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def patch_dag(
    dag_id: str,
    patch_body: DAGPatchBody,
    session: SessionDep,
    update_mask: list[str] | None = Query(None),
) -> DAGResponse:
    """Patch the specific DAG."""
    dag = session.get(DagModel, dag_id)

    if dag is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id: {dag_id} was not found")

    if update_mask:
        if update_mask != ["is_paused"]:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Only `is_paused` field can be updated through the REST API"
            )

        data = patch_body.model_dump(include=set(update_mask))
    else:
        data = patch_body.model_dump()

    for key, val in data.items():
        setattr(dag, key, val)

    return dag


@dags_router.patch(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def patch_dags(
    patch_body: DAGPatchBody,
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_id_pattern: QueryDagIdPatternSearchWithNone,
    only_active: QueryOnlyActiveFilter,
    paused: QueryPausedFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    session: SessionDep,
    update_mask: list[str] | None = Query(None),
) -> DAGCollectionResponse:
    """Patch multiple DAGs."""
    if update_mask:
        if update_mask != ["is_paused"]:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Only `is_paused` field can be updated through the REST API"
            )
    else:
        # todo: this is not used?
        update_mask = ["is_paused"]

    dags_select, total_entries = paginated_select(
        statement=dags_select_with_latest_dag_run,
        filters=[only_active, paused, dag_id_pattern, tags, owners, last_dag_run_state],
        order_by=None,
        offset=offset,
        limit=limit,
        session=session,
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
        dags=dags,
        total_entries=total_entries,
    )


@dags_router.delete(
    "/{dag_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_422_UNPROCESSABLE_ENTITY,
        ]
    ),
)
def delete_dag(
    dag_id: str,
    session: SessionDep,
) -> Response:
    """Delete the specific DAG."""
    try:
        delete_dag_module.delete_dag(dag_id, session=session)
    except DagNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id: {dag_id} was not found")
    except AirflowException:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"Task instances of dag with id: '{dag_id}' are still running"
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
