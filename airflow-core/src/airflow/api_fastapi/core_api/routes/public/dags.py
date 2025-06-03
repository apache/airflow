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

from fastapi import Depends, HTTPException, Query, Response, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import func, null, select, update

from airflow.api.common import delete_dag as delete_dag_module
from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    apply_filters_to_select,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    QueryDagDisplayNamePatternSearch,
    QueryDagIdPatternSearch,
    QueryDagIdPatternSearchWithNone,
    QueryExcludeStaleFilter,
    QueryLastDagRunStateFilter,
    QueryLimit,
    QueryOffset,
    QueryOwnersFilter,
    QueryPausedFilter,
    QueryTagsFilter,
    RangeFilter,
    SortParam,
    _transform_dag_run_states,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dags import (
    DAGCollectionResponse,
    DAGDetailsResponse,
    DAGPatchBody,
    DAGResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    EditableDagsFilterDep,
    ReadableDagsFilterDep,
    requires_access_dag,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models import DAG, DagModel
from airflow.models.dagrun import DagRun

dags_router = AirflowRouter(tags=["DAG"], prefix="/dags")


@dags_router.get("", dependencies=[Depends(requires_access_dag(method="GET"))])
def get_dags(
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_id_pattern: QueryDagIdPatternSearch,
    dag_display_name_pattern: QueryDagDisplayNamePatternSearch,
    exclude_stale: QueryExcludeStaleFilter,
    paused: QueryPausedFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    dag_run_start_date_range: Annotated[
        RangeFilter, Depends(datetime_range_filter_factory("dag_run_start_date", DagRun, "start_date"))
    ],
    dag_run_end_date_range: Annotated[
        RangeFilter, Depends(datetime_range_filter_factory("dag_run_end_date", DagRun, "end_date"))
    ],
    dag_run_state: Annotated[
        FilterParam[list[str]],
        Depends(
            filter_param_factory(
                DagRun.state,
                list[str],
                FilterOptionEnum.ANY_EQUAL,
                "dag_run_state",
                default_factory=list,
                transform_callable=_transform_dag_run_states,
            )
        ),
    ],
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
) -> DAGCollectionResponse:
    """Get all DAGs."""
    query = select(DagModel)

    max_run_id_query = (  # ordering by id will not always be "latest run", but it's a simplifying assumption
        select(DagRun.dag_id, func.max(DagRun.id).label("max_dag_run_id"))
        .where(DagRun.start_date.is_not(null()))
        .group_by(DagRun.dag_id)
        .subquery(name="mrq")
    )

    has_max_run_filter = (
        dag_run_state.value
        or last_dag_run_state.value
        or dag_run_start_date_range.is_active()
        or dag_run_end_date_range.is_active()
    )

    if has_max_run_filter or order_by.value in (
        "last_run_state",
        "last_run_start_date",
        "-last_run_state",
        "-last_run_start_date",
    ):
        query = query.join(
            max_run_id_query,
            DagModel.dag_id == max_run_id_query.c.dag_id,
            isouter=True,
        ).join(DagRun, DagRun.id == max_run_id_query.c.max_dag_run_id, isouter=True)

    if has_max_run_filter:
        query = apply_filters_to_select(
            statement=query,
            filters=[
                dag_run_start_date_range,
                dag_run_end_date_range,
                dag_run_state,
                last_dag_run_state,
            ],
        )

    dags_select, total_entries = paginated_select(
        statement=query,
        filters=[
            exclude_stale,
            paused,
            dag_id_pattern,
            dag_display_name_pattern,
            tags,
            owners,
            readable_dags_filter,
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
    dependencies=[Depends(requires_access_dag(method="GET"))],
)
def get_dag(
    dag_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> DAGResponse:
    """Get basic information about a DAG."""
    dag: DAG = dag_bag.get_dag(dag_id)
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
    dependencies=[Depends(requires_access_dag(method="GET"))],
)
def get_dag_details(dag_id: str, session: SessionDep, dag_bag: DagBagDep) -> DAGDetailsResponse:
    """Get details of DAG."""
    dag: DAG = dag_bag.get_dag(dag_id)
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
    dependencies=[Depends(requires_access_dag(method="PUT")), Depends(action_logging())],
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

    fields_to_update = patch_body.model_fields_set
    if update_mask:
        if update_mask != ["is_paused"]:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Only `is_paused` field can be updated through the REST API"
            )
        fields_to_update = fields_to_update.intersection(update_mask)
    else:
        try:
            DAGPatchBody(**patch_body.model_dump())
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    data = patch_body.model_dump(include=fields_to_update, by_alias=True)

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
    dependencies=[Depends(requires_access_dag(method="PUT")), Depends(action_logging())],
)
def patch_dags(
    patch_body: DAGPatchBody,
    limit: QueryLimit,
    offset: QueryOffset,
    tags: QueryTagsFilter,
    owners: QueryOwnersFilter,
    dag_id_pattern: QueryDagIdPatternSearchWithNone,
    exclude_stale: QueryExcludeStaleFilter,
    paused: QueryPausedFilter,
    editable_dags_filter: EditableDagsFilterDep,
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
        try:
            DAGPatchBody.model_validate(patch_body)
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    dags_select, total_entries = paginated_select(
        statement=select(DagModel),
        filters=[
            exclude_stale,
            paused,
            dag_id_pattern,
            tags,
            owners,
            editable_dags_filter,
        ],
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
    dependencies=[Depends(requires_access_dag(method="DELETE")), Depends(action_logging())],
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
