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
from sqlalchemy import delete, func, insert, select, update

from airflow.api.common import delete_dag as delete_dag_module
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
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
    QueryDagIdPatternSearchWithNone,
    QueryExcludeStaleFilter,
    QueryFavoriteFilter,
    QueryHasAssetScheduleFilter,
    QueryHasImportErrorsFilter,
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
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.api_fastapi.core_api.datamodels.dags import (
    DAGCollectionResponse,
    DAGDetailsResponse,
    DAGPatchBody,
    DAGResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    EditableDagsFilterDep,
    GetUserDep,
    ReadableDagsFilterDep,
    requires_access_dag,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models import DagModel
from airflow.models.dag_favorite import DagFavorite
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

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
    has_import_errors: QueryHasImportErrorsFilter,
    last_dag_run_state: QueryLastDagRunStateFilter,
    bundle_name: QueryBundleNameFilter,
    bundle_version: QueryBundleVersionFilter,
    has_asset_schedule: QueryHasAssetScheduleFilter,
    asset_dependency: QueryAssetDependencyFilter,
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
    is_favorite: QueryFavoriteFilter,
) -> DAGCollectionResponse:
    """Get all DAGs."""
    query = generate_dag_with_latest_run_query(
        max_run_filters=[
            dag_run_start_date_range,
            dag_run_end_date_range,
            dag_run_state,
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
            dag_display_name_pattern,
            tags,
            is_favorite,
            owners,
            readable_dags_filter,
            bundle_name,
            bundle_version,
            has_asset_schedule,
            asset_dependency,
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
            HTTP_422_UNPROCESSABLE_CONTENT,
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
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)
    dag_model = session.get(DagModel, dag_id)
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
def get_dag_details(
    dag_id: str, session: SessionDep, dag_bag: DagBagDep, user: GetUserDep
) -> DAGDetailsResponse:
    """Get details of DAG."""
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)

    dag_model = session.get(DagModel, dag_id)
    if not dag_model:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Unable to obtain dag with id {dag_id} from session")

    for key, value in dag.__dict__.items():
        if not key.startswith("_") and not hasattr(dag_model, key):
            setattr(dag_model, key, value)

    # Check if this DAG is marked as favorite by the current user
    user_id = str(user.get_id())
    is_favorite = (
        session.scalar(
            select(DagFavorite.dag_id).where(DagFavorite.user_id == user_id, DagFavorite.dag_id == dag_id)
        )
        is not None
    )

    # Count active (running + queued) DAG runs for this DAG
    active_runs_count = (
        session.scalar(
            select(func.count())
            .select_from(DagRun)
            .where(DagRun.dag_id == dag_id, DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED]))
        )
        or 0
    )

    # Add is_favorite and active_runs_count fields to the DAG model
    setattr(dag_model, "is_favorite", is_favorite)
    setattr(dag_model, "active_runs_count", active_runs_count)

    return DAGDetailsResponse.model_validate(dag_model)


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


@dags_router.post(
    "/{dag_id}/favorite",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET")), Depends(action_logging())],
)
def favorite_dag(dag_id: str, session: SessionDep, user: GetUserDep):
    """Mark the DAG as favorite."""
    dag = session.get(DagModel, dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"DAG with id '{dag_id}' not found")

    user_id = str(user.get_id())
    session.execute(insert(DagFavorite).values(dag_id=dag_id, user_id=user_id))


@dags_router.post(
    "/{dag_id}/unfavorite",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]),
    dependencies=[Depends(requires_access_dag(method="GET")), Depends(action_logging())],
)
def unfavorite_dag(dag_id: str, session: SessionDep, user: GetUserDep):
    """Unmark the DAG as favorite."""
    dag = session.get(DagModel, dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"DAG with id '{dag_id}' not found")

    user_id = str(user.get_id())

    favorite_exists = session.execute(
        select(DagFavorite).where(
            DagFavorite.dag_id == dag_id,
            DagFavorite.user_id == user_id,
        )
    ).first()

    if not favorite_exists:
        raise HTTPException(status.HTTP_409_CONFLICT, detail="DAG is not marked as favorite")

    session.execute(
        delete(DagFavorite).where(
            DagFavorite.dag_id == dag_id,
            DagFavorite.user_id == user_id,
        )
    )


@dags_router.delete(
    "/{dag_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            HTTP_422_UNPROCESSABLE_CONTENT,
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
