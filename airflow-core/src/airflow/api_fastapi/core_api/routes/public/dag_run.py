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

from typing import Annotated, Literal, cast

import structlog
from fastapi import Depends, HTTPException, Query, Request, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    LimitFilter,
    OffsetFilter,
    QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter,
    QueryLimit,
    QueryOffset,
    Range,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.assets import AssetEventCollectionResponse
from airflow.api_fastapi.core_api.datamodels.dag_run import (
    DAGRunClearBody,
    DAGRunCollectionResponse,
    DAGRunPatchBody,
    DAGRunPatchStates,
    DAGRunResponse,
    DAGRunsBatchBody,
    TriggerDAGRunPostBody,
)
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
    ReadableDagRunsFilterDep,
    requires_access_asset,
    requires_access_dag,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import ParamValidationError
from airflow.listeners.listener import get_listener_manager
from airflow.models import DAG, DagModel, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

log = structlog.get_logger(__name__)

dag_run_router = AirflowRouter(tags=["DagRun"], prefix="/dags/{dag_id}/dagRuns")


@dag_run_router.get(
    "/{dag_run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_dag_run(dag_id: str, dag_run_id: str, session: SessionDep) -> DAGRunResponse:
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    return dag_run


@dag_run_router.delete(
    "/{dag_run_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ],
    ),
    dependencies=[
        Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.RUN)),
        Depends(action_logging()),
    ],
)
def delete_dag_run(dag_id: str, dag_run_id: str, session: SessionDep):
    """Delete a DAG Run entry."""
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))

    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )
    session.delete(dag_run)


@dag_run_router.patch(
    "/{dag_run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ],
    ),
    dependencies=[
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.RUN)),
        Depends(action_logging()),
    ],
)
def patch_dag_run(
    dag_id: str,
    dag_run_id: str,
    patch_body: DAGRunPatchBody,
    session: SessionDep,
    request: Request,
    user: GetUserDep,
    update_mask: list[str] | None = Query(None),
) -> DAGRunResponse:
    """Modify a DAG Run."""
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)

    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    fields_to_update = patch_body.model_fields_set

    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)
    else:
        try:
            DAGRunPatchBody(**patch_body.model_dump())
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    data = patch_body.model_dump(include=fields_to_update, by_alias=True)

    for attr_name, attr_value in data.items():
        if attr_name == "state":
            attr_value = getattr(patch_body, "state")
            if attr_value == DAGRunPatchStates.SUCCESS:
                set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
                try:
                    get_listener_manager().hook.on_dag_run_success(dag_run=dag_run, msg="")
                except Exception:
                    log.exception("error calling listener")
            elif attr_value == DAGRunPatchStates.QUEUED:
                set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
                # Not notifying on queued - only notifying on RUNNING, this is happening in scheduler
            elif attr_value == DAGRunPatchStates.FAILED:
                set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
                try:
                    get_listener_manager().hook.on_dag_run_failed(dag_run=dag_run, msg="")
                except Exception:
                    log.exception("error calling listener")
        elif attr_name == "note":
            dag_run = session.get(DagRun, dag_run.id)
            if dag_run.dag_run_note is None:
                dag_run.note = (attr_value, user.get_id())
            else:
                dag_run.dag_run_note.content = attr_value
                dag_run.dag_run_note.user_id = user.get_id()

    dag_run = session.get(DagRun, dag_run.id)

    return dag_run


@dag_run_router.get(
    "/{dag_run_id}/upstreamAssetEvents",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_access_asset(method="GET")),
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN)),
    ],
)
def get_upstream_asset_events(
    dag_id: str, dag_run_id: str, session: SessionDep
) -> AssetEventCollectionResponse:
    """If dag run is asset-triggered, return the asset events that triggered it."""
    dag_run: DagRun | None = session.scalar(
        select(DagRun).where(
            DagRun.dag_id == dag_id,
            DagRun.run_id == dag_run_id,
        )
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )
    events = dag_run.consumed_asset_events
    return AssetEventCollectionResponse(
        asset_events=events,
        total_entries=len(events),
    )


@dag_run_router.post(
    "/{dag_run_id}/clear",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.RUN)),
        Depends(action_logging()),
    ],
)
def clear_dag_run(
    dag_id: str,
    dag_run_id: str,
    body: DAGRunClearBody,
    request: Request,
    session: SessionDep,
) -> TaskInstanceCollectionResponse | DAGRunResponse:
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)

    if body.dry_run:
        task_instances = dag.clear(
            run_id=dag_run_id,
            task_ids=None,
            only_failed=body.only_failed,
            dry_run=True,
            session=session,
        )

        return TaskInstanceCollectionResponse(
            task_instances=cast("list[TaskInstanceResponse]", task_instances),
            total_entries=len(task_instances),
        )
    dag.clear(
        run_id=dag_run_id,
        task_ids=None,
        only_failed=body.only_failed,
        session=session,
    )
    dag_run_cleared = session.scalar(select(DagRun).where(DagRun.id == dag_run.id))
    return dag_run_cleared


@dag_run_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_dag_runs(
    dag_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    run_after: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DagRun))],
    logical_date: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", DagRun))],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", DagRun))],
    end_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("end_date", DagRun))],
    update_at_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("updated_at", DagRun))],
    run_type: QueryDagRunRunTypesFilter,
    state: QueryDagRunStateFilter,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",
                    "state",
                    "dag_id",
                    "run_id",
                    "logical_date",
                    "run_after",
                    "start_date",
                    "end_date",
                    "updated_at",
                    "conf",
                ],
                DagRun,
                {"dag_run_id": "run_id"},
            ).dynamic_depends(default="id")
        ),
    ],
    readable_dag_runs_filter: ReadableDagRunsFilterDep,
    session: SessionDep,
    request: Request,
) -> DAGRunCollectionResponse:
    """
    Get all DAG Runs.

    This endpoint allows specifying `~` as the dag_id to retrieve Dag Runs for all DAGs.
    """
    query = select(DagRun)

    if dag_id != "~":
        dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
        if not dag:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"The DAG with dag_id: `{dag_id}` was not found")

        query = query.filter(DagRun.dag_id == dag_id)

    dag_run_select, total_entries = paginated_select(
        statement=query,
        filters=[
            run_after,
            logical_date,
            start_date_range,
            end_date_range,
            update_at_range,
            state,
            run_type,
            readable_dag_runs_filter,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    dag_runs = session.scalars(dag_run_select)

    return DAGRunCollectionResponse(
        dag_runs=dag_runs,
        total_entries=total_entries,
    )


@dag_run_router.post(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.RUN)),
        Depends(action_logging()),
    ],
)
def trigger_dag_run(
    dag_id,
    body: TriggerDAGRunPostBody,
    request: Request,
    user: GetUserDep,
    session: SessionDep,
) -> DAGRunResponse:
    """Trigger a DAG."""
    dm = session.scalar(select(DagModel).where(~DagModel.is_stale, DagModel.dag_id == dag_id).limit(1))
    if not dm:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"DAG with dag_id: '{dag_id}' not found")

    if dm.has_import_errors:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"DAG with dag_id: '{dag_id}' has import errors and cannot be triggered",
        )

    try:
        dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
        params = body.validate_context(dag)

        dag_run = dag.create_dagrun(
            run_id=params["run_id"],
            logical_date=params["logical_date"],
            data_interval=params["data_interval"],
            run_after=params["run_after"],
            conf=params["conf"],
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.REST_API,
            state=DagRunState.QUEUED,
            session=session,
        )
        dag_run_note = body.note
        if dag_run_note:
            current_user_id = user.get_id()
            dag_run.note = (dag_run_note, current_user_id)
        return dag_run
    except ValueError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))
    except ParamValidationError as e:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(e))


@dag_run_router.post(
    "/list",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def get_list_dag_runs_batch(
    dag_id: Literal["~"],
    body: DAGRunsBatchBody,
    readable_dag_runs_filter: ReadableDagRunsFilterDep,
    session: SessionDep,
) -> DAGRunCollectionResponse:
    """Get a list of DAG Runs."""
    dag_ids = FilterParam(DagRun.dag_id, body.dag_ids, FilterOptionEnum.IN)
    logical_date = RangeFilter(
        Range(lower_bound=body.logical_date_gte, upper_bound=body.logical_date_lte),
        attribute=DagRun.logical_date,
    )
    run_after = RangeFilter(
        Range(lower_bound=body.run_after_gte, upper_bound=body.run_after_lte),
        attribute=DagRun.run_after,
    )
    start_date = RangeFilter(
        Range(lower_bound=body.start_date_gte, upper_bound=body.start_date_lte),
        attribute=DagRun.start_date,
    )
    end_date = RangeFilter(
        Range(lower_bound=body.end_date_gte, upper_bound=body.end_date_lte),
        attribute=DagRun.end_date,
    )
    state = FilterParam(DagRun.state, body.states, FilterOptionEnum.ANY_EQUAL)

    offset = OffsetFilter(body.page_offset)
    limit = LimitFilter(body.page_limit)

    order_by = SortParam(
        [
            "id",
            "state",
            "dag_id",
            "run_after",
            "logical_date",
            "run_id",
            "start_date",
            "end_date",
            "updated_at",
            "conf",
        ],
        DagRun,
        {"dag_run_id": "run_id"},
    ).set_value(body.order_by)

    base_query = select(DagRun)
    dag_runs_select, total_entries = paginated_select(
        statement=base_query,
        filters=[dag_ids, logical_date, run_after, start_date, end_date, state, readable_dag_runs_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    dag_runs = session.scalars(dag_runs_select)

    return DAGRunCollectionResponse(
        dag_runs=dag_runs,
        total_entries=total_entries,
    )
