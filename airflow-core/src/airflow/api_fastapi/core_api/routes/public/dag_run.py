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

import textwrap
from typing import Annotated, Literal, cast

import structlog
from fastapi import Depends, HTTPException, Query, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import StreamingResponse
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    LimitFilter,
    OffsetFilter,
    QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter,
    QueryDagRunVersionFilter,
    QueryLimit,
    QueryOffset,
    Range,
    RangeFilter,
    SortParam,
    _SearchParam,
    datetime_range_filter_factory,
    search_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.common.types import Mimetype
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
from airflow.api_fastapi.core_api.services.public.dag_run import DagRunWaiter
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import ParamValidationError
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagModel, DagRun
from airflow.models.dag_version import DagVersion
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
    dag_run = session.scalar(
        select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id).options(joinedload(DagRun.dag_model))
    )
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
    dag_bag: DagBagDep,
    user: GetUserDep,
    update_mask: list[str] | None = Query(None),
) -> DAGRunResponse:
    """Modify a DAG Run."""
    dag_run = session.scalar(
        select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id).options(joinedload(DagRun.dag_model))
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag = get_dag_for_run(dag_bag, dag_run, session=session)

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
    dag_bag: DagBagDep,
    session: SessionDep,
) -> TaskInstanceCollectionResponse | DAGRunResponse:
    dag_run = session.scalar(
        select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id).options(joinedload(DagRun.dag_model))
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag = dag_bag.get_dag_for_run(dag_run, session=session)

    if body.dry_run:
        if not dag:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")
        task_instances = dag.clear(
            run_id=dag_run_id,
            task_ids=None,
            only_failed=body.only_failed,
            run_on_latest_version=body.run_on_latest_version,
            dry_run=True,
            session=session,
        )

        return TaskInstanceCollectionResponse(
            task_instances=cast("list[TaskInstanceResponse]", task_instances),
            total_entries=len(task_instances),
        )
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")
    dag.clear(
        run_id=dag_run_id,
        task_ids=None,
        only_failed=body.only_failed,
        run_on_latest_version=body.run_on_latest_version,
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
    dag_version: QueryDagRunVersionFilter,
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
                    "duration",
                ],
                DagRun,
                {"dag_run_id": "run_id"},
            ).dynamic_depends(default="id")
        ),
    ],
    readable_dag_runs_filter: ReadableDagRunsFilterDep,
    session: SessionDep,
    dag_bag: DagBagDep,
    run_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(DagRun.run_id, "run_id_pattern"))],
    triggering_user_name_pattern: Annotated[
        _SearchParam,
        Depends(search_param_factory(DagRun.triggering_user_name, "triggering_user_name_pattern")),
    ],
) -> DAGRunCollectionResponse:
    """
    Get all DAG Runs.

    This endpoint allows specifying `~` as the dag_id to retrieve Dag Runs for all DAGs.
    """
    query = select(DagRun)

    if dag_id != "~":
        get_latest_version_of_dag(dag_bag, dag_id, session)  # Check if the DAG exists.
        query = query.filter(DagRun.dag_id == dag_id).options(joinedload(DagRun.dag_model))

    # Add join with DagVersion if dag_version filter is active
    if dag_version.value:
        query = query.join(DagVersion, DagRun.created_dag_version_id == DagVersion.id)

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
            dag_version,
            readable_dag_runs_filter,
            run_id_pattern,
            triggering_user_name_pattern,
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
    dag_bag: DagBagDep,
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
        dag = get_latest_version_of_dag(dag_bag, dag_id, session)
        params = body.validate_context(dag)

        dag_run = dag.create_dagrun(
            run_id=params["run_id"],
            logical_date=params["logical_date"],
            data_interval=params["data_interval"],
            run_after=params["run_after"],
            conf=params["conf"],
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.REST_API,
            triggering_user_name=user.get_name(),
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


@dag_run_router.get(
    "/{dag_run_id}/wait",
    tags=["experimental"],
    summary="Experimental: Wait for a dag run to complete, and return task results if requested.",
    description="🚧 This is an experimental endpoint and may change or be removed without notice.Successful response are streamed as newline-delimited JSON (NDJSON). Each line is a JSON object representing the DAG run state.",
    responses={
        **create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
        status.HTTP_200_OK: {
            "description": "Successful Response",
            "content": {
                Mimetype.NDJSON: {
                    "schema": {
                        "type": "string",
                        "example": textwrap.dedent(
                            """\
                {"state": "running"}
                {"state": "success", "results": {"op": 42}}
                """
                        ),
                    }
                }
            },
        },
    },
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN))],
)
def wait_dag_run_until_finished(
    dag_id: str,
    dag_run_id: str,
    session: SessionDep,
    interval: Annotated[float, Query(gt=0.0, description="Seconds to wait between dag run state checks")],
    result_task_ids: Annotated[
        list[str] | None,
        Query(alias="result", description="Collect result XCom from task. Can be set multiple times."),
    ] = None,
):
    "Wait for a dag run until it finishes, and return its result(s)."
    if not session.scalar(select(1).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id)):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )
    waiter = DagRunWaiter(
        dag_id=dag_id,
        run_id=dag_run_id,
        interval=interval,
        result_task_ids=result_task_ids,
        session=session,
    )
    return StreamingResponse(waiter.wait())


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
        Range(
            lower_bound_gte=body.logical_date_gte,
            lower_bound_gt=body.logical_date_gt,
            upper_bound_lte=body.logical_date_lte,
            upper_bound_lt=body.logical_date_lt,
        ),
        attribute=DagRun.logical_date,
    )
    run_after = RangeFilter(
        Range(
            lower_bound_gte=body.run_after_gte,
            lower_bound_gt=body.run_after_gt,
            upper_bound_lte=body.run_after_lte,
            upper_bound_lt=body.run_after_lt,
        ),
        attribute=DagRun.run_after,
    )
    start_date = RangeFilter(
        Range(
            lower_bound_gte=body.start_date_gte,
            lower_bound_gt=body.start_date_gt,
            upper_bound_lte=body.start_date_lte,
            upper_bound_lt=body.start_date_lt,
        ),
        attribute=DagRun.start_date,
    )
    end_date = RangeFilter(
        Range(
            lower_bound_gte=body.end_date_gte,
            lower_bound_gt=body.end_date_gt,
            upper_bound_lte=body.end_date_lte,
            upper_bound_lt=body.end_date_lt,
        ),
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
    ).set_value([body.order_by] if body.order_by else None)

    base_query = select(DagRun).options(joinedload(DagRun.dag_model))
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
