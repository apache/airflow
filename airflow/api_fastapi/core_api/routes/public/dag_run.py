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

from fastapi import Depends, HTTPException, Query, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_fastapi.common.db.common import get_session, paginated_select
from airflow.api_fastapi.common.parameters import (
    DagIdsFilter,
    LimitFilter,
    OffsetFilter,
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
)
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models import DAG, DagRun

dag_run_router = AirflowRouter(tags=["DagRun"], prefix="/dags/{dag_id}/dagRuns")


@dag_run_router.get(
    "/{dag_run_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_dag_run(
    dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]
) -> DAGRunResponse:
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
        ]
    ),
)
def delete_dag_run(dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]):
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
        ]
    ),
)
def patch_dag_run(
    dag_id: str,
    dag_run_id: str,
    patch_body: DAGRunPatchBody,
    session: Annotated[Session, Depends(get_session)],
    request: Request,
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

    if update_mask:
        data = patch_body.model_dump(include=set(update_mask))
    else:
        data = patch_body.model_dump()

    for attr_name, attr_value in data.items():
        if attr_name == "state":
            attr_value = getattr(patch_body, "state")
            if attr_value == DAGRunPatchStates.SUCCESS:
                set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
            elif attr_value == DAGRunPatchStates.QUEUED:
                set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
            elif attr_value == DAGRunPatchStates.FAILED:
                set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
        elif attr_name == "note":
            # Once Authentication is implemented in this FastAPI app,
            # user id will be added when updating dag run note
            # Refer to https://github.com/apache/airflow/issues/43534
            dag_run = session.get(DagRun, dag_run.id)
            if dag_run.dag_run_note is None:
                dag_run.note = (attr_value, None)
            else:
                dag_run.dag_run_note.content = attr_value

    dag_run = session.get(DagRun, dag_run.id)

    return dag_run


@dag_run_router.get(
    "/{dag_run_id}/upstreamAssetEvents",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def get_upstream_asset_events(
    dag_id: str, dag_run_id: str, session: Annotated[Session, Depends(get_session)]
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
    "/{dag_run_id}/clear", responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND])
)
def clear_dag_run(
    dag_id: str,
    dag_run_id: str,
    body: DAGRunClearBody,
    request: Request,
    session: Annotated[Session, Depends(get_session)],
) -> TaskInstanceCollectionResponse | DAGRunResponse:
    dag_run = session.scalar(select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id))
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )

    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    start_date = dag_run.logical_date
    end_date = dag_run.logical_date

    if body.dry_run:
        task_instances = dag.clear(
            start_date=start_date,
            end_date=end_date,
            task_ids=None,
            only_failed=False,
            dry_run=True,
            session=session,
        )

        return TaskInstanceCollectionResponse(
            task_instances=cast(list[TaskInstanceResponse], task_instances),
            total_entries=len(task_instances),
        )
    else:
        dag.clear(
            start_date=dag_run.start_date,
            end_date=dag_run.end_date,
            task_ids=None,
            only_failed=False,
            session=session,
        )
        dag_run_cleared = session.scalar(select(DagRun).where(DagRun.id == dag_run.id))
        return dag_run_cleared


@dag_run_router.get("", responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]))
def get_dag_runs(
    dag_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    logical_date: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", DagRun))],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", DagRun))],
    end_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("end_date", DagRun))],
    update_at_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("updated_at", DagRun))],
    state: QueryDagRunStateFilter,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",
                    "state",
                    "dag_id",
                    "logical_date",
                    "dag_run_id",
                    "start_date",
                    "end_date",
                    "updated_at",
                    "external_trigger",
                    "conf",
                ],
                DagRun,
            ).dynamic_depends(default="id")
        ),
    ],
    session: Annotated[Session, Depends(get_session)],
    request: Request,
) -> DAGRunCollectionResponse:
    """
    Get all DAG Runs.

    This endpoint allows specifying `~` as the dag_id to retrieve Dag Runs for all DAGs.
    """
    base_query = select(DagRun)

    if dag_id != "~":
        dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
        if not dag:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"The DAG with dag_id: `{dag_id}` was not found")

        base_query = base_query.filter(DagRun.dag_id == dag_id)

    dag_run_select, total_entries = paginated_select(
        select=base_query,
        filters=[logical_date, start_date_range, end_date_range, update_at_range, state],
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


@dag_run_router.post("/list", responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]))
def get_list_dag_runs_batch(
    dag_id: Literal["~"], body: DAGRunsBatchBody, session: Annotated[Session, Depends(get_session)]
) -> DAGRunCollectionResponse:
    """Get a list of DAG Runs."""
    dag_ids = DagIdsFilter(DagRun, body.dag_ids)
    logical_date = RangeFilter(
        Range(lower_bound=body.logical_date_gte, upper_bound=body.logical_date_lte),
        attribute=DagRun.logical_date,
    )
    start_date = RangeFilter(
        Range(lower_bound=body.start_date_gte, upper_bound=body.start_date_lte),
        attribute=DagRun.start_date,
    )
    end_date = RangeFilter(
        Range(lower_bound=body.end_date_gte, upper_bound=body.end_date_lte),
        attribute=DagRun.end_date,
    )

    state = QueryDagRunStateFilter(body.states)

    offset = OffsetFilter(body.page_offset)
    limit = LimitFilter(body.page_limit)

    order_by = SortParam(
        [
            "id",
            "state",
            "dag_id",
            "logical_date",
            "dag_run_id",
            "start_date",
            "end_date",
            "updated_at",
            "external_trigger",
            "conf",
        ],
        DagRun,
    ).set_value(body.order_by)

    base_query = select(DagRun)
    dag_runs_select, total_entries = paginated_select(
        select=base_query,
        filters=[dag_ids, logical_date, start_date, end_date, state],
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
