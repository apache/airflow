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
from sqlalchemy import or_, select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.selectable import Select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    LimitFilter,
    OffsetFilter,
    QueryLimit,
    QueryOffset,
    QueryTIExecutorFilter,
    QueryTIPoolFilter,
    QueryTIQueueFilter,
    QueryTIStateFilter,
    Range,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    float_range_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    ClearTaskInstancesBody,
    PatchTaskInstanceBody,
    TaskDependencyCollectionResponse,
    TaskInstanceCollectionResponse,
    TaskInstanceHistoryCollectionResponse,
    TaskInstanceHistoryResponse,
    TaskInstanceReferenceCollectionResponse,
    TaskInstanceReferenceResponse,
    TaskInstanceResponse,
    TaskInstancesBatchBody,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.exceptions import TaskNotFound
from airflow.jobs.scheduler_job_runner import DR
from airflow.models import Base, DagRun
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.models.taskinstancehistory import TaskInstanceHistory as TIH
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
from airflow.utils.db import get_query_count
from airflow.utils.state import DagRunState, TaskInstanceState

task_instances_router = AirflowRouter(tags=["Task Instance"], prefix="/dags/{dag_id}")
task_instances_prefix = "/dagRuns/{dag_run_id}/taskInstances"


@task_instances_router.get(
    task_instances_prefix + "/{task_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_task_instance(
    dag_id: str, dag_run_id: str, task_id: str, session: SessionDep
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instance = session.scalar(query)

    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}` and task_id: `{task_id}` was not found",
        )
    if task_instance.map_index != -1:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "Task instance is mapped, add the map_index value to the URL"
        )

    return task_instance


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/listMapped",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_mapped_task_instances(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    request: Request,
    logical_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", TI))],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", TI))],
    end_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("end_date", TI))],
    update_at_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("updated_at", TI))],
    duration_range: Annotated[RangeFilter, Depends(float_range_filter_factory("duration", TI))],
    state: QueryTIStateFilter,
    pool: QueryTIPoolFilter,
    queue: QueryTIQueueFilter,
    executor: QueryTIExecutorFilter,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "state", "duration", "start_date", "end_date", "map_index", "rendered_map_index"],
                TI,
            ).dynamic_depends(default="map_index")
        ),
    ],
    session: SessionDep,
) -> TaskInstanceCollectionResponse:
    """Get list of mapped task instances."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index >= 0)
        .join(TI.dag_run)
    )
    # 0 can mean a mapped TI that expanded to an empty list, so it is not an automatic 404
    unfiltered_total_count = get_query_count(query, session=session)
    if unfiltered_total_count == 0:
        dag = request.app.state.dag_bag.get_dag(dag_id)
        if not dag:
            error_message = f"DAG {dag_id} not found"
            raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)
        try:
            task = dag.get_task(task_id)
        except TaskNotFound:
            error_message = f"Task id {task_id} not found"
            raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)
        if not task.get_needs_expansion():
            error_message = f"Task id {task_id} is not mapped"
            raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)

    task_instance_select, total_entries = paginated_select(
        statement=query,
        filters=[
            logical_date_range,
            start_date_range,
            end_date_range,
            update_at_range,
            duration_range,
            state,
            pool,
            queue,
            executor,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    task_instances = session.scalars(task_instance_select)

    return TaskInstanceCollectionResponse(
        task_instances=task_instances,
        total_entries=total_entries,
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/dependencies",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/dependencies",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_task_instance_dependencies(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    request: Request,
    map_index: int = -1,
) -> TaskDependencyCollectionResponse:
    """Get dependencies blocking task from getting scheduled."""
    query = select(TI).where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)

    if map_index == -1:
        query = query.where(TI.map_index == -1)
    else:
        query = query.where(TI.map_index == map_index)

    result = session.execute(query).one_or_none()

    if result is None:
        error_message = f"Task Instance not found for dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}"
        raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)

    ti = result[0]
    deps = []

    if ti.state in [None, TaskInstanceState.SCHEDULED]:
        dag = request.app.state.dag_bag.get_dag(ti.dag_id)

        if dag:
            try:
                ti.task = dag.get_task(ti.task_id)
            except TaskNotFound:
                pass
            else:
                dep_context = DepContext(SCHEDULER_QUEUED_DEPS)
                deps = sorted(
                    [
                        {"name": dep.dep_name, "reason": dep.reason}
                        for dep in ti.get_failed_dep_statuses(dep_context=dep_context, session=session)
                    ],
                    key=lambda x: x["name"],
                )

    return TaskDependencyCollectionResponse.model_validate({"dependencies": deps})


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/tries",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_task_instance_tries(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int = -1,
) -> TaskInstanceHistoryCollectionResponse:
    """Get list of task instances history."""

    def _query(orm_object: Base) -> Select:
        query = select(orm_object).where(
            orm_object.dag_id == dag_id,
            orm_object.run_id == dag_run_id,
            orm_object.task_id == task_id,
            orm_object.map_index == map_index,
        )
        return query

    # Exclude TaskInstance with state UP_FOR_RETRY since they have been recorded in TaskInstanceHistory
    tis = session.scalars(
        _query(TI).where(or_(TI.state != TaskInstanceState.UP_FOR_RETRY, TI.state.is_(None)))
    ).all()
    task_instances = session.scalars(_query(TIH)).all() + tis

    if not task_instances:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )

    return TaskInstanceHistoryCollectionResponse(
        task_instances=cast(list[TaskInstanceHistoryResponse], task_instances),
        total_entries=len(task_instances),
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/tries",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_mapped_task_instance_tries(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int,
) -> TaskInstanceHistoryCollectionResponse:
    return get_task_instance_tries(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        session=session,
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_mapped_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    session: SessionDep,
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index == map_index)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instance = session.scalar(query)

    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Mapped Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}`, and map_index: `{map_index}` was not found",
        )

    return task_instance


@task_instances_router.get(
    task_instances_prefix,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_task_instances(
    dag_id: str,
    dag_run_id: str,
    request: Request,
    logical_date: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", TI))],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", TI))],
    end_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("end_date", TI))],
    update_at_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("updated_at", TI))],
    duration_range: Annotated[RangeFilter, Depends(float_range_filter_factory("duration", TI))],
    state: QueryTIStateFilter,
    pool: QueryTIPoolFilter,
    queue: QueryTIQueueFilter,
    executor: QueryTIExecutorFilter,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "state", "duration", "start_date", "end_date", "map_index"],
                TI,
            ).dynamic_depends(default="map_index")
        ),
    ],
    session: SessionDep,
) -> TaskInstanceCollectionResponse:
    """
    Get list of task instances.

    This endpoint allows specifying `~` as the dag_id, dag_run_id to retrieve Task Instances for all DAGs
    and DAG runs.
    """
    query = select(TI).join(TI.dag_run)

    if dag_id != "~":
        dag = request.app.state.dag_bag.get_dag(dag_id)
        if not dag:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"DAG with dag_id: `{dag_id}` was not found")
        query = query.where(TI.dag_id == dag_id)

    if dag_run_id != "~":
        dag_run = session.scalar(select(DagRun).filter_by(run_id=dag_run_id))
        if not dag_run:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"DagRun with run_id: `{dag_run_id}` was not found",
            )
        query = query.where(TI.run_id == dag_run_id)

    task_instance_select, total_entries = paginated_select(
        statement=query,
        filters=[
            logical_date,
            start_date_range,
            end_date_range,
            update_at_range,
            duration_range,
            state,
            pool,
            queue,
            executor,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    task_instances = session.scalars(task_instance_select)
    return TaskInstanceCollectionResponse(
        task_instances=task_instances,
        total_entries=total_entries,
    )


@task_instances_router.post(
    task_instances_prefix + "/list",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_task_instances_batch(
    dag_id: Literal["~"],
    dag_run_id: Literal["~"],
    body: TaskInstancesBatchBody,
    session: SessionDep,
) -> TaskInstanceCollectionResponse:
    """Get list of task instances."""
    dag_ids = FilterParam(TI.dag_id, body.dag_ids, FilterOptionEnum.IN)
    dag_run_ids = FilterParam(TI.run_id, body.dag_run_ids, FilterOptionEnum.IN)
    task_ids = FilterParam(TI.task_id, body.task_ids, FilterOptionEnum.IN)
    logical_date = RangeFilter(
        Range(lower_bound=body.logical_date_gte, upper_bound=body.logical_date_lte),
        attribute=TI.logical_date,
    )
    start_date = RangeFilter(
        Range(lower_bound=body.start_date_gte, upper_bound=body.start_date_lte),
        attribute=TI.start_date,
    )
    end_date = RangeFilter(
        Range(lower_bound=body.end_date_gte, upper_bound=body.end_date_lte),
        attribute=TI.end_date,
    )
    duration = RangeFilter(
        Range(lower_bound=body.duration_gte, upper_bound=body.duration_lte),
        attribute=TI.duration,
    )
    state = FilterParam(TI.state, body.state, FilterOptionEnum.ANY_EQUAL)
    pool = FilterParam(TI.pool, body.pool, FilterOptionEnum.ANY_EQUAL)
    queue = FilterParam(TI.queue, body.queue, FilterOptionEnum.ANY_EQUAL)
    executor = FilterParam(TI.executor, body.executor, FilterOptionEnum.ANY_EQUAL)

    offset = OffsetFilter(body.page_offset)
    limit = LimitFilter(body.page_limit)

    order_by = SortParam(
        ["id", "state", "duration", "start_date", "end_date", "map_index"],
        TI,
    ).set_value(body.order_by)

    query = select(TI).join(TI.dag_run)
    task_instance_select, total_entries = paginated_select(
        statement=query,
        filters=[
            dag_ids,
            dag_run_ids,
            task_ids,
            logical_date,
            start_date,
            end_date,
            duration,
            state,
            pool,
            queue,
            executor,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    task_instance_select = task_instance_select.options(
        joinedload(TI.rendered_task_instance_fields), joinedload(TI.task_instance_note)
    )

    task_instances = session.scalars(task_instance_select)

    return TaskInstanceCollectionResponse(
        task_instances=task_instances,
        total_entries=total_entries,
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/tries/{task_try_number}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_task_instance_try_details(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: int,
    session: SessionDep,
    map_index: int = -1,
) -> TaskInstanceHistoryResponse:
    """Get task instance details by try number."""

    def _query(orm_object: Base) -> TI | TIH | None:
        query = select(orm_object).where(
            orm_object.dag_id == dag_id,
            orm_object.run_id == dag_run_id,
            orm_object.task_id == task_id,
            orm_object.try_number == task_try_number,
            orm_object.map_index == map_index,
        )

        task_instance = session.scalar(query)
        return task_instance

    result = _query(TI) or _query(TIH)
    if result is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}`, try_number: `{task_try_number}` and map_index: `{map_index}` was not found",
        )
    return result


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/tries/{task_try_number}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_mapped_task_instance_try_details(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: int,
    session: SessionDep,
    map_index: int,
) -> TaskInstanceHistoryResponse:
    return get_task_instance_try_details(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        task_try_number=task_try_number,
        map_index=map_index,
        session=session,
    )


@task_instances_router.post(
    "/clearTaskInstances",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def post_clear_task_instances(
    dag_id: str,
    request: Request,
    body: ClearTaskInstancesBody,
    session: SessionDep,
) -> TaskInstanceReferenceCollectionResponse:
    """Clear task instances."""
    dag = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        error_message = f"DAG {dag_id} not found"
        raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)

    reset_dag_runs = body.reset_dag_runs
    dry_run = body.dry_run
    # We always pass dry_run here, otherwise this would try to confirm on the terminal!
    dag_run_id = body.dag_run_id
    future = body.include_future
    past = body.include_past
    downstream = body.include_downstream
    upstream = body.include_upstream

    if dag_run_id is not None:
        dag_run: DR | None = session.scalar(select(DR).where(DR.dag_id == dag_id, DR.run_id == dag_run_id))
        if dag_run is None:
            error_message = f"Dag Run id {dag_run_id} not found in dag {dag_id}"
            raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)
        body.start_date = dag_run.logical_date
        body.end_date = dag_run.logical_date

    if past:
        body.start_date = None

    if future:
        body.end_date = None

    task_ids = body.task_ids
    if task_ids is not None:
        task_id = [task[0] if isinstance(task, tuple) else task for task in task_ids]
        dag = dag.partial_subset(
            task_ids_or_regex=task_id,
            include_downstream=downstream,
            include_upstream=upstream,
        )

        if len(dag.task_dict) > 1:
            # If we had upstream/downstream etc then also include those!
            task_ids.extend(tid for tid in dag.task_dict if tid != task_id)

    task_instances = dag.clear(
        dry_run=True,
        task_ids=task_ids,
        dag_bag=request.app.state.dag_bag,
        **body.model_dump(
            include={
                "start_date",
                "end_date",
                "only_failed",
                "only_running",
            }
        ),
    )

    if not dry_run:
        clear_task_instances(
            task_instances,
            session,
            dag,
            DagRunState.QUEUED if reset_dag_runs else False,
        )

    return TaskInstanceReferenceCollectionResponse(
        task_instances=[
            TaskInstanceReferenceResponse.model_validate(
                ti,
                from_attributes=True,
            )
            for ti in task_instances
        ],
        total_entries=len(task_instances),
    )


@task_instances_router.patch(
    task_instances_prefix + "/{task_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST]),
)
@task_instances_router.patch(
    task_instances_prefix + "/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST]),
)
def patch_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    request: Request,
    body: PatchTaskInstanceBody,
    session: SessionDep,
    map_index: int = -1,
    update_mask: list[str] | None = Query(None),
) -> TaskInstanceResponse:
    """Update the state of a task instance."""
    dag = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"DAG {dag_id} not found")

    if not dag.has_task(task_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task '{task_id}' not found in DAG '{dag_id}'")

    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    if map_index == -1:
        query = query.where(or_(TI.map_index == -1, TI.map_index is None))
    else:
        query = query.where(TI.map_index == map_index)

    try:
        ti = session.scalar(query)
    except MultipleResultsFound:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Multiple task instances found. As the TI is mapped, add the map_index value to the URL",
        )

    err_msg_404 = f"Task Instance not found for dag_id={dag_id}, run_id={dag_run_id}, task_id={task_id}"
    if ti is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, err_msg_404)

    fields_to_update = body.model_fields_set
    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)

    for field in fields_to_update:
        if field == "new_state":
            if not body.dry_run:
                tis: list[TI] = dag.set_task_instance_state(
                    task_id=task_id,
                    run_id=dag_run_id,
                    map_indexes=[map_index],
                    state=body.new_state,
                    upstream=body.include_upstream,
                    downstream=body.include_downstream,
                    future=body.include_future,
                    past=body.include_past,
                    commit=True,
                    session=session,
                )
                if not ti:
                    raise HTTPException(status.HTTP_404_NOT_FOUND, err_msg_404)
                ti = tis[0] if isinstance(tis, list) else tis
        elif field == "note":
            if update_mask or body.note is not None:
                # @TODO: replace None passed for user_id with actual user id when
                # permissions and auth is in place.
                if ti.task_instance_note is None:
                    ti.note = (body.note, None)
                else:
                    ti.task_instance_note.content = body.note
                    ti.task_instance_note.user_id = None
                session.commit()

    return TaskInstanceResponse.model_validate(ti, from_attributes=True)
