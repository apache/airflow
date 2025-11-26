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
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import or_, select
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.selectable import Select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import (
    DagBagDep,
    get_dag_for_run,
    get_dag_for_run_or_latest_version,
    get_latest_version_of_dag,
)
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.db.task_instances import eager_load_TI_and_TIH_for_validation
from airflow.api_fastapi.common.parameters import (
    FilterOptionEnum,
    FilterParam,
    LimitFilter,
    OffsetFilter,
    QueryLimit,
    QueryOffset,
    QueryTIDagVersionFilter,
    QueryTIExecutorFilter,
    QueryTIMapIndexFilter,
    QueryTIOperatorFilter,
    QueryTIOperatorNamePatternSearch,
    QueryTIPoolFilter,
    QueryTIPoolNamePatternSearch,
    QueryTIQueueFilter,
    QueryTIQueueNamePatternSearch,
    QueryTIStateFilter,
    QueryTITaskDisplayNamePatternSearch,
    QueryTITryNumberFilter,
    Range,
    RangeFilter,
    SortParam,
    _SearchParam,
    datetime_range_filter_factory,
    filter_param_factory,
    float_range_filter_factory,
    search_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.common import BulkBody, BulkResponse
from airflow.api_fastapi.core_api.datamodels.task_instance_history import (
    TaskInstanceHistoryCollectionResponse,
    TaskInstanceHistoryResponse,
)
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    BulkTaskInstanceBody,
    ClearTaskInstancesBody,
    PatchTaskInstanceBody,
    TaskDependencyCollectionResponse,
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
    TaskInstancesBatchBody,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, ReadableTIFilterDep, requires_access_dag
from airflow.api_fastapi.core_api.services.public.task_instances import (
    BulkTaskInstanceService,
    _patch_task_instance_note,
    _patch_task_instance_state,
    _patch_ti_validate_request,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import AirflowClearRunningTaskException, TaskNotFound
from airflow.models import Base, DagRun
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.models.taskinstancehistory import TaskInstanceHistory as TIH
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
from airflow.utils.db import get_query_count
from airflow.utils.state import DagRunState, TaskInstanceState

log = structlog.get_logger(__name__)

task_instances_router = AirflowRouter(tags=["Task Instance"], prefix="/dags/{dag_id}")
task_instances_prefix = "/dagRuns/{dag_run_id}/taskInstances"


@task_instances_router.get(
    task_instances_prefix + "/{task_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .options(joinedload(TI.rendered_task_instance_fields))
        .options(joinedload(TI.dag_version))
        .options(joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)))
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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_mapped_task_instances(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    dag_bag: DagBagDep,
    run_after_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", TI))],
    logical_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", TI))],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", TI))],
    end_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("end_date", TI))],
    update_at_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("updated_at", TI))],
    duration_range: Annotated[RangeFilter, Depends(float_range_filter_factory("duration", TI))],
    state: QueryTIStateFilter,
    pool: QueryTIPoolFilter,
    pool_name_pattern: QueryTIPoolNamePatternSearch,
    queue: QueryTIQueueFilter,
    queue_name_pattern: QueryTIQueueNamePatternSearch,
    executor: QueryTIExecutorFilter,
    version_number: QueryTIDagVersionFilter,
    try_number: QueryTITryNumberFilter,
    operator: QueryTIOperatorFilter,
    operator_name_pattern: QueryTIOperatorNamePatternSearch,
    map_index: QueryTIMapIndexFilter,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",
                    "state",
                    "duration",
                    "start_date",
                    "end_date",
                    "map_index",
                    "try_number",
                    "logical_date",
                    "run_after",
                    "data_interval_start",
                    "data_interval_end",
                    "rendered_map_index",
                    "operator",
                ],
                TI,
                to_replace={
                    "run_after": DagRun.run_after,
                    "logical_date": DagRun.logical_date,
                    "data_interval_start": DagRun.data_interval_start,
                    "data_interval_end": DagRun.data_interval_end,
                },
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
        .options(*eager_load_TI_and_TIH_for_validation())
    )
    # 0 can mean a mapped TI that expanded to an empty list, so it is not an automatic 404
    unfiltered_total_count = get_query_count(query, session=session)
    if unfiltered_total_count == 0:
        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))
        dag = get_dag_for_run_or_latest_version(dag_bag, dag_run, dag_id, session)
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
            run_after_range,
            logical_date_range,
            start_date_range,
            end_date_range,
            update_at_range,
            duration_range,
            state,
            pool,
            pool_name_pattern,
            queue,
            queue_name_pattern,
            executor,
            version_number,
            try_number,
            operator,
            operator_name_pattern,
            map_index,
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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="get_task_instance_dependencies",
)
@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/dependencies",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="get_task_instance_dependencies_by_map_index",
)
def get_task_instance_dependencies(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    map_index: int = -1,
) -> TaskDependencyCollectionResponse:
    """Get dependencies blocking task from getting scheduled."""
    query = select(TI).where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
    query = query.where(TI.map_index == map_index)

    result = session.execute(query).one_or_none()

    if result is None:
        error_message = (
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )
        raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)

    ti = result[0]
    deps = []

    if ti.state in [None, TaskInstanceState.SCHEDULED]:
        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == ti.dag_id, DagRun.run_id == ti.run_id))
        if dag_run:
            dag = dag_bag.get_dag_for_run(dag_run, session=session)
        else:
            dag = None

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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
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
        query = (
            select(orm_object)
            .where(
                orm_object.dag_id == dag_id,
                orm_object.run_id == dag_run_id,
                orm_object.task_id == task_id,
                orm_object.map_index == map_index,
            )
            .options(*eager_load_TI_and_TIH_for_validation(orm_object))
            .options(joinedload(orm_object.hitl_detail))
        )
        return query

    # Exclude TaskInstance with state UP_FOR_RETRY since they have been recorded in TaskInstanceHistory
    tis = session.scalars(
        _query(TI).where(or_(TI.state != TaskInstanceState.UP_FOR_RETRY, TI.state.is_(None)))
    ).all()
    task_instances = list(session.scalars(_query(TIH)).all()) + list(tis)

    if not task_instances:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )
    return TaskInstanceHistoryCollectionResponse(
        task_instances=cast("list[TaskInstanceHistoryResponse]", task_instances),
        total_entries=len(task_instances),
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/tries",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
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
        .options(joinedload(TI.rendered_task_instance_fields))
        .options(joinedload(TI.dag_version))
        .options(joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)))
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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_task_instances(
    dag_id: str,
    dag_run_id: str,
    dag_bag: DagBagDep,
    task_id: Annotated[FilterParam[str | None], Depends(filter_param_factory(TI.task_id, str | None))],
    run_after_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", TI))],
    logical_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", TI))],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", TI))],
    end_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("end_date", TI))],
    update_at_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("updated_at", TI))],
    duration_range: Annotated[RangeFilter, Depends(float_range_filter_factory("duration", TI))],
    task_display_name_pattern: QueryTITaskDisplayNamePatternSearch,
    dag_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(TI.dag_id, "dag_id_pattern"))],
    run_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(TI.run_id, "run_id_pattern"))],
    state: QueryTIStateFilter,
    pool: QueryTIPoolFilter,
    pool_name_pattern: QueryTIPoolNamePatternSearch,
    queue: QueryTIQueueFilter,
    queue_name_pattern: QueryTIQueueNamePatternSearch,
    executor: QueryTIExecutorFilter,
    version_number: QueryTIDagVersionFilter,
    try_number: QueryTITryNumberFilter,
    operator: QueryTIOperatorFilter,
    operator_name_pattern: QueryTIOperatorNamePatternSearch,
    map_index: QueryTIMapIndexFilter,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",
                    "state",
                    "duration",
                    "start_date",
                    "end_date",
                    "map_index",
                    "try_number",
                    "logical_date",
                    "run_after",
                    "data_interval_start",
                    "data_interval_end",
                    "rendered_map_index",
                    "operator",
                ],
                TI,
                to_replace={
                    "logical_date": DagRun.logical_date,
                    "run_after": DagRun.run_after,
                    "data_interval_start": DagRun.data_interval_start,
                    "data_interval_end": DagRun.data_interval_end,
                },
            ).dynamic_depends(default="map_index")
        ),
    ],
    readable_ti_filter: ReadableTIFilterDep,
    session: SessionDep,
) -> TaskInstanceCollectionResponse:
    """
    Get list of task instances.

    This endpoint allows specifying `~` as the dag_id, dag_run_id to retrieve Task Instances for all DAGs
    and DAG runs.
    """
    dag_run = None
    query = (
        select(TI).join(TI.dag_run).outerjoin(TI.dag_version).options(*eager_load_TI_and_TIH_for_validation())
    )
    if dag_run_id != "~":
        dag_run = session.scalar(select(DagRun).filter_by(run_id=dag_run_id))
        if not dag_run:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"DagRun with run_id: `{dag_run_id}` was not found",
            )
        query = query.where(TI.run_id == dag_run_id)
    if dag_id != "~":
        get_dag_for_run_or_latest_version(dag_bag, dag_run, dag_id, session)
        query = query.where(TI.dag_id == dag_id)

    task_instance_select, total_entries = paginated_select(
        statement=query,
        filters=[
            run_after_range,
            logical_date_range,
            start_date_range,
            end_date_range,
            update_at_range,
            duration_range,
            state,
            pool,
            pool_name_pattern,
            queue,
            queue_name_pattern,
            executor,
            task_id,
            task_display_name_pattern,
            dag_id_pattern,
            run_id_pattern,
            version_number,
            readable_ti_filter,
            try_number,
            operator,
            operator_name_pattern,
            map_index,
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
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def get_task_instances_batch(
    dag_id: Literal["~"],
    dag_run_id: Literal["~"],
    body: TaskInstancesBatchBody,
    readable_ti_filter: ReadableTIFilterDep,
    session: SessionDep,
) -> TaskInstanceCollectionResponse:
    """Get list of task instances."""
    dag_ids = FilterParam(TI.dag_id, body.dag_ids, FilterOptionEnum.IN)  # type: ignore[arg-type]
    dag_run_ids = FilterParam(TI.run_id, body.dag_run_ids, FilterOptionEnum.IN)  # type: ignore[arg-type]
    task_ids = FilterParam(TI.task_id, body.task_ids, FilterOptionEnum.IN)  # type: ignore[arg-type]
    run_after = RangeFilter(
        Range(
            lower_bound_gte=body.run_after_gte,
            lower_bound_gt=body.run_after_gt,
            upper_bound_lte=body.run_after_lte,
            upper_bound_lt=body.run_after_lt,
        ),
        attribute=DagRun.run_after,
    )
    logical_date = RangeFilter(
        Range(
            lower_bound_gte=body.logical_date_gte,
            lower_bound_gt=body.logical_date_gt,
            upper_bound_lte=body.logical_date_lte,
            upper_bound_lt=body.logical_date_lt,
        ),
        attribute=DagRun.logical_date,
    )
    start_date = RangeFilter(
        Range(
            lower_bound_gte=body.start_date_gte,
            lower_bound_gt=body.start_date_gt,
            upper_bound_lte=body.start_date_lte,
            upper_bound_lt=body.start_date_lt,
        ),
        attribute=TI.start_date,  # type: ignore[arg-type]
    )
    end_date = RangeFilter(
        Range(
            lower_bound_gte=body.end_date_gte,
            lower_bound_gt=body.end_date_gt,
            upper_bound_lte=body.end_date_lte,
            upper_bound_lt=body.end_date_lt,
        ),
        attribute=TI.end_date,  # type: ignore[arg-type]
    )
    duration = RangeFilter(
        Range(
            lower_bound_gte=body.duration_gte,
            lower_bound_gt=body.duration_gt,
            upper_bound_lte=body.duration_lte,
            upper_bound_lt=body.duration_lt,
        ),
        attribute=TI.duration,  # type: ignore[arg-type]
    )
    state = FilterParam(TI.state, body.state, FilterOptionEnum.ANY_EQUAL)  # type: ignore[arg-type]
    pool = FilterParam(TI.pool, body.pool, FilterOptionEnum.ANY_EQUAL)  # type: ignore[arg-type]
    queue = FilterParam(TI.queue, body.queue, FilterOptionEnum.ANY_EQUAL)  # type: ignore[arg-type]
    executor = FilterParam(TI.executor, body.executor, FilterOptionEnum.ANY_EQUAL)  # type: ignore[arg-type]

    offset = OffsetFilter(body.page_offset)
    limit = LimitFilter(body.page_limit)

    order_by = SortParam(
        ["id", "state", "duration", "start_date", "end_date", "map_index"],
        TI,
    ).set_value([body.order_by] if body.order_by else None)

    query = (
        select(TI).join(TI.dag_run).outerjoin(TI.dag_version).options(*eager_load_TI_and_TIH_for_validation())
    )
    task_instance_select, total_entries = paginated_select(
        statement=query,
        filters=[
            dag_ids,
            dag_run_ids,
            task_ids,
            run_after,
            logical_date,
            start_date,
            end_date,
            duration,
            state,
            pool,
            queue,
            executor,
            readable_ti_filter,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    task_instance_select = task_instance_select.options(
        joinedload(TI.rendered_task_instance_fields),
        joinedload(TI.task_instance_note),
        joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)),
    )

    task_instances = session.scalars(task_instance_select)

    return TaskInstanceCollectionResponse(
        task_instances=task_instances,
        total_entries=total_entries,
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/tries/{task_try_number}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
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
        query = (
            select(orm_object)
            .where(
                orm_object.dag_id == dag_id,
                orm_object.run_id == dag_run_id,
                orm_object.task_id == task_id,
                orm_object.try_number == task_try_number,
                orm_object.map_index == map_index,
            )
            .options(joinedload(orm_object.hitl_detail))
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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
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
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def post_clear_task_instances(
    dag_id: str,
    dag_bag: DagBagDep,
    body: ClearTaskInstancesBody,
    session: SessionDep,
) -> TaskInstanceCollectionResponse:
    """Clear task instances."""
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)

    reset_dag_runs = body.reset_dag_runs
    dry_run = body.dry_run
    # We always pass dry_run here, otherwise this would try to confirm on the terminal!
    dag_run_id = body.dag_run_id
    future = body.include_future
    past = body.include_past
    downstream = body.include_downstream
    upstream = body.include_upstream

    if dag_run_id is not None:
        dag_run: DagRun | None = session.scalar(
            select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id)
        )
        if dag_run is None:
            error_message = f"Dag Run id {dag_run_id} not found in dag {dag_id}"
            raise HTTPException(status.HTTP_404_NOT_FOUND, error_message)
        # Get the specific dag version:
        dag = get_dag_for_run(dag_bag, dag_run, session)
        if (past or future) and dag_run.logical_date is None:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "Cannot use include_past or include_future with no logical_date(e.g. manually or asset-triggered).",
            )
        body.start_date = dag_run.logical_date if dag_run.logical_date is not None else None
        body.end_date = dag_run.logical_date if dag_run.logical_date is not None else None

    if past:
        body.start_date = None

    if future:
        body.end_date = None

    if (task_markers_to_clear := body.task_ids) is not None:
        mapped_tasks_tuples = {t for t in task_markers_to_clear if isinstance(t, tuple)}
        # Unmapped tasks are expressed in their task_ids (without map_indexes)
        normal_task_ids = {t for t in task_markers_to_clear if not isinstance(t, tuple)}

        def _collect_relatives(run_id: str, direction: Literal["upstream", "downstream"]) -> None:
            from airflow.models.taskinstance import find_relevant_relatives

            relevant_relatives = find_relevant_relatives(
                normal_task_ids,
                mapped_tasks_tuples,
                dag=dag,
                run_id=run_id,
                direction=direction,
                session=session,
            )
            normal_task_ids.update(t for t in relevant_relatives if not isinstance(t, tuple))
            mapped_tasks_tuples.update(t for t in relevant_relatives if isinstance(t, tuple))

        # We can't easily calculate upstream/downstream map indexes when not
        # working for a specific dag run. It's possible by looking at the runs
        # one by one, but that is both resource-consuming and logically complex.
        # So instead we'll just clear all the tis based on task ID and hope
        # that's good enough for most cases.
        if dag_run_id is None:
            if upstream or downstream:
                partial_dag = dag.partial_subset(
                    task_ids=normal_task_ids.union(tid for tid, _ in mapped_tasks_tuples),
                    include_downstream=downstream,
                    include_upstream=upstream,
                    exclude_original=True,
                )
                normal_task_ids.update(partial_dag.task_dict)
        else:
            if upstream:
                _collect_relatives(dag_run_id, "upstream")
            if downstream:
                _collect_relatives(dag_run_id, "downstream")

        task_markers_to_clear = [
            *normal_task_ids,
            *((t, m) for t, m in mapped_tasks_tuples if t not in normal_task_ids),
        ]

    if dag_run_id is not None and not (past or future):
        # Use run_id-based clearing when we have a specific dag_run_id and not using past/future
        task_instances = dag.clear(
            dry_run=True,
            task_ids=task_markers_to_clear,
            run_id=dag_run_id,
            session=session,
            run_on_latest_version=body.run_on_latest_version,
            only_failed=body.only_failed,
            only_running=body.only_running,
        )
    else:
        # Use date-based clearing when no dag_run_id or when past/future is specified
        task_instances = dag.clear(
            dry_run=True,
            task_ids=task_markers_to_clear,
            start_date=body.start_date,
            end_date=body.end_date,
            session=session,
            run_on_latest_version=body.run_on_latest_version,
            only_failed=body.only_failed,
            only_running=body.only_running,
        )

    if not dry_run:
        try:
            clear_task_instances(
                task_instances,
                session,
                DagRunState.QUEUED if reset_dag_runs else False,
                run_on_latest_version=body.run_on_latest_version,
                prevent_running_task=body.prevent_running_task,
            )
        except AirflowClearRunningTaskException as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e)) from e

    return TaskInstanceCollectionResponse(
        task_instances=[TaskInstanceResponse.model_validate(ti) for ti in task_instances],
        total_entries=len(task_instances),
    )


@task_instances_router.patch(
    task_instances_prefix + "/{task_id}/dry_run",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST],
    ),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="patch_task_instance_dry_run",
)
@task_instances_router.patch(
    task_instances_prefix + "/{task_id}/{map_index}/dry_run",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST],
    ),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="patch_task_instance_dry_run_by_map_index",
)
def patch_task_instance_dry_run(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    dag_bag: DagBagDep,
    body: PatchTaskInstanceBody,
    session: SessionDep,
    map_index: int | None = None,
    update_mask: list[str] | None = Query(None),
) -> TaskInstanceCollectionResponse:
    """Update a task instance dry_run mode."""
    dag, tis, data = _patch_ti_validate_request(
        dag_id, dag_run_id, task_id, dag_bag, body, session, map_index, update_mask
    )

    if data.get("new_state"):
        tis = (
            dag.set_task_instance_state(
                task_id=task_id,
                run_id=dag_run_id,
                map_indexes=[map_index] if map_index is not None else None,
                state=data["new_state"],
                upstream=body.include_upstream,
                downstream=body.include_downstream,
                future=body.include_future,
                past=body.include_past,
                commit=False,
                session=session,
            )
            or []
        )

    return TaskInstanceCollectionResponse(
        task_instances=[
            TaskInstanceResponse.model_validate(
                ti,
            )
            for ti in tis
        ],
        total_entries=len(tis),
    )


@task_instances_router.patch(
    task_instances_prefix,
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def bulk_task_instances(
    request: BulkBody[BulkTaskInstanceBody],
    session: SessionDep,
    dag_id: str,
    dag_bag: DagBagDep,
    dag_run_id: str,
    user: GetUserDep,
) -> BulkResponse:
    """Bulk update, and delete task instances."""
    return BulkTaskInstanceService(
        session=session, request=request, dag_id=dag_id, dag_run_id=dag_run_id, dag_bag=dag_bag, user=user
    ).handle_request()


@task_instances_router.patch(
    task_instances_prefix + "/{task_id}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT],
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
    operation_id="patch_task_instance",
)
@task_instances_router.patch(
    task_instances_prefix + "/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT],
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
    operation_id="patch_task_instance_by_map_index",
)
def patch_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    dag_bag: DagBagDep,
    body: PatchTaskInstanceBody,
    user: GetUserDep,
    session: SessionDep,
    map_index: int | None = None,
    update_mask: list[str] | None = Query(None),
) -> TaskInstanceCollectionResponse:
    """Update a task instance."""
    dag, tis, data = _patch_ti_validate_request(
        dag_id, dag_run_id, task_id, dag_bag, body, session, map_index, update_mask
    )

    for key, _ in data.items():
        if key == "new_state":
            # Create BulkTaskInstanceBody object with map_index field
            bulk_ti_body = BulkTaskInstanceBody(
                task_id=task_id,
                map_index=map_index,
                new_state=body.new_state,
                note=body.note,
                include_upstream=body.include_upstream,
                include_downstream=body.include_downstream,
                include_future=body.include_future,
                include_past=body.include_past,
            )

            _patch_task_instance_state(
                task_id=task_id,
                dag_run_id=dag_run_id,
                dag=dag,
                task_instance_body=bulk_ti_body,
                data=data,
                session=session,
            )

        elif key == "note":
            _patch_task_instance_note(
                task_instance_body=body,
                tis=tis,
                user=user,
                update_mask=update_mask,
            )

    return TaskInstanceCollectionResponse(
        task_instances=[
            TaskInstanceResponse.model_validate(
                ti,
            )
            for ti in tis
        ],
        total_entries=len(tis),
    )


@task_instances_router.delete(
    task_instances_prefix + "/{task_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def delete_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int = -1,
) -> None:
    """Delete a task instance."""
    query = select(TI).where(
        TI.dag_id == dag_id,
        TI.run_id == dag_run_id,
        TI.task_id == task_id,
    )

    query = query.where(TI.map_index == map_index)
    task_instance = session.scalar(query)
    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )

    session.delete(task_instance)
