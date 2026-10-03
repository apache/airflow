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

from collections import defaultdict
from collections.abc import Sequence
from typing import Annotated, Literal, cast
from uuid import UUID

import structlog
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import select, tuple_
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.cursors import (
    apply_cursor_filter,
    encode_cursor,
    make_backward_cursor,
    parse_cursor,
)
from airflow.api_fastapi.common.dagbag import (
    DagBagDep,
    get_dag_for_run,
    get_dag_for_run_or_latest_version,
    get_latest_version_of_dag,
    resolve_run_on_latest_version,
)
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    apply_filters_to_select,
    bounded_total_entries,
    paginated_select,
)
from airflow.api_fastapi.common.db.dags import eager_load_teams
from airflow.api_fastapi.common.db.task_instances import eager_load_task_instance_for_validation
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
    QueryTIOperatorNamePrefixPatternSearch,
    QueryTIPoolFilter,
    QueryTIPoolNamePatternSearch,
    QueryTIPoolNamePrefixPatternSearch,
    QueryTIQueueFilter,
    QueryTIQueueNamePatternSearch,
    QueryTIQueueNamePrefixPatternSearch,
    QueryTIRenderedMapIndexPatternSearch,
    QueryTIRenderedMapIndexPrefixPatternSearch,
    QueryTIStateFilter,
    QueryTITaskDisplayNamePatternSearch,
    QueryTITaskDisplayNamePrefixPatternSearch,
    QueryTITaskGroupFilter,
    QueryTITryNumberFilter,
    Range,
    RangeFilter,
    SortParam,
    _DagIdTeamsFilter,
    _PrefixSearchParam,
    _SearchParam,
    datetime_range_filter_factory,
    filter_param_factory,
    float_range_filter_factory,
    prefix_search_param_factory,
    search_param_factory,
    teams_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.base import OrmClause
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
from airflow.api_fastapi.core_api.services.public.task_coordinates import (
    CoordinateResolverDep,
    TaskCoordinateView,
    TaskScopeDep,
    UnmappedTaskScopeDep,
    add_public_map_index,
    loop_iteration_filter,
    task_coordinate_response,
    task_coordinate_responses,
)
from airflow.api_fastapi.core_api.services.public.task_instances import (
    BulkTaskInstanceService,
    _discard_task_state_store,
    _get_task_group_task_ids,
    _patch_task_group_state,
    _patch_task_instance_note,
    _patch_task_instance_state,
    _patch_ti_group_validate_request,
    _patch_ti_validate_request,
    _reload_tis_with_rendered_fields,
    patch_region_selection,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import AirflowClearRunningTaskException, TaskNotFound
from airflow.models import DagRun
from airflow.models.dynamic_region import SENTINEL_REGION_ID
from airflow.models.renderedtifields import load_legacy_rendered_fields
from airflow.models.task_coordinates import (
    LOOP_GATE_OPERATOR,
    TaskCoordinateResolver,
    enclosing_loop,
    public_map_index_expression,
)
from airflow.models.taskinstance import (
    LoopClearScope,
    TaskInstance as TI,
    apply_loop_clear_scope,
    clear_task_instances,
    select_loop_clear_scope,
)
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULER_QUEUED_DEPS
from airflow.utils.db import get_query_count
from airflow.utils.state import DagRunState, State, TaskInstanceState

log = structlog.get_logger(__name__)

task_instances_router = AirflowRouter(tags=["Task Instance"], prefix="/dags/{dag_id}")
task_instances_prefix = "/dagRuns/{dag_run_id}/taskInstances"


@task_instances_router.get(
    task_instances_prefix + "/{task_id}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    scope: UnmappedTaskScopeDep,
    resolver: CoordinateResolverDep,
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .where(TI.region_id == scope.region_id)
        .options(joinedload(TI.rendered_task_instance_fields))
        .options(joinedload(TI.dag_version))
        .options(joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)))
        .options(*eager_load_teams(TI.dag_run, DagRun.dag_model))
    )
    if scope.region_id != SENTINEL_REGION_ID or scope.region_index != -1:
        query = query.where(TI.region_index == scope.region_index)
    task_instance = session.scalar(query)

    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}` and task_id: `{task_id}` was not found",
        )
    if resolver.public_map_index(task_instance) != -1:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "Task instance is mapped, add the map_index value to the URL"
        )
    load_legacy_rendered_fields([task_instance], session=session)

    return task_coordinate_response(TaskInstanceResponse, task_instance, resolver)


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/listMapped",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
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
    pool_name_prefix_pattern: QueryTIPoolNamePrefixPatternSearch,
    queue: QueryTIQueueFilter,
    queue_name_pattern: QueryTIQueueNamePatternSearch,
    queue_name_prefix_pattern: QueryTIQueueNamePrefixPatternSearch,
    executor: QueryTIExecutorFilter,
    version_number: QueryTIDagVersionFilter,
    try_number: QueryTITryNumberFilter,
    operator: QueryTIOperatorFilter,
    operator_name_pattern: QueryTIOperatorNamePatternSearch,
    operator_name_prefix_pattern: QueryTIOperatorNamePrefixPatternSearch,
    map_index: QueryTIMapIndexFilter,
    rendered_map_index_pattern: QueryTIRenderedMapIndexPatternSearch,
    rendered_map_index_prefix_pattern: QueryTIRenderedMapIndexPrefixPatternSearch,
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
                    "map_index": public_map_index_expression(TI),
                    "run_after": DagRun.run_after,
                    "logical_date": DagRun.logical_date,
                    "data_interval_start": DagRun.data_interval_start,
                    "data_interval_end": DagRun.data_interval_end,
                    # Compound sort: when _rendered_map_index is NULL (no map_index_template),
                    # all primary values tie and the integer map_index is the effective key,
                    # giving correct numeric ordering (0, 1, 2, 10…) rather than lexicographic
                    # ("0", "1", "10", "2"…).  When _rendered_map_index is set (map_index_template
                    # used), TIs are ordered by their human-readable label first, then by
                    # map_index for identical labels.
                    "rendered_map_index": [
                        TI._rendered_map_index,
                        public_map_index_expression(TI).label("map_index"),
                    ],
                },
            ).dynamic_depends(default="map_index")
        ),
    ],
    session: SessionDep,
    resolver: CoordinateResolverDep,
    region_id: Annotated[UUID | None, Query()] = None,
    region_index: Annotated[int | None, Query(ge=-1)] = None,
) -> TaskInstanceCollectionResponse:
    """Get list of mapped task instances."""
    query = add_public_map_index(
        eager_load_task_instance_for_validation(
            select(TI).where(
                TI.dag_id == dag_id,
                TI.run_id == dag_run_id,
                TI.task_id == task_id,
                public_map_index_expression(TI) >= 0,
            )
        )
    )
    if region_index is not None and region_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "region_index requires region_id")
    if region_id is not None:
        query = query.where(TI.region_id == region_id)
    if region_index is not None:
        query = query.where(TI.region_index == region_index)
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
            pool_name_prefix_pattern,
            queue,
            queue_name_pattern,
            queue_name_prefix_pattern,
            executor,
            version_number,
            try_number,
            operator,
            operator_name_pattern,
            operator_name_prefix_pattern,
            map_index,
            rendered_map_index_pattern,
            rendered_map_index_prefix_pattern,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    rows = session.execute(task_instance_select).all()
    load_legacy_rendered_fields([ti for ti, _ in rows], session=session)

    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(
            TaskInstanceResponse,
            [ti for ti, _ in rows],
            resolver,
            map_indexes=[map_index for _, map_index in rows],
        ),
        total_entries=total_entries,
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/dependencies",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="get_task_instance_dependencies",
)
@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/dependencies",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="get_task_instance_dependencies_by_map_index",
)
def get_task_instance_dependencies(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    dag_bag: DagBagDep,
    scope: TaskScopeDep,
    map_index: int = -1,
) -> TaskDependencyCollectionResponse:
    """Get dependencies blocking task from getting scheduled."""
    query = select(TI).where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
    query = query.where(TI.region_index == scope.region_index, TI.region_id == scope.region_id)

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
        if ti.dag_version_id:
            dag = dag_bag.get_dag(ti.dag_version_id, session=session)
        elif dag_run:
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
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_task_instance_tries(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    scope: TaskScopeDep,
    resolver: CoordinateResolverDep,
    map_index: int = -1,
) -> TaskInstanceHistoryCollectionResponse:
    """
    Get list of task instances history.

    Tries recorded before the task had regions are not included once it has them.
    """
    query = (
        eager_load_task_instance_for_validation(
            select(TI).where(
                TI.dag_id == dag_id,
                TI.run_id == dag_run_id,
                TI.task_id == task_id,
                TI.region_index == scope.region_index,
                TI.region_id == scope.region_id,
            )
        )
        .options(joinedload(TI.hitl_detail))
        .order_by(TI.try_number)
        .execution_options(include_all_attempts=True)
    )
    task_instances = list(session.scalars(query))

    if not task_instances:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )
    load_legacy_rendered_fields(task_instances, session=session)
    return TaskInstanceHistoryCollectionResponse(
        task_instances=task_coordinate_responses(TaskInstanceHistoryResponse, task_instances, resolver),
        total_entries=len(task_instances),
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/tries",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_mapped_task_instance_tries(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int,
    scope: TaskScopeDep,
    resolver: CoordinateResolverDep,
) -> TaskInstanceHistoryCollectionResponse:
    """
    Get list of task instances history for a mapped task instance.

    Tries recorded before the task had regions are not included once it has them.
    """
    return get_task_instance_tries(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        session=session,
        scope=scope,
        resolver=resolver,
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_mapped_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    session: SessionDep,
    scope: TaskScopeDep,
    resolver: CoordinateResolverDep,
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(
            TI.dag_id == dag_id,
            TI.run_id == dag_run_id,
            TI.task_id == task_id,
            TI.region_id == scope.region_id,
            TI.region_index == scope.region_index,
        )
        .options(joinedload(TI.rendered_task_instance_fields))
        .options(joinedload(TI.dag_version))
        .options(joinedload(TI.dag_run).options(joinedload(DagRun.dag_model)))
        .options(*eager_load_teams(TI.dag_run, DagRun.dag_model))
    )
    task_instance = session.scalar(query)

    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Mapped Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}`, and map_index: `{map_index}` was not found",
        )

    load_legacy_rendered_fields([task_instance], session=session)
    return task_coordinate_response(TaskInstanceResponse, task_instance, resolver)


@task_instances_router.get(
    task_instances_prefix,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
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
    task_display_name_prefix_pattern: QueryTITaskDisplayNamePrefixPatternSearch,
    task_group_id: QueryTITaskGroupFilter,
    dag_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(TI.dag_id, "dag_id_pattern"))],
    dag_id_prefix_pattern: Annotated[
        _PrefixSearchParam,
        Depends(prefix_search_param_factory(TI.dag_id, "dag_id_prefix_pattern")),
    ],
    run_id_pattern: Annotated[_SearchParam, Depends(search_param_factory(TI.run_id, "run_id_pattern"))],
    run_id_prefix_pattern: Annotated[
        _PrefixSearchParam,
        Depends(prefix_search_param_factory(TI.run_id, "run_id_prefix_pattern")),
    ],
    state: QueryTIStateFilter,
    pool: QueryTIPoolFilter,
    pool_name_pattern: QueryTIPoolNamePatternSearch,
    pool_name_prefix_pattern: QueryTIPoolNamePrefixPatternSearch,
    queue: QueryTIQueueFilter,
    queue_name_pattern: QueryTIQueueNamePatternSearch,
    queue_name_prefix_pattern: QueryTIQueueNamePrefixPatternSearch,
    executor: QueryTIExecutorFilter,
    version_number: QueryTIDagVersionFilter,
    teams: Annotated[_DagIdTeamsFilter, Depends(teams_filter_factory(TI.dag_id))],
    try_number: QueryTITryNumberFilter,
    operator: QueryTIOperatorFilter,
    operator_name_pattern: QueryTIOperatorNamePatternSearch,
    operator_name_prefix_pattern: QueryTIOperatorNamePrefixPatternSearch,
    map_index: QueryTIMapIndexFilter,
    rendered_map_index_pattern: QueryTIRenderedMapIndexPatternSearch,
    rendered_map_index_prefix_pattern: QueryTIRenderedMapIndexPrefixPatternSearch,
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
                    "map_index": public_map_index_expression(TI),
                    "logical_date": DagRun.logical_date,
                    "run_after": DagRun.run_after,
                    "data_interval_start": DagRun.data_interval_start,
                    "data_interval_end": DagRun.data_interval_end,
                    # Compound sort: see the listMapped endpoint comment for rationale.
                    "rendered_map_index": [
                        TI._rendered_map_index,
                        public_map_index_expression(TI).label("map_index"),
                    ],
                },
            ).dynamic_depends(default="map_index")
        ),
    ],
    readable_ti_filter: ReadableTIFilterDep,
    session: SessionDep,
    resolver: CoordinateResolverDep,
    region_id: Annotated[UUID | None, Query()] = None,
    region_index: Annotated[int | None, Query(ge=-1)] = None,
    cursor: str | None = Query(
        None,
        description="Cursor for keyset-based pagination. "
        "Pass an empty string for the first page, then use ``next_cursor`` from the response. "
        "When ``cursor`` is provided, ``offset`` is ignored.",
    ),
    loop_id: Annotated[str | None, Query()] = None,
    iteration: Annotated[int | None, Query(ge=0)] = None,
    loop_region_id: Annotated[UUID | None, Query()] = None,
) -> TaskInstanceCollectionResponse:
    """
    Get list of task instances.

    This endpoint allows specifying `~` as the dag_id, dag_run_id
    to retrieve task instances for all Dags and Dag runs.

    Supports two pagination modes:

    **Offset (default):** use `limit` and `offset` query parameters. Returns `total_entries`.

    **Cursor:** pass `cursor` (empty string for the first page, then `next_cursor` from the response).
    When `cursor` is provided, `offset` is ignored and `total_entries` is capped at
    `total_entries_limit` (a value equal to that limit means at least that many task instances
    match). ``next_cursor`` is ``null`` when there are no more pages; ``previous_cursor`` is
    ``null`` on the first page.
    """
    use_cursor = cursor is not None
    dag_run = None
    query = add_public_map_index(eager_load_task_instance_for_validation(select(TI)))
    if loop_id is None and (iteration is not None or loop_region_id is not None):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "iteration and loop_region_id require loop_id")
    if loop_id is not None:
        if dag_id == "~" or dag_run_id == "~":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "loop_id requires a specific Dag run")
        query = query.where(
            loop_iteration_filter(
                dag_id=dag_id,
                run_id=dag_run_id,
                loop_id=loop_id,
                iteration=iteration,
                loop_region_id=loop_region_id,
                session=session,
            )
        )
    if region_index is not None and region_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "region_index requires region_id")
    if region_id is not None:
        query = query.where(TI.region_id == region_id)
    if region_index is not None:
        query = query.where(TI.region_index == region_index)
    if dag_run_id != "~":
        if dag_id == "~":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "dag_id is required when dag_run_id is specified",
            )
        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))
        if not dag_run:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
            )
        query = query.where(TI.run_id == dag_run_id)
    if dag_id != "~":
        dag = get_dag_for_run_or_latest_version(dag_bag, dag_run, dag_id, session)
        query = query.where(TI.dag_id == dag_id)
        if dag:
            task_group_id.dag = dag

    filters: list[OrmClause] = [
        run_after_range,
        logical_date_range,
        start_date_range,
        end_date_range,
        update_at_range,
        duration_range,
        state,
        pool,
        pool_name_pattern,
        pool_name_prefix_pattern,
        queue,
        queue_name_pattern,
        queue_name_prefix_pattern,
        executor,
        task_id,
        task_display_name_pattern,
        task_display_name_prefix_pattern,
        task_group_id,
        dag_id_pattern,
        dag_id_prefix_pattern,
        run_id_pattern,
        run_id_prefix_pattern,
        version_number,
        readable_ti_filter,
        try_number,
        operator,
        operator_name_pattern,
        operator_name_prefix_pattern,
        map_index,
        rendered_map_index_pattern,
        rendered_map_index_prefix_pattern,
        teams,
    ]

    if use_cursor:
        # Fetch one extra row so we can detect whether a next page exists.
        page_limit = cast(
            "int", limit.value
        )  # LimitFilter value is guaranteed to be set to the default value of QueryLimit
        cursor_limit = LimitFilter().set_value(page_limit + 1)
        task_instance_select = apply_filters_to_select(statement=query, filters=[*filters, cursor_limit])
        task_instance_select = order_by.to_orm(task_instance_select)

        is_backward = False
        if cursor:
            token, is_backward = parse_cursor(cursor)
            if is_backward:
                task_instance_select = order_by.to_orm(task_instance_select, reversed=True)
            task_instance_select = apply_cursor_filter(
                task_instance_select,
                token,
                order_by,
                session.get_bind().dialect.name,
                is_backward=is_backward,
            )

        fetched = list(session.execute(task_instance_select).all())
        has_more = len(fetched) > page_limit
        rows = fetched[:page_limit]

        if is_backward:
            rows.reverse()
            has_prev = has_more
            has_next = True
        else:
            has_prev = bool(cursor)
            has_next = has_more

        total_entries, total_entries_limit = bounded_total_entries(
            statement=query, filters=filters, session=session
        )
        load_legacy_rendered_fields([ti for ti, _ in rows], session=session)
        return TaskInstanceCollectionResponse(
            task_instances=task_coordinate_responses(
                TaskInstanceResponse,
                [ti for ti, _ in rows],
                resolver,
                map_indexes=[map_index for _, map_index in rows],
            ),
            total_entries=total_entries,
            total_entries_limit=total_entries_limit,
            next_cursor=(
                encode_cursor(TaskCoordinateView(rows[-1][0], resolver, rows[-1][1]), order_by)
                if has_next and rows
                else None
            ),
            previous_cursor=(
                make_backward_cursor(
                    encode_cursor(TaskCoordinateView(rows[0][0], resolver, rows[0][1]), order_by)
                )
                if has_prev and rows
                else None
            ),
        )

    task_instance_select, total_entries = paginated_select(
        statement=query,
        filters=filters,
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    page_rows = session.execute(task_instance_select).all()
    load_legacy_rendered_fields([ti for ti, _ in page_rows], session=session)
    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(
            TaskInstanceResponse,
            [ti for ti, _ in page_rows],
            resolver,
            map_indexes=[map_index for _, map_index in page_rows],
        ),
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
    resolver: CoordinateResolverDep,
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
        to_replace={"map_index": public_map_index_expression(TI)},
    ).set_value([body.order_by] if body.order_by else None)

    query = add_public_map_index(eager_load_task_instance_for_validation(select(TI)))
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
    rows = session.execute(task_instance_select).all()
    load_legacy_rendered_fields([ti for ti, _ in rows], session=session)

    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(
            TaskInstanceResponse,
            [ti for ti, _ in rows],
            resolver,
            map_indexes=[map_index for _, map_index in rows],
        ),
        total_entries=total_entries,
    )


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/tries/{task_try_number}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_task_instance_try_details(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: int,
    session: SessionDep,
    scope: TaskScopeDep,
    resolver: CoordinateResolverDep,
    map_index: int = -1,
) -> TaskInstanceHistoryResponse:
    """Get task instance details by try number."""
    query = eager_load_task_instance_for_validation(
        select(TI)
        .where(
            TI.dag_id == dag_id,
            TI.run_id == dag_run_id,
            TI.task_id == task_id,
            TI.try_number == task_try_number,
            TI.region_index == scope.region_index,
            TI.region_id == scope.region_id,
        )
        .execution_options(include_all_attempts=True)
    )
    ti = session.scalar(query)
    if ti is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}`, try_number: `{task_try_number}` and map_index: `{map_index}` was not found",
        )
    load_legacy_rendered_fields([ti], session=session)
    return task_coordinate_response(TaskInstanceHistoryResponse, ti, resolver)


@task_instances_router.get(
    task_instances_prefix + "/{task_id}/{map_index}/tries/{task_try_number}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_mapped_task_instance_try_details(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    task_try_number: int,
    session: SessionDep,
    map_index: int,
    scope: TaskScopeDep,
    resolver: CoordinateResolverDep,
) -> TaskInstanceHistoryResponse:
    return get_task_instance_try_details(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        task_try_number=task_try_number,
        map_index=map_index,
        session=session,
        scope=scope,
        resolver=resolver,
    )


@task_instances_router.post(
    "/clearTaskInstances",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
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
    user: GetUserDep,
    resolver: CoordinateResolverDep,
) -> TaskInstanceCollectionResponse:
    """Clear task instances."""
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)

    resolved_run_on_latest = resolve_run_on_latest_version(body.run_on_latest_version, dag_id, session)

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

    # A task group has no per-task list at the call site; resolve every task in it from the dag
    # structure so all are cleared, not just the first page the UI could enumerate.
    if body.task_group_id is not None:
        body.task_ids = cast(
            "list[str | tuple[str, int]]",
            _get_task_group_task_ids(dag_id, body.task_group_id, dag),
        )

    gate_query = select(TI.id).join(TI.dag_run).where(TI.dag_id == dag_id, TI.operator == LOOP_GATE_OPERATOR)
    if dag_run_id is not None and not (past or future):
        gate_query = gate_query.where(TI.run_id == dag_run_id)
    else:
        if body.start_date is not None:
            gate_query = gate_query.where(DagRun.logical_date >= body.start_date)
        if body.end_date is not None:
            gate_query = gate_query.where(DagRun.logical_date <= body.end_date)
    loop_aware = (
        body.task_instance_ids is not None
        or any(enclosing_loop(task) for task in dag.tasks)
        or session.scalar(gate_query.limit(1)) is not None
    )
    task_markers_to_clear = body.task_ids
    if task_markers_to_clear is not None and not loop_aware:
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

    clear_markers = task_markers_to_clear
    if loop_aware and task_markers_to_clear is not None:
        clear_markers = [marker if isinstance(marker, str) else marker[0] for marker in task_markers_to_clear]

    def select_candidates() -> Sequence[TI]:
        if body.task_instance_ids is not None:
            return session.scalars(
                select(TI).where(
                    TI.dag_id == dag_id,
                    TI.run_id == dag_run_id,
                    TI.working_set.is_(True),
                    TI.id.in_(body.task_instance_ids),
                )
            ).all()
        if dag_run_id is not None and not (past or future):
            return dag.clear(
                dry_run=True,
                task_ids=clear_markers,
                run_id=dag_run_id,
                session=session,
                run_on_latest_version=resolved_run_on_latest,
                only_failed=body.only_failed and not loop_aware,
                only_running=body.only_running and not loop_aware,
            )
        return dag.clear(
            dry_run=True,
            task_ids=clear_markers,
            start_date=body.start_date,
            end_date=body.end_date,
            session=session,
            run_on_latest_version=resolved_run_on_latest,
            only_failed=body.only_failed and not loop_aware,
            only_running=body.only_running and not loop_aware,
        )

    task_instances = select_candidates()
    if body.task_instance_ids is not None and {ti.id for ti in task_instances} != set(body.task_instance_ids):
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "Selected task execution is not current in this DAG run"
        )

    if task_instances:
        if not dry_run:
            run_keys = {(ti.dag_id, ti.run_id) for ti in task_instances}
            session.scalars(
                select(DagRun)
                .where(tuple_(DagRun.dag_id, DagRun.run_id).in_(run_keys))
                .order_by(DagRun.dag_id, DagRun.run_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            ).all()
            if body.task_instance_ids is None:
                task_instances = [ti for ti in select_candidates() if (ti.dag_id, ti.run_id) in run_keys]
        task_instances = session.scalars(
            select(TI)
            .where(TI.id.in_([ti.id for ti in task_instances]))
            .execution_options(populate_existing=True)
        ).all()

    scopes: list[LoopClearScope] = []
    if loop_aware and task_markers_to_clear is not None:
        task_instances = [
            ti
            for ti in task_instances
            if ti.task_id in task_markers_to_clear
            or (ti.task_id, resolver.public_map_index(ti)) in task_markers_to_clear
        ]
    if body.task_instance_ids is not None:
        if {ti.id for ti in task_instances} != set(body.task_instance_ids):
            raise HTTPException(status.HTTP_409_CONFLICT, "Selected task execution changed before clearing")
    if loop_aware:
        selections: dict[tuple[str, str], list[TI]] = defaultdict(list)
        for ti in task_instances:
            selections[(ti.dag_id, ti.run_id)].append(ti)
        try:
            for selected in selections.values():
                selected_ids = {ti.id for ti in selected}
                scope = select_loop_clear_scope(
                    selected,
                    whole_expansion_ids=set(body.whole_expansion_ids) & selected_ids,
                    upstream=body.include_upstream,
                    downstream=body.include_downstream,
                    later_loop_iterations=body.include_later_loop_iterations
                    and not (body.only_failed or body.only_running),
                    include_setups_and_teardowns=body.task_instance_ids is None,
                    session=session,
                )
                if body.only_failed or body.only_running:
                    eligible = session.scalars(
                        select(TI).where(
                            TI.id.in_(scope.retry_ids),
                            TI.state.in_(State.failed_states)
                            if body.only_failed
                            else TI.state == State.RUNNING,
                        )
                    ).all()
                    scope = select_loop_clear_scope(
                        eligible,
                        downstream=False,
                        later_loop_iterations=body.include_later_loop_iterations,
                        session=session,
                    )
                scopes.append(scope)
        except ValueError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        task_instances = session.scalars(
            select(TI)
            .where(
                TI.id.in_({identity for scope in scopes for identity in scope.retry_ids | scope.archive_ids})
            )
            .execution_options(populate_existing=True)
        ).all()

    if not dry_run:
        whole_task_keys = {
            (ti.dag_id, ti.run_id, ti.task_id)
            for ti in task_instances
            if not body.only_failed
            and not body.only_running
            and body.task_instance_ids is None
            and (task_markers_to_clear is None or ti.task_id in task_markers_to_clear)
        }
        if not body.only_failed and not body.only_running:
            whole_task_keys.update(
                (ti.dag_id, ti.run_id, ti.task_id)
                for ti in task_instances
                if ti.id in body.whole_expansion_ids
            )
        try:
            if loop_aware:
                cleared: list[TI] = []
                for scope in scopes:
                    cleared += apply_loop_clear_scope(
                        scope,
                        session=session,
                        later_loop_iterations=body.include_later_loop_iterations,
                        dag_run_state=DagRunState.QUEUED if reset_dag_runs else False,
                        run_on_latest_version=resolved_run_on_latest,
                        prevent_running_task=body.prevent_running_task,
                        whole_task_keys=whole_task_keys,
                    )
                task_instances = cleared
            else:
                task_instances = clear_task_instances(
                    list(task_instances),
                    session,
                    DagRunState.QUEUED if reset_dag_runs else False,
                    run_on_latest_version=resolved_run_on_latest,
                    prevent_running_task=body.prevent_running_task,
                    whole_task_keys=whole_task_keys,
                )
        except AirflowClearRunningTaskException as e:
            raise HTTPException(status.HTTP_409_CONFLICT, str(e)) from e
        except ValueError as e:
            if not loop_aware:
                raise
            raise HTTPException(status.HTTP_409_CONFLICT, str(e)) from e

        # After the clear has succeeded, so a failed clear cannot take the task state with it.
        # This is the only clear path that discards task state today; Dag-run clear and
        # mark-as-failed/success (which clear downstream tasks) still keep it unconditionally.
        # It is tracked through https://github.com/apache/airflow/issues/72929
        if not body.keep_task_state:
            _discard_task_state_store(task_instances, session, event="Discarded task state on clear")

        if body.note is not None:
            _patch_task_instance_note(
                task_instance_body=body,
                tis=list(task_instances),
                user=user,
            )
            # The reload below refreshes with populate_existing, which would discard an unflushed note.
            session.flush()

    task_instances = _reload_tis_with_rendered_fields(list(task_instances), session)

    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(TaskInstanceResponse, task_instances, resolver),
        total_entries=len(task_instances),
    )


@task_instances_router.patch(
    "/dagRuns/{dag_run_id}/taskGroupInstances/{group_id}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT],
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
    operation_id="patch_task_group_instances",
)
def patch_task_group_instances(
    dag_id: str,
    dag_run_id: str,
    group_id: str,
    dag_bag: DagBagDep,
    body: PatchTaskInstanceBody,
    session: SessionDep,
    user: GetUserDep,
    update_mask: list[str] | None = Query(None),
    region_id: UUID | None = None,
    region_index: int | None = None,
) -> TaskInstanceCollectionResponse:
    """Update the state of all task instances in a task group."""
    body = patch_region_selection(body, region_id, region_index)
    dag, tis, data = _patch_ti_group_validate_request(
        dag_id, dag_run_id, group_id, dag_bag, body, session, update_mask
    )

    response_tis = tis
    # Apply "note" before "state" so listeners fired inside _patch_task_group_state() see the updated note.
    if "note" in data:
        _patch_task_instance_note(
            task_instance_body=body,
            tis=response_tis,
            user=user,
            update_mask=update_mask,
        )
    if "new_state" in data:
        response_tis = _patch_task_group_state(
            group_id=group_id,
            dag_run_id=dag_run_id,
            dag=dag,
            body=body,
            data=data,
            session=session,
            selected=tis,
        )

    response_tis = _reload_tis_with_rendered_fields(response_tis, session)

    resolver = TaskCoordinateResolver(dag_bag, session)
    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(TaskInstanceResponse, response_tis, resolver),
        total_entries=len(response_tis),
    )


@task_instances_router.patch(
    "/dagRuns/{dag_run_id}/taskGroupInstances/{group_id}/dry_run",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT],
    ),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="patch_task_group_instances_dry_run",
)
def patch_task_group_instances_dry_run(
    dag_id: str,
    dag_run_id: str,
    group_id: str,
    dag_bag: DagBagDep,
    body: PatchTaskInstanceBody,
    session: SessionDep,
    region_id: UUID | None = None,
    region_index: int | None = None,
) -> TaskInstanceCollectionResponse:
    """Dry-run of updating the state of all task instances in a task group."""
    body = patch_region_selection(body, region_id, region_index)
    dag, tis, data = _patch_ti_group_validate_request(
        dag_id, dag_run_id, group_id, dag_bag, body, session, lock=False
    )

    if body.new_state and body.region_id is not None:
        tis = _patch_task_group_state(
            group_id,
            dag_run_id,
            dag,
            body,
            data,
            session=session,
            selected=tis,
            commit=False,
        )
    elif body.new_state:
        tis = (
            dag.set_task_group_state(
                group_id=group_id,
                run_id=dag_run_id,
                state=body.new_state,
                upstream=body.include_upstream,
                downstream=body.include_downstream,
                future=body.include_future,
                past=body.include_past,
                commit=False,
                session=session,
            )
            or []
        )

    tis = _reload_tis_with_rendered_fields(tis, session)

    resolver = TaskCoordinateResolver(dag_bag, session)
    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(TaskInstanceResponse, tis, resolver),
        total_entries=len(tis),
    )


@task_instances_router.patch(
    task_instances_prefix + "/{task_id}/dry_run",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT],
    ),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.TASK_INSTANCE))],
    operation_id="patch_task_instance_dry_run",
)
@task_instances_router.patch(
    task_instances_prefix + "/{task_id}/{map_index}/dry_run",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_404_NOT_FOUND, status.HTTP_400_BAD_REQUEST, status.HTTP_409_CONFLICT],
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
    region_id: UUID | None = None,
    region_index: int | None = None,
) -> TaskInstanceCollectionResponse:
    """Update a task instance dry_run mode."""
    tis: Sequence[TI]
    body = patch_region_selection(body, region_id, region_index)
    dag, tis, data = _patch_ti_validate_request(
        dag_id, dag_run_id, task_id, dag_bag, body, session, map_index, update_mask, lock=False
    )

    if data.get("new_state") and body.region_id is not None:
        tis = _patch_task_instance_state(
            task_id,
            dag_run_id,
            dag,
            body,
            data,
            session,
            selected=list(tis),
            commit=False,
        )
    elif data.get("new_state"):
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

    tis = _reload_tis_with_rendered_fields(tis, session)

    resolver = TaskCoordinateResolver(dag_bag, session)
    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(TaskInstanceResponse, tis, resolver),
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
    region_id: UUID | None = None,
    region_index: int | None = None,
) -> TaskInstanceCollectionResponse:
    """Update a task instance."""
    body = patch_region_selection(body, region_id, region_index)
    dag, tis, data = _patch_ti_validate_request(
        dag_id, dag_run_id, task_id, dag_bag, body, session, map_index, update_mask
    )

    # Apply "note" before "state" so listeners fired inside _patch_task_instance_state() see the updated note.
    if "note" in data:
        _patch_task_instance_note(
            task_instance_body=body,
            tis=tis,
            user=user,
            update_mask=update_mask,
        )
    if "new_state" in data:
        # Create BulkTaskInstanceBody object with map_index field
        bulk_ti_body = BulkTaskInstanceBody(
            task_id=task_id,
            map_index=map_index,
            region_id=body.region_id,
            region_index=body.region_index,
            new_state=body.new_state,
            note=body.note,
            include_upstream=body.include_upstream,
            include_downstream=body.include_downstream,
            include_future=body.include_future,
            include_past=body.include_past,
        )
        updated_tis = _patch_task_instance_state(
            task_id=task_id,
            dag_run_id=dag_run_id,
            dag=dag,
            task_instance_body=bulk_ti_body,
            data=data,
            session=session,
            selected=tis,
        )
        if body.region_id is not None:
            tis = updated_tis

    load_legacy_rendered_fields(tis, session=session)
    resolver = TaskCoordinateResolver(dag_bag, session)
    return TaskInstanceCollectionResponse(
        task_instances=task_coordinate_responses(TaskInstanceResponse, tis, resolver),
        total_entries=len(tis),
    )


@task_instances_router.delete(
    task_instances_prefix + "/{task_id}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND, status.HTTP_409_CONFLICT]
    ),
    dependencies=[Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def delete_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    scope: TaskScopeDep,
    map_index: int = -1,
) -> None:
    """Delete a task instance."""
    query = select(TI).where(
        TI.dag_id == dag_id,
        TI.run_id == dag_run_id,
        TI.task_id == task_id,
    )

    query = query.where(TI.region_index == scope.region_index, TI.region_id == scope.region_id)
    task_instance = session.scalar(query)
    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )

    TI.delete_attempts(
        dag_id=dag_id,
        run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        region_id=scope.region_id,
        session=session,
    )
