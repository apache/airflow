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

from collections.abc import Generator, Iterable
from typing import TYPE_CHECKING, Annotated, Any

import structlog
from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import exists, select
from sqlalchemy.orm import Session, joinedload, load_only

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.db.dag_runs import attach_dag_versions_to_runs
from airflow.api_fastapi.common.parameters import (
    QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter,
    QueryDagRunTriggeringUserSearch,
    QueryIncludeDownstream,
    QueryIncludeUpstream,
    QueryLimit,
    QueryOffset,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.common import (
    GridNodeResponse,
    GridRunsResponse,
)
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridTISummaries,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.grid import (
    GridNodeAgg,
    _find_aggregates,
    _get_aggs_for_node,
    _merge_node_dicts,
)
from airflow.api_fastapi.core_api.services.ui.task_group import (
    get_task_group_children_getter,
    task_group_to_dict_grid,
)
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session

log = structlog.get_logger(logger_name=__name__)
grid_router = AirflowRouter(prefix="/grid", tags=["Grid"])


def _get_latest_serdag(dag_id, session):
    serdag = session.scalar(
        select(SerializedDagModel)
        .where(
            SerializedDagModel.dag_id == dag_id,
        )
        .order_by(SerializedDagModel.id.desc())
        .limit(1)
    )
    if not serdag:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Dag with id {dag_id} was not found",
        )
    return serdag


def _get_serdag(dag_id, dag_version_id, session) -> SerializedDagModel | None:
    # this is a simplification - we account for structure based on the first task
    version = session.scalar(
        select(DagVersion)
        .where(DagVersion.id == dag_version_id)
        .options(joinedload(DagVersion.serialized_dag))
    )
    if not version:
        version = session.scalar(
            select(DagVersion)
            .where(
                DagVersion.dag_id == dag_id,
            )
            .options(joinedload(DagVersion.serialized_dag))
            .order_by(DagVersion.id)  # ascending cus this is mostly for pre-3.0 upgrade
            .limit(1)
        )
    if not version:
        return None
    if not (serdag := version.serialized_dag):
        log.error(
            "No serialized dag found",
            dag_id=dag_id,
            version_id=version.id,
            version_number=version.version_number,
        )
    return serdag


@grid_router.get(
    "/structure/{dag_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN)),
    ],
    response_model_exclude_none=True,
)
def get_dag_structure(
    dag_id: str,
    session: SessionDep,
    offset: QueryOffset,
    limit: QueryLimit,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["run_after", "logical_date", "start_date", "end_date"], DagRun).dynamic_depends()),
    ],
    run_after: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DagRun))],
    run_type: QueryDagRunRunTypesFilter,
    state: QueryDagRunStateFilter,
    triggering_user: QueryDagRunTriggeringUserSearch,
    include_upstream: QueryIncludeUpstream = False,
    include_downstream: QueryIncludeDownstream = False,
    depth: int | None = None,
    root: str | None = None,
) -> list[GridNodeResponse]:
    """Return dag structure for grid view."""
    latest_serdag = _get_latest_serdag(dag_id, session)
    latest_dag = latest_serdag.dag
    latest_serdag_id = latest_serdag.id
    session.expunge(latest_serdag)  # allow GC of serdag; only latest_dag is needed from here

    # Apply filtering if root task is specified
    if root:
        latest_dag = latest_dag.partial_subset(
            task_ids=root,
            include_upstream=include_upstream,
            include_downstream=include_downstream,
            depth=depth,
        )

    # Retrieve, sort the previous DAG Runs
    base_query = select(DagRun.id).where(DagRun.dag_id == dag_id)
    # This comparison is to fall back to DAG timetable when no order_by is provided
    if order_by.value == [order_by.get_primary_key_string()]:
        ordering = list(latest_dag.timetable.run_ordering)
        order_by = SortParam(
            allowed_attrs=ordering,
            model=DagRun,
        ).set_value(ordering)
    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        order_by=order_by,
        offset=offset,
        filters=[run_after, run_type, state, triggering_user],
        limit=limit,
    )
    run_ids = list(session.scalars(dag_runs_select_filter))

    task_group_sort = get_task_group_children_getter()
    if not run_ids:
        nodes = [task_group_to_dict_grid(x) for x in task_group_sort(latest_dag.task_group)]
        return [GridNodeResponse(**n) for n in nodes]

    # Process and merge the latest serdag first
    merged_nodes: list[dict[str, Any]] = []
    nodes = [task_group_to_dict_grid(x) for x in task_group_sort(latest_dag.task_group)]
    _merge_node_dicts(merged_nodes, nodes)
    del latest_dag

    # Process serdags one by one and merge immediately to reduce memory usage.
    # Use yield_per() for streaming results and expunge each serdag after processing
    # to allow garbage collection and prevent memory buildup in the session identity map.
    serdags_query = (
        select(SerializedDagModel)
        .where(
            # Even though dag_id is filtered in base_query,
            # adding this line here can improve the performance of this endpoint
            SerializedDagModel.dag_id == dag_id,
            SerializedDagModel.id != latest_serdag_id,
            SerializedDagModel.dag_version_id.in_(
                select(TaskInstance.dag_version_id)
                .join(TaskInstance.dag_run)
                .where(
                    DagRun.id.in_(run_ids),
                )
                .distinct()
            ),
        )
        .execution_options(yield_per=5)  # balance between peak memory usage and round trips
    )

    for serdag in session.scalars(serdags_query):
        filtered_dag = serdag.dag
        # Apply the same filtering to historical DAG versions
        if root:
            filtered_dag = filtered_dag.partial_subset(
                task_ids=root,
                include_upstream=include_upstream,
                include_downstream=include_downstream,
                depth=depth,
            )
        # Merge immediately instead of collecting all DAGs in memory
        nodes = [task_group_to_dict_grid(x) for x in task_group_sort(filtered_dag.task_group)]
        _merge_node_dicts(merged_nodes, nodes)

        session.expunge(serdag)  # to allow garbage collection

    return [GridNodeResponse(**n) for n in merged_nodes]


@grid_router.get(
    "/runs/{dag_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.TASK_INSTANCE,
            )
        ),
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
    response_model_exclude_none=True,
)
def get_grid_runs(
    dag_id: str,
    session: SessionDep,
    offset: QueryOffset,
    limit: QueryLimit,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "run_after",
                    "logical_date",
                    "start_date",
                    "end_date",
                ],
                DagRun,
            ).dynamic_depends()
        ),
    ],
    run_after: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DagRun))],
    run_type: QueryDagRunRunTypesFilter,
    state: QueryDagRunStateFilter,
    triggering_user: QueryDagRunTriggeringUserSearch,
) -> list[GridRunsResponse]:
    """Get info about a run for the grid."""
    # Retrieve, sort the previous DAG Runs
    has_missed_deadline = (
        exists()
        .where(Deadline.dagrun_id == DagRun.id, Deadline.missed.is_(True))
        .correlate(DagRun)
        .label("has_missed_deadline")
    )
    base_query = (
        select(DagRun, has_missed_deadline)
        .where(DagRun.dag_id == dag_id)
        .options(
            load_only(
                DagRun.dag_id,
                DagRun.run_id,
                DagRun.queued_at,
                DagRun.start_date,
                DagRun.end_date,
                DagRun.run_after,
                DagRun.state,
                DagRun.run_type,
                DagRun.bundle_version,
            ),
            joinedload(DagRun.created_dag_version).joinedload(DagVersion.bundle),
            joinedload(DagRun.created_dag_version).joinedload(DagVersion.dag_model),
        )
    )

    # This comparison is to fall back to DAG timetable when no order_by is provided
    if order_by.value == [order_by.get_primary_key_string()]:
        latest_serdag = _get_latest_serdag(dag_id, session)
        latest_dag = latest_serdag.dag
        ordering = list(latest_dag.timetable.run_ordering)
        order_by = SortParam(
            allowed_attrs=ordering,
            model=DagRun,
        ).set_value(ordering)
    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        order_by=order_by,
        offset=offset,
        filters=[run_after, run_type, state, triggering_user],
        limit=limit,
        return_total_entries=False,
    )
    results = session.execute(dag_runs_select_filter).unique().all()
    dag_runs = [run for run, _ in results]
    attach_dag_versions_to_runs(dag_runs, session=session)
    grid_runs = []
    for run, has_missed in results:
        grid_runs.append(
            GridRunsResponse.model_validate(
                {
                    "dag_id": run.dag_id,
                    "run_id": run.run_id,
                    "queued_at": run.queued_at,
                    "start_date": run.start_date,
                    "end_date": run.end_date,
                    "run_after": run.run_after,
                    "state": run.state,
                    "run_type": run.run_type,
                    "dag_versions": run.dag_versions,
                    "has_missed_deadline": has_missed,
                }
            )
        )
    return grid_runs


def _build_ti_summaries(
    dag_id: str,
    run_id: str,
    task_instances: Iterable[Any],
    session: Session,
    *,
    serdag: SerializedDagModel | None = None,
    serdag_cache: dict[Any, SerializedDagModel | None] | None = None,
) -> dict[str, Any] | None:
    ti_details: dict[str, GridNodeAgg] = {}
    dag_version_id = None
    for ti in task_instances:
        dag_version_id = dag_version_id or ti.dag_version_id
        summary = ti_details.get(ti.task_id)
        if summary is None:
            summary = ti_details[ti.task_id] = GridNodeAgg()
        summary.add_ti(
            state=ti.state,
            start_date=ti.start_date,
            end_date=ti.end_date,
            dag_version_number=getattr(ti, "version_number", None),
        )
    if not ti_details:
        return None
    if serdag is None:
        if serdag_cache is not None:
            if dag_version_id not in serdag_cache:
                serdag_cache[dag_version_id] = _get_serdag(
                    dag_id=dag_id,
                    dag_version_id=dag_version_id,
                    session=session,
                )
            serdag = serdag_cache[dag_version_id]
        else:
            serdag = _get_serdag(
                dag_id=dag_id,
                dag_version_id=dag_version_id,
                session=session,
            )
    if TYPE_CHECKING:
        assert serdag

    def get_node_summaries() -> Iterable[dict[str, Any]]:
        yielded_task_ids: set[str] = set()
        for node, _ in _find_aggregates(
            node=serdag.dag.task_group,
            parent_node=None,
            ti_details=ti_details,
        ):
            if node["type"] in {"task", "mapped_task"}:
                yielded_task_ids.add(node["task_id"])
                if node["type"] == "task":
                    node["child_states"] = None
            yield node
        missing_task_ids = set(ti_details.keys()) - yielded_task_ids
        for task_id in sorted(missing_task_ids):
            detail = ti_details[task_id]
            agg = _get_aggs_for_node(detail)
            yield {
                "task_id": task_id,
                "task_display_name": task_id,
                "type": "task",
                "parent_id": None,
                **agg,
                "child_states": None,
            }

    nodes = list(get_node_summaries())
    # If a group id and a task id collide, prefer the group record
    group_ids = {n.get("task_id") for n in nodes if n.get("type") == "group"}
    filtered = [n for n in nodes if not (n.get("type") == "task" and n.get("task_id") in group_ids)]
    return {"run_id": run_id, "dag_id": dag_id, "task_instances": filtered}


@grid_router.get(
    "/ti_summaries/{dag_id}",
    response_class=StreamingResponse,
    response_model=GridTISummaries,
    responses={
        **create_openapi_http_exception_doc(
            [
                status.HTTP_400_BAD_REQUEST,
                status.HTTP_404_NOT_FOUND,
            ]
        ),
        200: {
            "content": {"application/x-ndjson": {"schema": {"type": "string"}}},
            "description": "NDJSON stream — one ``GridTISummaries`` JSON object per line, one per Dag run",
        },
    },
    dependencies=[
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.TASK_INSTANCE,
            )
        ),
        Depends(
            requires_access_dag(
                method="GET",
                access_entity=DagAccessEntity.RUN,
            )
        ),
    ],
)
def get_grid_ti_summaries_stream(
    dag_id: str,
    run_ids: Annotated[list[str] | None, Query()] = None,
) -> StreamingResponse:
    """
    Stream TI summaries for multiple Dag runs as NDJSON (one JSON line per run).

    Each line is a serialized ``GridTISummaries`` object emitted as soon as that
    run's task instances have been processed, so the client can render columns
    progressively without waiting for all runs to complete.

    The serialized Dag structure is loaded once and reused for all runs that
    share the same ``dag_version_id``, avoiding repeated deserialization.
    """

    def _generate() -> Generator[str, None, None]:
        # Each iteration opens and closes its own DB session so the connection is
        # released between yields.  This prevents a slow client from holding a
        # database connection open for the entire stream duration.
        # See https://github.com/apache/airflow/issues/65010.

        serdag_cache: dict[Any, SerializedDagModel | None] = {}
        for run_id in run_ids or []:
            with create_session(scoped=False) as session:
                tis = session.execute(
                    select(
                        TaskInstance.task_id,
                        TaskInstance.state,
                        TaskInstance.dag_version_id,
                        TaskInstance.start_date,
                        TaskInstance.end_date,
                        DagVersion.version_number,
                    )
                    .outerjoin(DagVersion, TaskInstance.dag_version_id == DagVersion.id)
                    .where(TaskInstance.dag_id == dag_id)
                    .where(TaskInstance.run_id == run_id)
                    .order_by(TaskInstance.task_id)
                    .execution_options(yield_per=1000)
                )
                summary = _build_ti_summaries(
                    dag_id,
                    run_id,
                    tis,
                    session,
                    serdag_cache=serdag_cache,
                )
            if summary is None:
                continue
            yield GridTISummaries.model_validate(summary).model_dump_json() + "\n"

    return StreamingResponse(content=_generate(), media_type="application/x-ndjson")
