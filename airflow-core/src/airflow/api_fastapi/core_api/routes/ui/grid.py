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

import collections
from typing import TYPE_CHECKING, Annotated, Any

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
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
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance

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
    root: str | None = None,
) -> list[GridNodeResponse]:
    """Return dag structure for grid view."""
    latest_serdag = _get_latest_serdag(dag_id, session)
    latest_dag = latest_serdag.dag

    # Apply filtering if root task is specified
    if root:
        latest_dag = latest_dag.partial_subset(
            task_ids=root, include_upstream=include_upstream, include_downstream=include_downstream
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

    serdags = session.scalars(
        select(SerializedDagModel).where(
            # Even though dag_id is filtered in base_query,
            # adding this line here can improve the performance of this endpoint
            SerializedDagModel.dag_id == dag_id,
            SerializedDagModel.id != latest_serdag.id,
            SerializedDagModel.dag_version_id.in_(
                select(TaskInstance.dag_version_id)
                .join(TaskInstance.dag_run)
                .where(
                    DagRun.id.in_(run_ids),
                )
                .distinct()
            ),
        )
    )
    merged_nodes: list[dict[str, Any]] = []
    dags = [latest_dag]
    for serdag in serdags:
        if serdag:
            filtered_dag = serdag.dag
            # Apply the same filtering to historical DAG versions
            if root:
                filtered_dag = filtered_dag.partial_subset(
                    task_ids=root, include_upstream=include_upstream, include_downstream=include_downstream
                )
            dags.append(filtered_dag)
    for dag in dags:
        nodes = [task_group_to_dict_grid(x) for x in task_group_sort(dag.task_group)]
        _merge_node_dicts(merged_nodes, nodes)

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
    base_query = select(
        DagRun.dag_id,
        DagRun.run_id,
        DagRun.queued_at,
        DagRun.start_date,
        DagRun.end_date,
        DagRun.run_after,
        DagRun.state,
        DagRun.run_type,
    ).where(DagRun.dag_id == dag_id)

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
    )
    return [GridRunsResponse(**row._mapping) for row in session.execute(dag_runs_select_filter)]


@grid_router.get(
    "/ti_summaries/{dag_id}/{run_id}",
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
)
def get_grid_ti_summaries(
    dag_id: str,
    run_id: str,
    session: SessionDep,
) -> GridTISummaries:
    """
    Get states for TIs / "groups" of TIs.

    Essentially this is to know what color to put in the squares in the grid.

    The tricky part here is that we aggregate the state for groups and mapped tasks.

    We don't add all the TIs for mapped TIs -- we only add one entry for the mapped task and
    its state is an aggregate of its TI states.

    And for task groups, we add a "task" for that which is not really a task but is just
    an entry that represents the group (so that we can show a filled in box when the group
    is not expanded) and its state is an agg of those within it.
    """
    tis_of_dag_runs, _ = paginated_select(
        statement=(
            select(
                TaskInstance.task_id,
                TaskInstance.state,
                TaskInstance.dag_version_id,
                TaskInstance.start_date,
                TaskInstance.end_date,
            )
            .where(TaskInstance.dag_id == dag_id)
            .where(
                TaskInstance.run_id == run_id,
            )
        ),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance).set_value(["task_id"]),
        limit=None,
        return_total_entries=False,
    )
    task_instances = list(session.execute(tis_of_dag_runs))
    if not task_instances:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"No task instances for dag_id={dag_id} run_id={run_id}"
        )
    ti_details = collections.defaultdict(list)
    for ti in task_instances:
        ti_details[ti.task_id].append(
            {
                "state": ti.state,
                "start_date": ti.start_date,
                "end_date": ti.end_date,
            }
        )
    serdag = _get_serdag(
        dag_id=dag_id,
        dag_version_id=task_instances[0].dag_version_id,
        session=session,
    )
    if TYPE_CHECKING:
        assert serdag

    def get_node_sumaries():
        yielded_task_ids: set[str] = set()

        # Yield all nodes discoverable from the serialized DAG structure
        for node in _find_aggregates(
            node=serdag.dag.task_group,
            parent_node=None,
            ti_details=ti_details,
        ):
            if node["type"] in {"task", "mapped_task"}:
                yielded_task_ids.add(node["task_id"])
                if node["type"] == "task":
                    node["child_states"] = None
                    node["min_start_date"] = None
                    node["max_end_date"] = None
            yield node

        # For good history: add synthetic leaf nodes for task_ids that have TIs in this run
        # but are not present in the current DAG structure (e.g. removed tasks)
        missing_task_ids = set(ti_details.keys()) - yielded_task_ids
        for task_id in sorted(missing_task_ids):
            detail = ti_details[task_id]
            # Create a leaf task node with aggregated state from its TIs
            agg = _get_aggs_for_node(detail)
            yield {
                "task_id": task_id,
                "type": "task",
                "parent_id": None,
                **agg,
                # Align with leaf behavior
                "child_states": None,
                "min_start_date": None,
                "max_end_date": None,
            }

    task_instances = list(get_node_sumaries())
    # If a group id and a task id collide, prefer the group record
    group_ids = {n.get("task_id") for n in task_instances if n.get("type") == "group"}
    filtered = [n for n in task_instances if not (n.get("type") == "task" and n.get("task_id") in group_ids)]

    return {  # type: ignore[return-value]
        "run_id": run_id,
        "dag_id": dag_id,
        "task_instances": filtered,
    }
