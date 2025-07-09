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
import itertools
from typing import Annotated

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload, selectinload

from airflow import DAG
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter,
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
    LatestRunResponse,
)
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridDAGRunwithTIs,
    GridResponse,
    GridTISummaries,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.ui.grid import (
    _find_aggregates,
    _merge_node_dicts,
    fill_task_instance_summaries,
    get_child_task_map,
    get_task_group_map,
)
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import (
    get_task_group_children_getter,
    task_group_to_dict_grid,
)

log = structlog.get_logger(logger_name=__name__)
grid_router = AirflowRouter(prefix="/grid", tags=["Grid"])


@grid_router.get(
    "/{dag_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.RUN)),
    ],
    response_model_exclude_none=True,
)
def grid_data(
    dag_id: str,
    session: SessionDep,
    offset: QueryOffset,
    dag_bag: DagBagDep,
    run_type: QueryDagRunRunTypesFilter,
    state: QueryDagRunStateFilter,
    limit: QueryLimit,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["run_after", "logical_date", "start_date", "end_date"], DagRun).dynamic_depends()),
    ],
    run_after: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DagRun))],
    logical_date: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", DagRun))],
    include_upstream: QueryIncludeUpstream = False,
    include_downstream: QueryIncludeDownstream = False,
    root: str | None = None,
) -> GridResponse:
    """Return grid data."""
    dag: DAG = dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    # Retrieve, sort the previous DAG Runs
    base_query = (
        select(DagRun)
        .join(DagRun.dag_run_note, isouter=True)
        .options(joinedload(DagRun.task_instances).joinedload(TaskInstance.dag_version))
        .options(joinedload(DagRun.task_instances_histories).joinedload(TaskInstanceHistory.dag_version))
        .where(DagRun.dag_id == dag.dag_id)
    )

    # This comparison is to falls to DAG timetable when no order_by is provided
    if order_by.value == order_by.get_primary_key_string():
        order_by = SortParam(
            allowed_attrs=[run_ordering for run_ordering in dag.timetable.run_ordering], model=DagRun
        ).set_value(dag.timetable.run_ordering[0])

    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        filters=[
            run_type,
            state,
            run_after,
            logical_date,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
    )

    dag_runs = list(session.scalars(dag_runs_select_filter).unique())
    # Check if there are any DAG Runs with given criteria to eliminate unnecessary queries/errors
    if not dag_runs:
        return GridResponse(dag_runs=[])

    # Retrieve, sort and encode the Task Instances
    tis_of_dag_runs, _ = paginated_select(
        statement=select(TaskInstance)
        .options(selectinload(TaskInstance.task_instance_note))
        .where(TaskInstance.dag_id == dag.dag_id)
        .where(TaskInstance.run_id.in_([dag_run.run_id for dag_run in dag_runs])),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance).set_value("task_id"),
        offset=offset,
        limit=None,
    )

    task_instances = session.scalars(tis_of_dag_runs)

    tis_by_run_id: dict[str, list[TaskInstance]] = collections.defaultdict(list)
    for ti in task_instances:
        tis_by_run_id[ti.run_id].append(ti)

    # Generate Grouped Task Instances
    task_node_map_exclude = None
    if root:
        task_node_map_exclude = get_task_group_map(
            dag=dag.partial_subset(
                task_ids=root,
                include_upstream=include_upstream,
                include_downstream=include_downstream,
            )
        )

    # Group the Task Instances by Parent Task (TaskGroup or Mapped) and All Task Instances
    parent_tis: dict[tuple[str, str], list] = collections.defaultdict(list)
    all_tis: dict[tuple[str, str], list] = collections.defaultdict(list)

    for tis in tis_by_run_id.values():
        # this is a simplification - we account for structure based on the first task
        version = tis[0].dag_version
        if not version:
            version = session.scalar(
                select(DagVersion)
                .where(
                    DagVersion.dag_id == tis[0].dag_id,
                )
                .order_by(DagVersion.id)  # ascending cus this is mostly for pre-3.0 upgrade
                .limit(1)
            )
        if not version.serialized_dag:
            log.error(
                "No serialized dag found",
                dag_id=tis[0].dag_id,
                version_id=version.id,
                version_number=version.version_number,
            )
            continue
        run_dag = version.serialized_dag.dag
        task_node_map = get_task_group_map(dag=run_dag)
        for ti in tis:
            # Skip the Task Instances if upstream/downstream filtering is applied or if the task was removed.
            if (
                task_node_map_exclude and ti.task_id not in task_node_map_exclude
            ) or ti.state == TaskInstanceState.REMOVED:
                continue

            # Populate the Grouped Task Instances (All Task Instances except the Parent Task Instances)
            if ti.task_id in get_child_task_map(
                parent_task_id=task_node_map[ti.task_id]["parent_id"], task_node_map=task_node_map
            ):
                all_tis[(ti.task_id, ti.run_id)].append(ti)
            # Populate the Parent Task Instances
            parent_id = task_node_map[ti.task_id]["parent_id"]
            if not parent_id and task_node_map[ti.task_id]["is_group"]:
                parent_tis[(ti.task_id, ti.run_id)].append(ti)
            elif parent_id and task_node_map[parent_id]["is_group"]:
                parent_tis[(parent_id, ti.run_id)].append(ti)

    # Clear task_node_map_exclude to free up memory
    if task_node_map_exclude:
        task_node_map_exclude.clear()

    task_node_map = get_task_group_map(dag=dag)
    # Extend subgroup task instances to parent task instances to calculate the aggregates states
    task_group_map = {k: v for k, v in task_node_map.items() if v["is_group"]}
    parent_tis.update(
        {
            (task_id_parent, run_id): parent_tis[(task_id_parent, run_id)] + parent_tis[(task_id, run_id)]
            for task_id, task_map in task_group_map.items()
            if task_map["is_group"]
            for (task_id_parent, run_id), tis in list(parent_tis.items())
            if task_id_parent == task_map["parent_id"]
        }
    )
    # Create the Task Instance Summaries to be used in the Grid Response
    task_instance_summaries: dict[str, list] = {
        run_id: [] for _, run_id in itertools.chain(parent_tis, all_tis)
    }

    # Fill the Task Instance Summaries for the Parent and Grouped Task Instances.
    # First the Parent Task Instances because they are used in the Grouped Task Instances
    fill_task_instance_summaries(
        grouped_task_instances=parent_tis,
        task_instance_summaries_to_fill=task_instance_summaries,
        session=session,
    )
    # Fill the Task Instance Summaries for the Grouped Task Instances
    fill_task_instance_summaries(
        grouped_task_instances=all_tis,
        task_instance_summaries_to_fill=task_instance_summaries,
        session=session,
    )

    # Aggregate the Task Instances by DAG Run
    grid_dag_runs = [
        GridDAGRunwithTIs(
            run_id=dag_run.run_id,
            queued_at=dag_run.queued_at,
            start_date=dag_run.start_date,
            end_date=dag_run.end_date,
            run_after=dag_run.run_after,
            logical_date=dag_run.logical_date,
            state=dag_run.state,
            run_type=dag_run.run_type,
            data_interval_start=dag_run.data_interval_start,
            data_interval_end=dag_run.data_interval_end,
            note=dag_run.note,
            task_instances=task_instance_summaries.get(dag_run.run_id, []),
        )
        for dag_run in dag_runs
    ]
    return GridResponse(dag_runs=grid_dag_runs)


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
    version = session.scalar(select(DagVersion).where(DagVersion.id == dag_version_id))
    if not version:
        version = session.scalar(
            select(DagVersion)
            .where(
                DagVersion.dag_id == dag_id,
            )
            .order_by(DagVersion.id)  # ascending cus this is mostly for pre-3.0 upgrade
            .limit(1)
        )
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
) -> list[GridNodeResponse]:
    """Return dag structure for grid view."""
    latest_serdag = _get_latest_serdag(dag_id, session)
    latest_dag = latest_serdag.dag

    # Retrieve, sort the previous DAG Runs
    base_query = select(DagRun.id).where(DagRun.dag_id == dag_id)
    # This comparison is to fall back to DAG timetable when no order_by is provided
    if order_by.value == order_by.get_primary_key_string():
        ordering = list(latest_dag.timetable.run_ordering)
        order_by = SortParam(
            allowed_attrs=ordering,
            model=DagRun,
        ).set_value(ordering[0])
    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        order_by=order_by,
        offset=offset,
        filters=[run_after],
        limit=limit,
    )
    run_ids = list(session.scalars(dag_runs_select_filter))

    task_group_sort = get_task_group_children_getter()
    if not run_ids:
        nodes = [task_group_to_dict_grid(x) for x in task_group_sort(latest_dag.task_group)]
        return nodes

    serdags = session.scalars(
        select(SerializedDagModel).where(
            SerializedDagModel.dag_version_id.in_(
                select(TaskInstance.dag_version_id)
                .join(TaskInstance.dag_run)
                .where(
                    DagRun.id.in_(run_ids),
                    SerializedDagModel.id != latest_serdag.id,
                )
            )
        )
    )
    merged_nodes: list[GridNodeResponse] = []
    dags = [latest_dag]
    for serdag in serdags:
        if serdag:
            dags.append(serdag.dag)
    for dag in dags:
        nodes = [task_group_to_dict_grid(x) for x in task_group_sort(dag.task_group)]
        _merge_node_dicts(merged_nodes, nodes)

    return merged_nodes


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
    if order_by.value == order_by.get_primary_key_string():
        latest_serdag = _get_latest_serdag(dag_id, session)
        latest_dag = latest_serdag.dag
        ordering = list(latest_dag.timetable.run_ordering)
        order_by = SortParam(
            allowed_attrs=ordering,
            model=DagRun,
        ).set_value(ordering[0])
    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        order_by=order_by,
        offset=offset,
        filters=[run_after],
        limit=limit,
    )
    return session.execute(dag_runs_select_filter)


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
            )
            .where(TaskInstance.dag_id == dag_id)
            .where(
                TaskInstance.run_id == run_id,
            )
        ),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance).set_value("task_id"),
        limit=None,
        return_total_entries=False,
    )
    task_instances = list(session.execute(tis_of_dag_runs))
    task_id_states = collections.defaultdict(list)
    for ti in task_instances:
        task_id_states[ti.task_id].append(ti.state)

    serdag = _get_serdag(
        dag_id=dag_id,
        dag_version_id=task_instances[0].dag_version_id,
        session=session,
    )
    if not serdag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")
    tis = list(
        _find_aggregates(
            node=serdag.dag.task_group,
            parent_node=None,
            ti_states=task_id_states,
        )
    )

    return {  # type: ignore[return-value]
        "run_id": run_id,
        "dag_id": dag_id,
        "task_instances": list(tis),
    }


@grid_router.get(
    "/latest_run/{dag_id}",
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
def get_latest_run(
    dag_id: str,
    session: SessionDep,
) -> LatestRunResponse | None:
    """
    Get information about the latest dag run by run_after.

    This is used by the UI to figure out if it needs to rerun queries and resume auto refresh.
    """
    return session.execute(
        select(
            DagRun.id,
            DagRun.dag_id,
            DagRun.run_id,
            DagRun.run_after,
        )
        .where(DagRun.dag_id == dag_id)
        .order_by(DagRun.run_after.desc())
        .limit(1)
    ).one_or_none()
