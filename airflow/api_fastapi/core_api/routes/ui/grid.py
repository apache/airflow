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

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow import DAG
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
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridDAGRunwithTIs,
    GridResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.services.ui.grid import (
    fill_task_instance_summaries,
    get_child_task_map,
    get_task_group_map,
)
from airflow.models import DagRun, TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory

grid_router = AirflowRouter(prefix="/grid", tags=["Grid"])


@grid_router.get(
    "/{dag_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
)
def grid_data(
    dag_id: str,
    session: SessionDep,
    offset: QueryOffset,
    request: Request,
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
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
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

    dag_runs = session.scalars(dag_runs_select_filter).unique()

    # Check if there are any DAG Runs with given criteria to eliminate unnecessary queries/errors
    if not dag_runs:
        return GridResponse(dag_runs=[])

    # Retrieve, sort and encode the Task Instances
    tis_of_dag_runs, _ = paginated_select(
        statement=select(TaskInstance)
        .join(TaskInstance.task_instance_note, isouter=True)
        .where(TaskInstance.dag_id == dag.dag_id),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance).set_value("task_id"),
        offset=offset,
        limit=None,
    )

    task_instances = session.scalars(tis_of_dag_runs)

    # Generate Grouped Task Instances
    task_node_map_exclude = None
    if root:
        task_node_map_exclude = get_task_group_map(
            dag=dag.partial_subset(
                task_ids_or_regex=root,
                include_upstream=include_upstream,
                include_downstream=include_downstream,
            )
        )

    task_node_map = get_task_group_map(dag=dag)
    # Group the Task Instances by Parent Task (TaskGroup or Mapped) and All Task Instances
    parent_tis: dict[tuple[str, str], list] = collections.defaultdict(list)
    all_tis: dict[tuple[str, str], list] = collections.defaultdict(list)
    for ti in task_instances:
        # Skip the Task Instances if upstream/downstream filtering is applied
        if task_node_map_exclude and ti.task_id not in task_node_map_exclude.keys():
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

    # Extend subgroup task instances to parent task instances to calculate the aggregates states
    task_group_map = {k: v for k, v in task_node_map.items() if v["is_group"]}
    parent_tis.update(
        {
            (task_id_parent, run_id): parent_tis[(task_id_parent, run_id)] + parent_tis[(task_id, run_id)]
            for task_id, task_map in task_group_map.items()
            if task_map["is_group"]
            for (task_id_parent, run_id), tis in parent_tis.items()
            if task_id_parent == task_map["parent_id"]
        }
    )
    # Create the Task Instance Summaries to be used in the Grid Response
    task_instance_summaries: dict[str, list] = {
        run_id: [] for (_, run_id), _ in itertools.chain(parent_tis.items(), all_tis.items())
    }

    # Fill the Task Instance Summaries for the Parent and Grouped Task Instances.
    # First the Parent Task Instances because they are used in the Grouped Task Instances
    fill_task_instance_summaries(
        grouped_task_instances=parent_tis,
        task_instance_summaries_to_fill=task_instance_summaries,
        task_node_map=task_node_map,
        session=session,
    )
    # Fill the Task Instance Summaries for the Grouped Task Instances
    fill_task_instance_summaries(
        grouped_task_instances=all_tis,
        task_instance_summaries_to_fill=task_instance_summaries,
        task_node_map=task_node_map,
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
            version_number=dag_run.version_number,
            note=dag_run.note,
            task_instances=(
                task_instance_summaries[dag_run.run_id] if dag_run.run_id in task_instance_summaries else []
            ),
        )
        for dag_run in dag_runs
    ]

    return GridResponse(dag_runs=grid_dag_runs)
