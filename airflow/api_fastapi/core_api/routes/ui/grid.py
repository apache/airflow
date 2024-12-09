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

from airflow import DAG
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    OptionalDateTimeQuery,
    QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter,
    QueryIncludeDownstream,
    QueryIncludeUpstream,
    QueryLimit,
    QueryOffset,
    Range,
    RangeFilter,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridDAGRunwithTIs,
    GridResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.services.ui.grid import (
    fill_task_instance_summaries,
    get_dag_run_sort_param,
    get_task_group_map,
)
from airflow.models import DagRun, TaskInstance
from airflow.models.dagrun import DagRunNote
from airflow.models.taskinstance import TaskInstanceNote

grid_router = AirflowRouter(prefix="/grid", tags=["Grid"])


@grid_router.get(
    "/{dag_id}",
    include_in_schema=False,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
)
def grid_data(
    dag_id: str,
    run_types: QueryDagRunRunTypesFilter,
    run_states: QueryDagRunStateFilter,
    session: SessionDep,
    offset: QueryOffset,
    request: Request,
    limit: QueryLimit,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["logical_date", "data_interval_start", "data_interval_end", "start_date", "end_date"], DagRun
            ).dynamic_depends()
        ),
    ],
    include_upstream: QueryIncludeUpstream = False,
    include_downstream: QueryIncludeDownstream = False,
    logical_date_gte: OptionalDateTimeQuery = None,
    logical_date_lte: OptionalDateTimeQuery = None,
    root: str | None = None,
) -> GridResponse:
    """Return grid data."""
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    date_filter = RangeFilter(
        Range(lower_bound=logical_date_gte, upper_bound=logical_date_lte),
        attribute=DagRun.logical_date,
    )
    # Retrieve, sort and encode the previous DAG Runs
    base_query = (
        select(
            DagRun.run_id,
            DagRun.queued_at,
            DagRun.start_date,
            DagRun.end_date,
            DagRun.state,
            DagRun.run_type,
            DagRun.data_interval_start,
            DagRun.data_interval_end,
            DagRun.dag_version_id.label("version_number"),
            DagRunNote.content.label("note"),
        )
        .join(DagRun.dag_run_note, isouter=True)
        .select_from(DagRun)
        .where(DagRun.dag_id == dag.dag_id)
    )

    dag_runs_select_filter, _ = paginated_select(
        statement=base_query,
        filters=[
            run_types,
            run_states,
            date_filter,
        ],
        order_by=get_dag_run_sort_param(dag=dag, request_order_by=order_by),
        offset=offset,
        limit=limit,
    )

    dag_runs = session.execute(dag_runs_select_filter)

    # Check if there are any DAG Runs with given criteria to eliminate unnecessary queries/errors
    if not dag_runs:
        return GridResponse(dag_runs=[])

    # Retrieve, sort and encode the Task Instances
    tis_of_dag_runs, _ = paginated_select(
        statement=select(
            TaskInstance.run_id,
            TaskInstance.task_id,
            TaskInstance.try_number,
            TaskInstance.state,
            TaskInstance.start_date,
            TaskInstance.end_date,
            TaskInstance.queued_dttm.label("queued_dttm"),
            TaskInstanceNote.content.label("note"),
        )
        .join(TaskInstance.task_instance_note, isouter=True)
        .where(TaskInstance.dag_id == dag.dag_id),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance).set_value("task_id"),
        offset=offset,
        limit=None,
    )

    task_instances = session.execute(tis_of_dag_runs)

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
    parent_tis: dict[tuple[str, str], list] = collections.defaultdict(list)
    all_tis: dict[tuple[str, str], list] = collections.defaultdict(list)
    for ti in task_instances:
        if task_node_map_exclude and ti.task_id not in task_node_map_exclude.keys():
            continue
        all_tis[(ti.task_id, ti.run_id)].append(ti)
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
            **dag_run,
            task_instances=task_instance_summaries[dag_run.run_id]
            if dag_run.run_id in task_instance_summaries
            else [],
        )
        for dag_run in dag_runs
    ]

    return GridResponse(dag_runs=grid_dag_runs)
