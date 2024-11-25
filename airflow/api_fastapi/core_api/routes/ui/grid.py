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
import operator
from functools import cache
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Any

from airflow import DAG
from airflow.api_fastapi.common.db.common import get_session, paginated_select
from airflow.api_fastapi.common.parameters import (
    DateTimeQuery,
    QueryDagRunRunStatesFilter,
    QueryDagRunRunTypesFilter,
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridDAGRun,
    GridDAGRunwithTIs,
    GridResponse,
    GridTaskInstance,
    GridTaskInstanceSummary,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models import DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup

grid_router = AirflowRouter(prefix="/grid", tags=["Grid"])


@grid_router.get(
    "/",
    include_in_schema=False,
    responses=create_openapi_http_exception_doc([status.HTTP_400_BAD_REQUEST, status.HTTP_404_NOT_FOUND]),
)
def grid_data(
    dag_id: str,
    base_date: DateTimeQuery,
    run_types: QueryDagRunRunTypesFilter,
    run_states: QueryDagRunRunStatesFilter,
    session: Annotated[Session, Depends(get_session)],
    offset: QueryOffset,
    request: Request,
    num_runs: QueryLimit,
    root: str | None = None,
    filter_upstream: bool = False,
    filter_downstream: bool = False,
) -> GridResponse:
    """Return grid data."""
    ## Database calls to retrieve the DAG Runs and Task Instances and validate the data
    dag: DAG = request.app.state.dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")

    if root:
        dag = dag.partial_subset(
            task_ids_or_regex=root, include_upstream=filter_upstream, include_downstream=filter_downstream
        )

    if num_runs is None:
        num_runs = QueryLimit(conf.getint("webserver", "default_dag_run_display_number"))

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
        )
        .select_from(DagRun)
        .where(DagRun.dag_id == dag.dag_id, DagRun.logical_date <= base_date)
    )

    dag_runs_select_filter, _ = paginated_select(
        select=base_query,
        filters=[
            run_types,
            run_states,
        ],
        order_by=SortParam(allowed_attrs=["id"], model=DagRun),
        offset=offset,
        limit=num_runs,
    )

    dag_runs = session.execute(dag_runs_select_filter)
    # Validate the DAG Runs to have consistent data
    dag_runs = [GridDAGRun(**dag_run) for dag_run in dag_runs.all()]

    # Check if there are any DAG Runs with given criteria to eliminate unnecessary queries/errors
    if not dag_runs:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No DAG Runs found for DAG {dag.dag_id} with given criteria, please check the filters",
        )

    # Retrieve, sort and encode the Task Instances
    tis_of_dag_runs, _ = paginated_select(
        select=select(
            TaskInstance.run_id,
            TaskInstance.task_id,
            TaskInstance.try_number,
            TaskInstance.state,
            TaskInstance.start_date,
            TaskInstance.end_date,
            TaskInstance.queued_dttm.label("queued_dttm"),
        )
        .join(TaskInstance.task_instance_note, isouter=True)
        .where(TaskInstance.dag_id == dag.dag_id),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance),
        offset=offset,
        limit=None,
    )

    task_instances = session.execute(tis_of_dag_runs)

    # Validate the task instances to have consistent data
    task_instances = [GridTaskInstance(**ti) for ti in task_instances]

    ## Additional logic to calculate the overall state and task count dict of states
    priority: list[None | TaskInstanceState] = [
        TaskInstanceState.FAILED,
        TaskInstanceState.UPSTREAM_FAILED,
        TaskInstanceState.UP_FOR_RETRY,
        TaskInstanceState.UP_FOR_RESCHEDULE,
        TaskInstanceState.QUEUED,
        TaskInstanceState.SCHEDULED,
        TaskInstanceState.DEFERRED,
        TaskInstanceState.RUNNING,
        TaskInstanceState.RESTARTING,
        None,
        TaskInstanceState.SUCCESS,
        TaskInstanceState.SKIPPED,
        TaskInstanceState.REMOVED,
    ]

    @cache
    def get_task_group_children_getter() -> operator.methodcaller:
        sort_order = conf.get("webserver", "grid_view_sorting_order", fallback="topological")
        if sort_order == "topological":
            return operator.methodcaller("topological_sort")
        if sort_order == "hierarchical_alphabetical":
            return operator.methodcaller("hierarchical_alphabetical_sort")
        raise AirflowConfigException(f"Unsupported grid_view_sorting_order: {sort_order}")

    @cache
    def get_task_group_map() -> dict[str, dict[str, Any]]:
        task_nodes = {}

        def _fill_task_group_map_list(
            task_node: BaseOperator | TaskGroup | None, parent_node: BaseOperator | TaskGroup | None
        ):
            if task_node is None:
                return
            if isinstance(task_node, BaseOperator):
                task_nodes.update(
                    {
                        task_node.task_id: {
                            "is_group": False,
                            "parent_id": parent_node.node_id if parent_node else None,
                            "task_count": 1,
                        }
                    }
                )
                return
            elif isinstance(task_node, TaskGroup):
                task_nodes.update(
                    {
                        task_node.node_id: {
                            "is_group": True,
                            "parent_id": parent_node.node_id if parent_node else None,
                            "task_count": len(
                                [
                                    child
                                    for child in get_task_group_children_getter()(task_node)
                                    if isinstance(child, BaseOperator)
                                ]
                            ),
                        }
                    }
                )
                return [
                    _fill_task_group_map_list(child, task_node)
                    for child in get_task_group_children_getter()(task_node)
                ]

        for node in [child for child in get_task_group_children_getter()(dag.task_group)]:
            _fill_task_group_map_list(node, None)

        return task_nodes

    task_group_map = get_task_group_map()
    for ti in task_instances:
        if (
            task_group_map[ti.task_id]["is_group"]
            or not task_group_map[ti.task_id]["parent_id"]
            and task_group_map[ti.task_id]["is_group"]
        ):
            ti.task_id = ti.task_id
        elif task_group_map[ti.task_id]["parent_id"] and not task_group_map[ti.task_id]["is_group"]:
            ti.task_id = task_group_map[ti.task_id]["parent_id"]

    grouped_tis: dict[tuple, list[GridTaskInstance]] = collections.defaultdict(
        list,
        (
            ((task_id, run_id), list(tis))
            for (task_id, run_id), tis in itertools.groupby(
                task_instances, key=lambda ti: (ti.task_id, ti.run_id)
            )
        ),
    )

    task_instance_summaries: dict[str, list[GridTaskInstanceSummary]] = {
        ti.run_id: [] for ti in task_instances
    }

    for (task_id, run_id), tis in grouped_tis.items():
        overall_state = next(
            (state.value for ti in tis for state in priority if state is not None and ti.state == state), None
        )
        ti_try_number = max([ti.try_number for ti in tis])
        ti_start_date = min([ti.start_date for ti in tis if ti.start_date])
        ti_end_date = max([ti.end_date for ti in tis if ti.end_date])
        ti_queued_dttm = min([ti.queued_dttm for ti in tis if ti.queued_dttm], default=None)
        all_states = {"no_status" if state is None else state.name.lower(): 0 for state in priority}
        all_states.update(
            {
                "no_status" if state is None else state.name.lower(): len(
                    [ti for ti in tis if ti.state == state]
                )
                for state in priority
            }
        )

        task_instance_summaries[run_id].append(
            GridTaskInstanceSummary(
                task_id=task_id,
                try_number=ti_try_number,
                start_date=ti_start_date,
                end_date=ti_end_date,
                queued_dttm=ti_queued_dttm,
                states=all_states,
                task_count=task_group_map[task_id]["task_count"],
                overall_state=overall_state,
            )
        )

    # Aggregate the Task Instances by DAG Run
    grid_dag_runs = [
        GridDAGRunwithTIs(
            **dag_run.model_dump(),
            task_instances=task_instance_summaries[dag_run.run_id],
        )
        for dag_run in dag_runs
    ]

    return GridResponse(dag_runs=grid_dag_runs)
