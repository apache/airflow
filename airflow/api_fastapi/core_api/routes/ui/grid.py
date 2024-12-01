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

from fastapi import HTTPException, Request, status
from sqlalchemy import func, select
from sqlalchemy.sql.operators import ColumnOperators
from typing_extensions import Any

from airflow import DAG
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    OptionalDateTimeQuery,
    QueryDagRunRunTypesFilter,
    QueryDagRunStateFilter,
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridDAGRunwithTIs,
    GridResponse,
    GridTaskInstanceSummary,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models import DagRun, MappedOperator, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskmap import TaskMap
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import MappedTaskGroup, TaskGroup

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
    num_runs: QueryLimit,
    base_date: OptionalDateTimeQuery = None,
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

    current_time = timezone.utcnow()
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
        .where(DagRun.dag_id == dag.dag_id, DagRun.logical_date <= func.coalesce(base_date, current_time))
        .order_by(DagRun.id.desc())
    )

    def get_dag_run_sort_param():
        """Get the Sort Param for the DAG Run."""

        def _get_run_ordering_expr(name: str) -> ColumnOperators:
            """Get the Run Ordering Expression."""
            expr = DagRun.__mapper__.columns[name]
            # Data interval columns are NULL for runs created before 2.3, but SQL's
            # NULL-sorting logic would make those old runs always appear first. In a
            # perfect world we'd want to sort by ``get_run_data_interval()``, but that's
            # not efficient, so instead the columns are coalesced into logical_date,
            # which is good enough in most cases.
            if name in ("data_interval_start", "data_interval_end"):
                expr = func.coalesce(expr, DagRun.logical_date)
            return expr.desc()

        ordering_expression = (_get_run_ordering_expr(name) for name in dag.timetable.run_ordering)
        # create SortParam with ordering_expression and DagRun.id.desc()
        return ordering_expression

    dag_runs_select_filter, _ = paginated_select(
        statement=base_query.order_by(*get_dag_run_sort_param(), DagRun.id.desc()),
        filters=[
            run_types,
            run_states,
        ],
        order_by=None,
        offset=offset,
        limit=num_runs,
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
        )
        .join(TaskInstance.task_instance_note, isouter=True)
        .where(TaskInstance.dag_id == dag.dag_id),
        filters=[],
        order_by=SortParam(allowed_attrs=["task_id", "run_id"], model=TaskInstance).dynamic_depends(
            "task_id"
        )(),
        offset=offset,
        limit=None,
    )

    task_instances = session.execute(tis_of_dag_runs)

    @cache
    def get_task_group_children_getter() -> operator.methodcaller:
        """Get the Task Group Children Getter for the DAG."""
        sort_order = conf.get("webserver", "grid_view_sorting_order", fallback="topological")
        if sort_order == "topological":
            return operator.methodcaller("topological_sort")
        if sort_order == "hierarchical_alphabetical":
            return operator.methodcaller("hierarchical_alphabetical_sort")
        raise AirflowConfigException(f"Unsupported grid_view_sorting_order: {sort_order}")

    @cache
    def get_task_group_map() -> dict[str, dict[str, Any]]:
        """Get the Task Group Map for the DAG."""
        task_nodes = {}

        def _fill_task_group_map(
            task_node: BaseOperator | MappedTaskGroup | TaskMap | None,
            parent_node: BaseOperator | MappedTaskGroup | TaskMap | None,
        ):
            """Recursively fill the Task Group Map."""
            if task_node is None:
                return
            if isinstance(task_node, MappedOperator):
                task_nodes[task_node.node_id] = {
                    "is_group": False,
                    "parent_id": parent_node.node_id if parent_node else None,
                    "task_count": task_node,
                }
                return
            elif isinstance(task_node, BaseOperator):
                task_nodes[task_node.task_id] = {
                    "is_group": False,
                    "parent_id": parent_node.node_id if parent_node else None,
                    "task_count": 1,
                }
                return
            elif isinstance(task_node, TaskGroup):
                task_nodes[task_node.node_id] = {
                    "is_group": True,
                    "parent_id": parent_node.node_id if parent_node else None,
                    "task_count": len([child for child in get_task_group_children_getter()(task_node)]),
                }
                return [
                    _fill_task_group_map(task_node=child, parent_node=task_node)
                    for child in get_task_group_children_getter()(task_node)
                ]

        for node in [child for child in get_task_group_children_getter()(dag.task_group)]:
            _fill_task_group_map(task_node=node, parent_node=None)

        return task_nodes

    # Generate Grouped Task Instances
    task_node_map = get_task_group_map()
    parent_tis: dict[tuple[str, str], list] = collections.defaultdict(list)
    all_tis: dict[tuple[str, str], list] = collections.defaultdict(list)
    for ti in task_instances:
        all_tis[(ti.task_id, ti.run_id)].append(ti)
        parent_id = task_node_map[ti.task_id]["parent_id"]
        if not parent_id and task_node_map[ti.task_id]["is_group"]:
            parent_tis[(ti.task_id, ti.run_id)].append(ti)
        elif parent_id and task_node_map[parent_id]["is_group"]:
            parent_tis[(parent_id, ti.run_id)].append(ti)

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

    def fill_task_instance_summaries(
        grouped_task_instances: dict[tuple[str, str], list],
        task_instance_summaries_to_fill: dict[str, list],
    ):
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

        for (task_id, run_id), tis in grouped_task_instances.items():
            overall_state = next(
                (state.value for ti in tis for state in priority if state is not None and ti.state == state),
                None,
            )
            ti_try_number = max([ti.try_number for ti in tis])
            ti_start_date = min([ti.start_date for ti in tis if ti.start_date], default=None)
            ti_end_date = max([ti.end_date for ti in tis if ti.end_date], default=None)
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
            # Task Count is either integer or a TaskGroup to get the task count
            task_count = task_node_map[task_id]["task_count"]
            task_instance_summaries_to_fill[run_id].append(
                GridTaskInstanceSummary(
                    task_id=task_id,
                    try_number=ti_try_number,
                    start_date=ti_start_date,
                    end_date=ti_end_date,
                    queued_dttm=ti_queued_dttm,
                    states=all_states,
                    task_count=task_count
                    if type(task_count) is int
                    else task_count.get_mapped_ti_count(run_id=run_id, session=session),
                    overall_state=overall_state,
                )
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
    )
    # Fill the Task Instance Summaries for the Grouped Task Instances
    fill_task_instance_summaries(
        grouped_task_instances=all_tis,
        task_instance_summaries_to_fill=task_instance_summaries,
    )

    # Aggregate the Task Instances by DAG Run
    grid_dag_runs = [
        GridDAGRunwithTIs(
            **dag_run,
            task_instances=task_instance_summaries[dag_run.run_id],
        )
        for dag_run in dag_runs
    ]

    return GridResponse(dag_runs=grid_dag_runs)
