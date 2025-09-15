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

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, TypedDict

from sqlalchemy import Column, and_, column, func, select, text
from sqlalchemy.orm import Query, selectinload
from typing_extensions import Unpack

from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DagModel
from airflow.models.pool import Pool
from airflow.task.task_selector_strategy import TaskSelectorStrategy
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Query
    from sqlalchemy.sql import expression
    from sqlalchemy.sql.selectable import CTE, Select

    from airflow.configuration import AirflowConfigParser
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
    from airflow.models.base import Base


class ParamsProviderType(TypedDict):
    """Allows for better type hints."""

    conf: AirflowConfigParser
    scheduler_job_runner: SchedulerJobRunner | None


@dataclass
class LimitWindowDescriptor:
    """
    Describes a limit window.

    Args:
        running_now_join (Subquery): the subquery on which we join for the window to get the additional parallelism
        limits data, part of the so called "predicate map" (explanation later in the code).
        running_now_join_predicates (Collection[str]): on what (additional) columns we do the join for for the running_now_join.
        running_now_join, also used in the group by.
        limit_column (Column): the column by which we check the window result, the window result has to be
        less than or equal to the limit_column (used for concurrency limits).
        window (expression.ColumnElement): the window expression itself.
        limit_join_model (Base | None): the model on which we join to get an additional concurrency limit,
        sometimes it is needed for limits which are outside of dagrun, taskinstance and dag.
        additional_select_from_previous_query (list[str]): additional fields and columns to select from previous window in order to reduce joins.
    """

    running_now_join: CTE
    running_now_join_predicates: Collection[str]
    limit_column: Column
    window: expression.ColumnElement
    limit_join_model: Base | None = None
    additional_select_from_previous_query: list[str] = field(default_factory=list)


TI = TaskInstance
DR = DagRun
DM = DagModel


class PessimisticTaskSelector(TaskSelectorStrategy):
    """
    Pessimisticly query task instances ready for scheduling.

    Works by delegating almost all the work from python do sql.
    Uses a few nested window functions to query only ready tasks.

    The query is built in a dynamic manner, meaning, it can be extended easily
    but it might be hard to understand how everything connects.

    Each window checks a single concurrency limit (i.e parallelism, dag max active tasks, for more info visit https://stackoverflow.com/questions/56370720/how-to-control-the-parallelism-or-concurrency-of-an-airflow-installation)

    as of now, there exist 4 windows that check `mapped_tis_per_task_run_count` which checks for mapped
    tasks, `total_tis_per_dagrun_count` which checks for tis across all dagruns, `tis_per_dag_count` which
    check for specific task across all dagruns and `pool_slots_taken` which checks for pool slots taken by all
    tasks we want to set to queued.

    For each window, we have a value whome reflects the current state of the limit.
    These are called `concurrency map`, the name is taken from optimistic implementation.

    These include:

        `running_total_tis_per_dagrun`: counts all running tis per dagrun for the `total_tis_per_dagrun_count` window.

        `running_tis_per_dag`: counts all specific ti's across a dag (across all dagruns) for the `tis_per_dag_count` window.

        `running_total_tis_per_task_run`: counts all mapped tasks of specific task run for the `mapped_tis_per_task_run_count` window.

        `running_tis_per_pool`: which counts how many tasks are running per pool for the `pool_slots_taken` window.

    All of these are created using the `running_tasks_group` and are called the `Concurrency Map` function which just counts and groups tasks by the fields we give it, along with having some predicates to only count running tasks.
    """

    def __init__(self) -> None:
        self.priority_order = [-TaskInstance.priority_weight, DagRun.logical_date, TaskInstance.map_index]
        self.toggle_reverse_query_order = False

    def get_query(self, **additional_params) -> Query:
        max_tis = additional_params["max_tis"]

        query = (
            select(TI.id, DR.logical_date)
            .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
            .join(DR, and_(TI.run_id == DR.run_id, TI.dag_id == DR.dag_id))
            .where(DR.state == DagRunState.RUNNING)
            .join(TI.dag_model)
            .where(~DM.is_paused)
            .where(TI.state == TaskInstanceState.SCHEDULED)
            .where(DM.bundle_name.is_not(None))
        )

        def running_tasks_group(
            name: str,
            group_fields: Collection[Column],
            states: Collection[TaskInstanceState] = EXECUTION_STATES,
        ) -> CTE:
            return (
                select(*group_fields, func.count("*").label("now_running"))
                .where(TI.state.in_(states))
                .group_by(*group_fields)
                .cte(name)
            )

        def add_window_limit(query: Select, limit: LimitWindowDescriptor) -> Select:
            """
            Create a query with an added window limit from the LimitWindowDescriptor.

            This function uses the old query and the results from the old query (the query field)
            to join on the new window created dynamically (in order to use the tasks queried from the old query),
            and it is used to filter more and more tasks each window we create.
            It allows for additional outer join predicates in case they are needed.

            Priority Behaviour:
                The behaviour of priorities using this task selector is that we always drop less prioritized tasks,
                as long as they are related (meaning either same pool, same dag, dagrun or task (for mapped)).
                if the tasks are not related, we can have a lower priority task running before a higher priority one.
                Related tasks are tasks which have any common trait (i.e same pool), meaning,
                they can cause another related task to not run because they occupy some slot in the concurrency map of the same task.
                Otherwise, we can run lower priority tasks as they do not relate in any way and cannot interfere with a higher priority task running.
            """
            cte_query = query.add_columns(limit.window).cte()
            query = (
                select(
                    TI.id,
                    *(
                        getattr(cte_query.c, additional_select)
                        for additional_select in limit.additional_select_from_previous_query
                    ),
                )
                .join(cte_query, TI.id == cte_query.c.id)
                .outerjoin(
                    limit.running_now_join,
                    and_(
                        *(
                            getattr(TI, predicate) == getattr(limit.running_now_join.c, predicate)
                            for predicate in limit.running_now_join_predicates
                        )
                    ),
                )
            )
            if limit.limit_join_model is not None:
                query = query.join(limit.limit_join_model)

            return query.where(
                and_(
                    func.coalesce(getattr(cte_query.c, limit.window.name), text("0"))
                    + func.coalesce(limit.running_now_join.c.now_running, text("0"))
                    <= func.coalesce(limit.limit_column, max_tis)
                )
            )

        running_total_tis_per_dagrun = running_tasks_group("dag_run_active_tasks", [TI.dag_id, TI.run_id])
        running_tis_per_dag = running_tasks_group("active_tis_across_dag_runs", [TI.dag_id, TI.task_id])
        running_total_tis_per_task_run = running_tasks_group(
            "active_tis_in_one_task", [TI.dag_id, TI.run_id, TI.task_id]
        )
        running_tis_per_pool = running_tasks_group(
            "pool_active_tasks", [TI.pool], [*EXECUTION_STATES, TaskInstanceState.DEFERRED]
        )

        window_order_by: Collection[Column] = [
            column("priority_weight"),
            column("logical_date"),
            column("map_index"),
            func.random(),
        ]

        additional_select_values: list[str] = ["logical_date"]

        total_tis_per_dagrun_count = (
            func.row_number()
            .over(partition_by=(TI.dag_id, TI.run_id), order_by=window_order_by, rows=(None, 0))
            .label("total_tis_per_dagrun_count")
        )
        tis_per_dag_count = (
            func.row_number()
            .over(partition_by=(TI.dag_id, TI.task_id), order_by=window_order_by, rows=(None, 0))
            .label("tis_per_dag_count")
        )
        mapped_tis_per_task_run_count = (
            func.row_number()
            .over(partition_by=(TI.dag_id, TI.run_id, TI.task_id), order_by=window_order_by, rows=(None, 0))
            .label("mapped_tis_per_dagrun_count")
        )
        pool_slots_taken = (
            func.sum(TI.pool_slots)
            .over(partition_by=(TI.pool), order_by=window_order_by, rows=(None, 0))
            .label("pool_slots_taken_sum")
        )

        # The order of the LimitWindowDescriptors here matters, it is arranged to go from the
        # most specific limit to least specific limit, where mapped tasks are first, as it only drops mapped
        # tasks which do not pass their concurrency limit.
        # we then have the dagrun window, which selects all tasks which a dagrun can run according to given limits.
        # the next is the tis per dag, which is included in the dagrun window as it checks a "row" of tasks across all dagruns.
        # and the last is pool, the least specific, to select the most prioritized tasks for each pool

        limits = [
            LimitWindowDescriptor(
                running_total_tis_per_task_run,
                ["dag_id", "run_id", "task_id"],
                TI.max_active_tis_per_dagrun,
                mapped_tis_per_task_run_count,
                additional_select_from_previous_query=additional_select_values,
            ),
            LimitWindowDescriptor(
                running_tis_per_dag,
                ["dag_id", "task_id"],
                TI.max_active_tis_per_dag,
                tis_per_dag_count,
                additional_select_from_previous_query=additional_select_values,
            ),
            LimitWindowDescriptor(
                running_total_tis_per_dagrun,
                ["dag_id", "run_id"],
                DagModel.max_active_tasks,
                total_tis_per_dagrun_count,
                TI.dag_model,
                additional_select_from_previous_query=additional_select_values,
            ),
            LimitWindowDescriptor(
                running_tis_per_pool,
                ["pool"],
                Pool.slots,
                pool_slots_taken,
                TI.pool_model,
                additional_select_from_previous_query=additional_select_values,
            ),
        ]

        if self.toggle_reverse_query_order:
            # reverse the query order to solve all possible starvation cases
            # as it is not possible to solve all starvation cases using sql WF
            # due to the fact that they are not dynamic, and tasks will be dropped
            # even if they can be scheduled in favor of other non schedulable tasks because
            # of the usage of row numbers, it also solves the issue of orthogonal limits.
            limits.reverse()

        self.toggle_reverse_query_order = not self.toggle_reverse_query_order

        for limit in limits:
            query = add_window_limit(query, limit)

        query = query.with_only_columns([TI])
        query = query.options(selectinload(TI.dag_model))
        query = query.limit(max_tis)

        return query


def get_params_for_pessimistic_selector(
    **kwargs: Unpack[ParamsProviderType],
) -> Mapping[str, Any]:
    conf = kwargs["conf"]

    params = {}

    params["max_tis"] = conf.getint("scheduler", "max_tis_per_query")

    return params


PESSIMISTIC_SELECTOR = "PESSIMISTIC"
