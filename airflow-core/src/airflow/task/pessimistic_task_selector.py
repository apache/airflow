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

from collections.abc import Collection
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import Column, and_, func, select, text
from sqlalchemy.orm import Query, selectinload
from sqlalchemy.sql import expression
from sqlalchemy.sql.selectable import CTE

from airflow.models import DagRun, TaskInstance
from airflow.models.base import Base
from airflow.models.dag import DagModel
from airflow.models.pool import Pool
from airflow.task.task_selector_strategy import TaskSelectorStrategy
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Query
    from sqlalchemy.sql.selectable import Select


@dataclass
class LimitWindowDescriptor:
    """
    Describes a limit window.

    Args:
        running_now_join (Subquery): the subquery on which we join for the window to get the additional parallelism
        limits data.
        running_now_join_predicates (Collection[str]): on what columns we do the join for for the
        running_now_join, also used in the group by.
        limit_column (Column): The column which decides how many rows should be chosen per window for
        concurency limits.
        window (expression.ColumnElement): the column by which we window.
        limit_join_model (Base | None): the model on which we join to get a concurency limit.
    """

    running_now_join: CTE
    running_now_join_predicates: Collection[str]
    limit_column: Column
    window: expression.ColumnElement
    limit_join_model: Base | None = None


TI = TaskInstance
DR = DagRun
DM = DagModel


class PessimisticTaskSelector(TaskSelectorStrategy):
    """
    Pessimisticly query task instances ready for scheduling.

    Works by delegating almost all the work from python do sql.
    Uses a few nested window functions to query only ready tasks.
    """

    def __init__(self) -> None:
        self.priority_order = [-TaskInstance.priority_weight, DagRun.logical_date, TaskInstance.map_index]

    def get_query(self, **additional_params) -> Query:
        priority_order = [-TI.priority_weight, DR.logical_date, TI.map_index]
        max_tis = additional_params["max_tis"]

        query = (
            select(TI)
            .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
            .join(TI.dag_run)
            .where(DR.state == DagRunState.RUNNING)
            .join(TI.dag_model)
            .where(~DM.is_paused)
            .where(TI.state == TaskInstanceState.SCHEDULED)
            .where(DM.bundle_name.is_not(None))
        )

        def running_tasks_group(
            group_fields: Collection[Column], states: Collection[TaskInstanceState] = EXECUTION_STATES
        ) -> CTE:
            return (
                select(*group_fields, func.count("*").label("now_running"))
                .where(TI.state.in_(states))
                .group_by(*group_fields)
                .cte()
            )

        def add_window_limit(query: Select, limit: LimitWindowDescriptor) -> Select:
            inner_query = query.add_columns(limit.window).subquery()
            query = (
                select(TI)
                .join(inner_query, TI.id == inner_query.c.id)
                .outerjoin(
                    limit.running_now_join,
                    and_(
                        *(
                            getattr(TI, predicate) == getattr(limit.running_now_join.c, predicate)
                            for predicate in limit.running_now_join_predicates
                        )
                    ),
                )
                .join(DR, TI.run_id == DR.run_id)
            )
            if limit.limit_join_model is not None:
                query = query.join(limit.limit_join_model)

            return query.where(
                and_(
                    func.coalesce(getattr(inner_query.c, limit.window.name), text("0"))
                    + func.coalesce(limit.running_now_join.c.now_running, text("0"))
                    <= func.coalesce(limit.limit_column, max_tis)
                )
            )

        running_total_tis_per_dagrun = running_tasks_group([TI.dag_id, TI.run_id])
        running_tis_per_dag = running_tasks_group([TI.dag_id, TI.task_id])
        running_total_tis_per_task_run = running_tasks_group([TI.dag_id, TI.run_id, TI.task_id])
        running_tis_per_pool = running_tasks_group([TI.pool], [*EXECUTION_STATES, TaskInstanceState.DEFERRED])

        total_tis_per_dagrun_count = (
            func.row_number()
            .over(partition_by=(TI.dag_id, TI.run_id), order_by=priority_order)
            .label("total_tis_per_dagrun_count")
        )
        tis_per_dag_count = (
            func.row_number()
            .over(partition_by=(TI.dag_id, TI.task_id), order_by=priority_order)
            .label("tis_per_dag_count")
        )
        mapped_tis_per_task_run_count = (
            func.row_number()
            .over(partition_by=(TI.dag_id, TI.run_id, TI.task_id), order_by=priority_order)
            .label("mapped_tis_per_dagrun_count")
        )
        pool_slots_taken = (
            func.sum(TI.pool_slots)
            .over(partition_by=(TI.pool), order_by=priority_order)
            .label("pool_slots_taken")
        )

        limits = [
            LimitWindowDescriptor(
                running_total_tis_per_dagrun,
                ["dag_id", "run_id"],
                DagModel.max_active_tasks,
                total_tis_per_dagrun_count,
                TI.dag_model,
            ),
            LimitWindowDescriptor(
                running_tis_per_dag, ["dag_id", "task_id"], TI.max_active_tis_per_dag, tis_per_dag_count
            ),
            LimitWindowDescriptor(
                running_total_tis_per_task_run,
                ["dag_id", "run_id", "task_id"],
                TI.max_active_tis_per_dagrun,
                mapped_tis_per_task_run_count,
            ),
            LimitWindowDescriptor(
                running_tis_per_pool, ["pool"], Pool.slots, pool_slots_taken, TI.pool_model
            ),
        ]

        for limit in limits:
            query = add_window_limit(query, limit)

        query = query.options(selectinload(TI.dag_model))
        query = query.limit(max_tis)

    def _running_tasks_group(self, *group_fields: Column) -> CTE:
        return (
            select(TI, func.count("*").label("now_running"))
            .where(TI.state.in_(EXECUTION_STATES))
            .group_by(*group_fields)
            .cte()
        )

    def _add_window_limit(
        self, priority_order: list[Column], query: Select, limit: LimitWindowDescriptor
    ) -> Select:
        inner_query = query.add_columns(limit.window).order_by(*priority_order).subquery()
        return (
            select(TI)
            .join(inner_query, TI.id == inner_query.c.id)
            .join(DR, TI.run_id == DR.id)
            .join(
                limit.running_now_join,
                *(
                    getattr(TI, predicate) == getattr(limit.running_now_join.c, predicate)
                    for predicate in limit.join_predicates
                ),
            )
            .where(
                getattr(inner_query.c, limit.window.name) + limit.running_now_join.c.now_running
                < limit.max_units
            )
        )
