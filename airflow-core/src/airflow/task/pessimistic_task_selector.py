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

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import Column, func, select
from sqlalchemy.orm import Query, selectinload
from sqlalchemy.sql import expression
from sqlalchemy.sql.selectable import CTE

from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DagModel
from airflow.models.pool import Pool
from airflow.task.task_selector_strategy import TaskSelectorStrategy
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Query
    from sqlalchemy.sql.selectable import Select, Subquery


@dataclass
class LimitWindowDescriptor:
    """
    Describes a limit window.

    Args:
        join_on (Subquery): the subquery on which we join for the window to get the additional parallelism
        limits data.
        choose_up_to (Column): a column which describes how many TI's we select.
        window_over (ColumnElement): the column over which we window
    """

    join_on: Subquery
    choose_up_to: Column
    window_over: expression.ColumnElement


class PessimisticTaskQuerierStrategy(TaskSelectorStrategy):
    """Pessimisticly query task instances ready for scheduling."""

    def get_query(self, **additional_params) -> Query:
        priority_order: list[Column] = additional_params["priority_order"]
        max_tis: int = additional_params["max_tis"]
        query = (
            select(TaskInstance)
            .with_hint(TaskInstance, "USE INDEX (ti_state)", dialect_name="mysql")
            .join(TaskInstance.dag_run)
            .where(DagRun.state == DagRunState.RUNNING)
            .join(TaskInstance.dag_model)
            .where(~DagModel.is_paused)
            .where(TaskInstance.state == TaskInstanceState.SCHEDULED)
            .where(DagModel.bundle_name.is_not(None))
            .options(selectinload(TaskInstance.dag_model))
        )

        def running_tasks_group(*group_fields: Column) -> CTE:
            return (
                select(TaskInstance, func.count("*").label("now_running"))
                .where(TaskInstance.state.in_(EXECUTION_STATES))
                .group_by(*group_fields)
                .cte()
            )

        def add_window_limit(query: Select, limit: LimitWindowDescriptor) -> Select:
            inner_query = (
                query.add_columns(limit.window_over)
                .join(limit.join_on, TaskInstance.id == limit.join_on.c.id)
                .subquery()
            )
            return (
                select(TaskInstance)
                .join(inner_query, TaskInstance.id == inner_query.c.id)
                .where(
                    getattr(inner_query.c, limit.window_over.name) + limit.join_on.c.now_running
                    < limit.choose_up_to
                )
            )

        running_total_tis_per_dagrun = running_tasks_group(TaskInstance.dag_id, TaskInstance.run_id)
        running_tis_per_dag = running_tasks_group(TaskInstance.dag_id, TaskInstance.task_id)
        running_total_tis_per_task_run = running_tasks_group(
            TaskInstance.dag_id, TaskInstance.run_id, TaskInstance.task_id
        )
        running_tis_per_pool = running_tasks_group(TaskInstance.pool)

        total_tis_per_dagrun_count = (
            func.row_number()
            .over(partition_by=(TaskInstance.dag_id, TaskInstance.run_id), order_by=priority_order)
            .label("total_tis_per_dagrun_count")
        )
        tis_per_dag_count = (
            func.row_number()
            .over(partition_by=(TaskInstance.dag_id, TaskInstance.task_id), order_by=priority_order)
            .label("tis_per_dag_count")
        )
        mapped_tis_per_task_run_count = (
            func.row_number()
            .over(
                partition_by=(TaskInstance.dag_id, TaskInstance.run_id, TaskInstance.task_id),
                order_by=priority_order,
            )
            .label("mapped_tis_per_dagrun_count")
        )
        pool_slots_taken = (
            func.sum(TaskInstance.pool_slots)
            .over(partition_by=(TaskInstance.pool), order_by=priority_order)
            .label("pool_slots_taken")
        )

        limits = [
            LimitWindowDescriptor(
                running_total_tis_per_dagrun, DagModel.max_active_tasks, total_tis_per_dagrun_count
            ),
            LimitWindowDescriptor(
                running_tis_per_dag, TaskInstance.max_active_tis_per_dag, tis_per_dag_count
            ),
            LimitWindowDescriptor(
                running_total_tis_per_task_run,
                TaskInstance.max_active_tis_per_dagrun,
                mapped_tis_per_task_run_count,
            ),
            LimitWindowDescriptor(running_tis_per_pool, Pool.slots, pool_slots_taken),
        ]

        for limit in limits:
            query = add_window_limit(query, limit)

        query = query.limit(max_tis)

        return query
