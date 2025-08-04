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

import itertools
from collections import Counter
from functools import cache
from typing import TYPE_CHECKING

from sqlalchemy import Column, select, tuple_, update
from sqlalchemy.orm import Query, Session, selectinload

from airflow.models import DagRun, Pool, TaskInstance
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.serialized_dag import SerializedDagModel
from airflow.stats import Stats
from airflow.task.task_selector_strategy import TaskSelectorStrategy
from airflow.utils.concurency import ConcurrencyMap
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from airflow.models.pool import PoolStats


class OptimisticTaskSelector(TaskSelectorStrategy, LoggingMixin):
    """
    Optimistic task selector.

    This task selector contains the way the old airflow scheduler fetches tasks.
    It works in an optimistic manner meaning that it always fetches `max_tis` tasks untill it can get at least
    1 task to be scheduled, this causes issues as described in GitHub Issue #45636.
    The 'old scheduler' can be seen here:
    https://github.com/apache/airflow/blob/478a2b458bae57a04aebcda3ac5b231fb49ff764/airflow-core/src/airflow/jobs/scheduler_job_runner.py#L293
    """

    def __init__(self) -> None:
        super().__init__()
        self.num_starving_tasks_total = 0
        self.priority_order = [-TaskInstance.priority_weight, DagRun.logical_date, TaskInstance.map_index]

    @cache
    def get_query(self, **additional_params) -> Query:
        priority_order = self.priority_order
        max_tis = additional_params.get("max_tis", 32)
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
            .order_by(*priority_order)
            .limit(max_tis)
        )

        return query

    def query_tasks_with_locks(self, session: Session, **additional_params) -> Query:
        priority_order: list[Column] = additional_params["priority_order"]
        executor_slots_available: dict[str, int] = additional_params["executor_slots_available"]
        max_tis: int = additional_params.get("max_tis", 32)

        query = self.get_query(
            priority_order=priority_order,
            max_tis=max_tis,
        )

        pools, pool_slots_free = self._get_pool_stats(session)

        if pool_slots_free == 0:
            self.log.debug("All pools are full!")
            return []

        starved_pools = {pool_name for pool_name, stats in pools.items() if stats["open"] <= 0}

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        concurrency_map = ConcurrencyMap()
        concurrency_map.load(session=session)

        # Number of tasks that cannot be scheduled because of no open slot in pool
        num_starving_tasks_total = 0

        # dag and task ids that can't be queued because of concurrency limits
        starved_dags: set[str] = set()
        starved_tasks: set[tuple[str, str]] = set()
        starved_tasks_task_dagrun_concurrency: set[tuple[str, str, str]] = set()

        # TODO: think about whether the metrics should be extrected from here or not
        pool_num_starving_tasks: dict[str, int] = Counter()
        executable_tis: list[TaskInstance] = []

        for loop_count in itertools.count(start=1):
            num_starved_pools = len(starved_pools)
            num_starved_dags = len(starved_dags)
            num_starved_tasks = len(starved_tasks)
            num_starved_tasks_task_dagrun_concurrency = len(starved_tasks_task_dagrun_concurrency)

            query = self._add_query_predicates(
                query,
                starved_pools,
                starved_tasks,
                starved_dags,
                starved_tasks_task_dagrun_concurrency,
            )
            query = with_row_locks(query, of=TaskInstance, session=session, skip_locked=True)
            tasks_to_examine = session.scalars(query).all()

            executable_tis_result, total_starved_tasks = self._examine_task_instances(
                session,
                tasks_to_examine,
                pools,
                pool_num_starving_tasks,
                concurrency_map,
                starved_pools,
                starved_tasks,
                starved_dags,
                starved_tasks_task_dagrun_concurrency,
                executor_slots_available,
            )

            executable_tis.extend(executable_tis_result)

            num_starving_tasks_total += total_starved_tasks

            is_done = executable_tis or len(tasks_to_examine) < max_tis
            # Check this to avoid accidental infinite loops
            found_new_filters = (
                len(starved_pools) > num_starved_pools
                or len(starved_dags) > num_starved_dags
                or len(starved_tasks) > num_starved_tasks
                or len(starved_tasks_task_dagrun_concurrency) > num_starved_tasks_task_dagrun_concurrency
            )

            if is_done or not found_new_filters:
                break

            self.log.info(
                "Found no task instances to queue on query iteration %s "
                "but there could be more candidate task instances to check.",
                loop_count,
            )

        for pool_name, num_starving_tasks in pool_num_starving_tasks.items():
            Stats.gauge(f"pool.starving_tasks.{pool_name}", num_starving_tasks)
            # Same metric with tagging
            Stats.gauge("pool.starving_tasks", num_starving_tasks, tags={"pool_name": pool_name})

        Stats.gauge("scheduler.tasks.starving", num_starving_tasks_total)

        return executable_tis

    def _examine_task_instances(
        self,
        session: Session,
        task_instances_to_examine: list[TaskInstance],
        pools: dict[str, PoolStats],
        pool_num_starving_tasks: dict[str, int],
        concurrency_map: ConcurrencyMap,
        starved_pools: set[str],
        starved_tasks: set[tuple[str, str]],
        starved_dags: set[str],
        starved_tasks_task_dagrun_concurrency: set[tuple[str, str, str]],
        executor_slots_available: dict[str, int],
    ) -> tuple[list[TaskInstance], int]:
        executable_tis: list[TaskInstance] = []
        num_starving_tasks_total: int = 0

        for task_instance in task_instances_to_examine:
            dag_id = task_instance.dag_id
            dag_run_key = (dag_id, task_instance.run_id)
            pool_name = task_instance.pool

            pool_stats = pools.get(pool_name)

            if not pool_stats:
                self.log.warning("Tasks using non-existent pool '%s' will not be scheduled", pool_name)
                starved_pools.add(pool_name)
                continue

            open_slots = pool_stats["open"]

            can_tasks_schedule_on_pool = self._check_pool_slots_predicates(task_instance, pool_stats)

            if not can_tasks_schedule_on_pool:
                # Can't schedule any more, predicates checked in the method above.
                pool_num_starving_tasks[pool_name] += 1
                num_starving_tasks_total += 1
                starved_pools.add(pool_name)

                continue

            # Make sure to emit metrics if pool has no starving tasks
            pool_num_starving_tasks.setdefault(pool_name, 0)

            if not self._check_dag_max_active_tasks_not_exceeded(
                task_instance,
                concurrency_map,
                starved_dags,
            ):
                continue

            if task_instance.dag_model.has_task_concurrency_limits:
                if (
                    not self._check_not_serialized_dag(task_instance, session)
                    or not self._check_task_dag_concurency(task_instance, starved_tasks, concurrency_map)
                    or not self._check_task_dagrun_concurency(
                        task_instance, starved_tasks_task_dagrun_concurrency, concurrency_map
                    )
                ):
                    continue

            executable_tis.append(task_instance)
            open_slots -= task_instance.pool_slots
            concurrency_map.dag_run_active_tasks_map[dag_run_key] += 1
            concurrency_map.task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1
            concurrency_map.task_dagrun_concurrency_map[
                (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
            ] += 1

            pool_stats["open"] = open_slots

            if executor_name := task_instance.executor:
                if TYPE_CHECKING:
                    # All executors should have a name if they are initted from the executor_loader.
                    # But we need to check for None to make mypy happy.
                    assert executor_name
                if executor_slots_available[executor_name] <= 0:
                    self.log.debug(
                        "Not scheduling %s since its executor %s does not currently have any more "
                        "available slots"
                    )
                    starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                    continue
                executor_slots_available[executor_name] -= 1
            else:
                # This is a defensive guard for if we happen to have a task who's executor cannot be
                # found. The check in the dag parser should make this not realistically possible but the
                # loader can fail if some direct DB modification has happened or another as yet unknown
                # edge case. _try_to_load_executor will log an error message explaining the executor
                # cannot be found.
                starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                continue

        return executable_tis, num_starving_tasks_total

    def _add_query_predicates(
        self,
        query: Query,
        starved_pools: set[str],
        starved_tasks: set[tuple[str, str]],
        starved_dags: set[str],
        starved_tasks_task_dagrun_concurrency: set[tuple[str, str, str]],
    ) -> Query:
        if starved_pools:
            query = query.where(TaskInstance.pool.not_in(starved_pools))

        if starved_dags:
            query = query.where(TaskInstance.dag_id.not_in(starved_dags))

        if starved_tasks:
            query = query.where(tuple_(TaskInstance.dag_id, TaskInstance.task_id).not_in(starved_tasks))

        if starved_tasks_task_dagrun_concurrency:
            query = query.where(
                tuple_(TaskInstance.dag_id, TaskInstance.run_id, TaskInstance.task_id).not_in(
                    starved_tasks_task_dagrun_concurrency
                )
            )

    def _get_pool_stats(self, session: Session) -> tuple[dict[str, PoolStats], int]:
        # Get the pool settings. We get a lock on the pool rows, treating this as a "critical section"
        # Throws an exception if lock cannot be obtained, rather than blocking
        pools = Pool.slots_stats(lock_rows=True, session=session)

        # If the pools are full, there is no point doing anything!
        # If _somehow_ the pool is overfull, don't let the limit go negative - it breaks SQL
        pool_slots_free = sum(max(0, pool["open"]) for pool in pools.values())

        return pools, pool_slots_free

    def _check_pool_slots_predicates(self, task_instance: TaskInstance, pool_stats: PoolStats) -> bool:
        pool_name = task_instance.pool

        pool_total = pool_stats["total"]
        open_slots = pool_stats["open"]
        if open_slots <= 0:
            self.log.info("Not scheduling since there are %s open slots in pool %s", open_slots, pool_name)
            return False

        if task_instance.pool_slots > pool_total:
            self.log.warning(
                "Not executing %s. Requested pool slots (%s) are greater than "
                "total pool slots: '%s' for pool: %s.",
                task_instance,
                task_instance.pool_slots,
                pool_total,
                pool_name,
            )

            return False

        if task_instance.pool_slots > open_slots:
            self.log.info(
                "Not executing %s since it requires %s slots but there are %s open slots in the pool %s.",
                task_instance,
                task_instance.pool_slots,
                open_slots,
                pool_name,
            )
            return False

        return True

    def _check_dag_max_active_tasks_not_exceeded(
        self,
        task_instance: TaskInstance,
        concurrency_map: ConcurrencyMap,
        starved_dags: set[str],
    ) -> bool:
        # Check to make sure that the task max_active_tasks of the DAG hasn't been
        # reached.
        dag_id = task_instance.dag_id
        dag_run_key = (dag_id, task_instance.run_id)
        current_active_tasks_per_dag_run = concurrency_map.dag_run_active_tasks_map[dag_run_key]
        dag_max_active_tasks = task_instance.dag_model.max_active_tasks
        self.log.info(
            "DAG %s has %s/%s running and queued tasks",
            dag_id,
            current_active_tasks_per_dag_run,
            dag_max_active_tasks,
        )
        if current_active_tasks_per_dag_run >= dag_max_active_tasks:
            self.log.info(
                "Not executing %s since the number of tasks running or queued "
                "from DAG %s is >= to the DAG's max_active_tasks limit of %s",
                task_instance,
                dag_id,
                dag_max_active_tasks,
            )
            starved_dags.add(dag_id)
            return False

        return True

    def _check_not_serialized_dag(self, task_instance: TaskInstance, session: Session):
        dag_id = task_instance.dag_id
        # Many dags don't have a task_concurrency, so where we can avoid loading the full
        # If the dag is missing, fail the task and continue to the next task.
        serialized_dag = (
            select(TaskInstance, DagVersion, SerializedDagModel)
            .where(TaskInstance.id == task_instance.id)
            .join(DagVersion, DagVersion.id == task_instance.dag_version_id)
            .join(SerializedDagModel, SerializedDagModel.dag_version_id == DagVersion.id)
        )
        serialized_dag = session.execute(serialized_dag).all()
        if not serialized_dag:
            self.log.error(
                "DAG '%s' for task instance %s not found in serialized_dag table",
                dag_id,
                task_instance,
            )
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.dag_id == dag_id, TaskInstance.state == TaskInstanceState.SCHEDULED)
                .values(state=TaskInstanceState.FAILED)
                .execution_options(synchronize_session="fetch")
            )
            return False

        return True

    def _check_task_dag_concurency(
        self,
        task_instance: TaskInstance,
        starved_tasks: set,
        concurrency_map: ConcurrencyMap,
    ) -> bool:
        task_concurrency_limit: int | None = task_instance.max_active_tis_per_dag

        if task_concurrency_limit is None:
            return True

        current_task_concurrency = concurrency_map.task_concurrency_map[
            (task_instance.dag_id, task_instance.task_id)
        ]

        if current_task_concurrency >= task_concurrency_limit:
            self.log.info(
                "Not executing %s since the task concurrency for this task has been reached.",
                task_instance,
            )
            starved_tasks.add((task_instance.dag_id, task_instance.task_id))
            return False

        return True

    def _check_task_dagrun_concurency(
        self,
        task_instance: TaskInstance,
        starved_tasks_task_dagrun_concurrency: set,
        concurrency_map: ConcurrencyMap,
    ) -> bool:
        task_dagrun_concurrency_limit: int | None = task_instance.max_active_tis_per_dagrun
        if task_dagrun_concurrency_limit is None:
            return True

        current_task_dagrun_concurrency = concurrency_map.task_dagrun_concurrency_map[
            (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
        ]

        if current_task_dagrun_concurrency >= task_dagrun_concurrency_limit:
            self.log.info(
                "Not executing %s since the task concurrency per DAG run for this task has been reached.",
                task_instance,
            )
            starved_tasks_task_dagrun_concurrency.add(
                (
                    task_instance.dag_id,
                    task_instance.run_id,
                    task_instance.task_id,
                )
            )
            return False

        return True
