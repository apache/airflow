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

from collections import Counter

from sqlalchemy import Column, select, update
from sqlalchemy.orm import Query, Session, selectinload

from airflow.models import DagRun, Pool, TaskInstance
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.pool import PoolStats
from airflow.models.serialized_dag import SerializedDagModel
from airflow.task.task_querier_strategy import TaskQuerierStrategy
from airflow.utils.concurency import ConcurrencyMap
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import DagRunState, TaskInstanceState


class OptimisticTaskQuerierStrategy(TaskQuerierStrategy, LoggingMixin):
    def __init__(self) -> None:
        super().__init__()
        self.num_starving_tasks_total = 0

    def get_query(self, priority_order: list[Column], max_tis: int) -> Query:
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

    def query_tasks_with_locks(self, session: Session, **additional_params) -> Query:
        priority_order: list[Column] = additional_params["priority_order"]
        max_tis: int = additional_params.get("max_tis", 32)

        task_instances_to_examine = super().query_tasks_with_locks(
            session,
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

        for task_instance in task_instances_to_examine:
            pool_name = task_instance.pool

            pool_stats = pools.get(pool_name)
            if not pool_stats:
                self.log.warning("Tasks using non-existent pool '%s' will not be scheduled", pool_name)
                starved_pools.add(pool_name)
                continue

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

            # TODO: extract this to a different function
            if task_instance.dag_model.has_task_concurrency_limits:
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
                        .where(
                            TaskInstance.dag_id == dag_id, TaskInstance.state == TaskInstanceState.SCHEDULED
                        )
                        .values(state=TaskInstanceState.FAILED)
                        .execution_options(synchronize_session="fetch")
                    )
                    continue

                task_concurrency_limit: int | None = task_instance.max_active_tis_per_dag

                if task_concurrency_limit is not None:
                    current_task_concurrency = concurrency_map.task_concurrency_map[
                        (task_instance.dag_id, task_instance.task_id)
                    ]

                    if current_task_concurrency >= task_concurrency_limit:
                        self.log.info(
                            "Not executing %s since the task concurrency for this task has been reached.",
                            task_instance,
                        )
                        starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                        continue

                task_dagrun_concurrency_limit: int | None = task_instance.max_active_tis_per_dagrun

                if task_dagrun_concurrency_limit is not None:
                    current_task_dagrun_concurrency = concurrency_map.task_dagrun_concurrency_map[
                        (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
                    ]

                    if current_task_dagrun_concurrency >= task_dagrun_concurrency_limit:
                        self.log.info(
                            "Not executing %s since the task concurrency per DAG run for"
                            " this task has been reached.",
                            task_instance,
                        )
                        starved_tasks_task_dagrun_concurrency.add(
                            (
                                task_instance.dag_id,
                                task_instance.run_id,
                                task_instance.task_id,
                            )
                        )
                        continue

            # TODO: maybe remove this as it should not be part of the querier
            # as it does add to starved_tasks
            # change to a sql query
            if executor_obj := self._try_to_load_executor(task_instance.executor):
                if TYPE_CHECKING:
                    # All executors should have a name if they are initted from the executor_loader.
                    # But we need to check for None to make mypy happy.
                    assert executor_obj.name
                if executor_slots_available[executor_obj.name] <= 0:
                    self.log.debug(
                        "Not scheduling %s since its executor %s does not currently have any more "
                        "available slots"
                    )
                    starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                    continue
                executor_slots_available[executor_obj.name] -= 1
            else:
                # This is a defensive guard for if we happen to have a task who's executor cannot be
                # found. The check in the dag parser should make this not realistically possible but the
                # loader can fail if some direct DB modification has happened or another as yet unknown
                # edge case. _try_to_load_executor will log an error message explaining the executor
                # cannot be found.
                starved_tasks.add((task_instance.dag_id, task_instance.task_id))
                continue

            executable_tis.append(task_instance)
            open_slots -= task_instance.pool_slots
            concurrency_map.dag_run_active_tasks_map[dag_run_key] += 1
            concurrency_map.task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1
            concurrency_map.task_dagrun_concurrency_map[
                (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
            ] += 1

            pool_stats["open"] = open_slots
