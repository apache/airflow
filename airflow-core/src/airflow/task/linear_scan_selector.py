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

from typing import TYPE_CHECKING

from sqlalchemy import exists, text, update
from sqlalchemy.orm import selectinload

from airflow.models import TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.task.in_code_selector_helpers import (
    BASE_ORDERED_TI_QUERY,
    get_pool_stats,
    set_tis_failed_for_dag,
)
from airflow.task.task_selector_strategy import TaskSelectorStrategy
from airflow.utils.concurrency import ConcurrencyMap
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Query, Session

TI = TaskInstance


def _get_tis_for_ids(session: Session, task_ids: list[str]) -> list[TI]:
    return (
        session.query(TI)
        .join(TI.dag_run)
        .filter(TI.id.in_(task_ids))
        .options(selectinload(TI.dag_model))
        .all()
    )


def _get_ti_ids_from_query_with_limit(session: Session, query: Query, max_tis: int) -> list:
    return session.execute(statement=query, params={"max_tis": max_tis}).scalars().all()


def _get_tis_from_query_with_limit(session: Session, query: Query, max_tis: int) -> list[TI]:
    ti_ids = _get_ti_ids_from_query_with_limit(session, query, max_tis)
    return _get_tis_for_ids(
        session, [str(ti_id) for ti_id in ti_ids]
    )  # SQLAlchemy needs string for filtering


def _set_tis_failed_for_missing_dags(session: Session) -> None:
    """Bulk set all scheduled task instances for DAGs that are missing to failed."""
    query = (
        update(TI)
        .where(
            TI.state == TaskInstanceState.SCHEDULED,
            ~exists().where(SerializedDagModel.dag_version_id == TI.dag_version_id),
        )
        .values(state=TaskInstanceState.FAILED)
        .execution_options(synchronize_session="fetch")
    )
    session.execute(query)


def _select_tasks_with_locks_pgsql(session: Session, **additional_params) -> list[TI]:
    _set_tis_failed_for_missing_dags(session)
    query = text("SELECT * FROM select_scheduled_tis_to_queue(:max_tis)")
    return _get_tis_from_query_with_limit(session, query, additional_params["max_tis"])


def _select_tasks_with_locks_mysql(session: Session, **additional_params) -> list[TI]:
    _set_tis_failed_for_missing_dags(session)
    query = text("CALL select_scheduled_tis_to_queue(:max_tis)")
    return _get_tis_from_query_with_limit(session, query, additional_params["max_tis"])


def _select_tasks_with_locks_sqlite(session: Session, **additional_params) -> list[TI]:
    tis_to_queue: list[TI] = []

    dag_bag = additional_params["dag_bag"]

    concurrency_map = ConcurrencyMap()
    concurrency_map.load(session=session)
    pools, pool_slots_free = get_pool_stats(session)

    query = with_row_locks(BASE_ORDERED_TI_QUERY, of=TaskInstance, session=session, skip_locked=True)
    task_instances_to_examine = session.scalars(query).all()

    for task_instance in task_instances_to_examine:
        dag_id = task_instance.dag_id

        if not dag_bag.get_dag_for_run(dag_run=task_instance.dag_run, session=session):
            set_tis_failed_for_dag(session, dag_id)
            continue

        dag_run_key = (dag_id, task_instance.run_id)
        task_key = (dag_id, task_instance.task_id)
        task_run_key = (dag_id, task_instance.run_id, task_instance.task_id)
        pool_name = task_instance.pool
        pool_stats = pools.get(pool_name)
        if not pool_stats:
            continue

        open_slots = pool_stats["open"]

        current_active_tis_per_dagrun = concurrency_map.dag_run_active_tasks_map[dag_run_key]
        current_active_tis_per_dag = concurrency_map.task_concurrency_map[task_key]
        current_active_tis_per_dagrun_task = concurrency_map.task_dagrun_concurrency_map[task_run_key]

        dag_max_active_tasks = task_instance.dag_model.max_active_tasks
        max_active_tis_per_dag = task_instance.max_active_tis_per_dag
        max_active_tis_per_dagrun = task_instance.max_active_tis_per_dagrun

        if not pool_stats or task_instance.pool_slots > open_slots:
            continue

        if current_active_tis_per_dagrun >= dag_max_active_tasks:
            continue

        if max_active_tis_per_dag and current_active_tis_per_dag >= max_active_tis_per_dag:
            continue

        if max_active_tis_per_dagrun and current_active_tis_per_dagrun_task >= max_active_tis_per_dagrun:
            continue

        tis_to_queue.append(task_instance)

        open_slots -= task_instance.pool_slots
        pool_stats["open"] = open_slots

        concurrency_map.dag_run_active_tasks_map[dag_run_key] += 1
        concurrency_map.task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1
        concurrency_map.task_dagrun_concurrency_map[
            (task_instance.dag_id, task_instance.run_id, task_instance.task_id)
        ] += 1

        if len(tis_to_queue) >= additional_params["max_tis"]:
            break

    return tis_to_queue


class LinearScanSelector(TaskSelectorStrategy):
    """
    Simple task selector that scans the task instance table linearly after it is sorted by priority fields.

    The strategy returns exactly `max_tis` task instances if available. Otherwise, it returns all available task instances.
    """

    _SELECTOR_BY_DB_VENDOR: dict[str, Callable] = {
        "postgresql": _select_tasks_with_locks_pgsql,
        "mysql": _select_tasks_with_locks_mysql,
        "sqlite": _select_tasks_with_locks_sqlite,
    }

    def query_tasks_with_locks(self, session: Session, **additional_params) -> list[TI]:
        selector: Callable = self._SELECTOR_BY_DB_VENDOR[session.get_bind().dialect.name]
        return selector(session, **additional_params)
