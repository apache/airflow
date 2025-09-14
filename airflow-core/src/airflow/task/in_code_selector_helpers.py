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

from sqlalchemy import select, update
from sqlalchemy.orm import Session, selectinload

from airflow.models import DagRun, Pool, TaskInstance
from airflow.models.dag import DagModel
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from airflow.models.pool import PoolStats

TI = TaskInstance
DR = DagRun

_PRIORITY_ORDER = [-TaskInstance.priority_weight, DagRun.logical_date, TaskInstance.map_index]

BASE_ORDERED_TI_QUERY = (
    select(TaskInstance)
    .with_hint(TaskInstance, "USE INDEX (ti_state)", dialect_name="mysql")
    .join(TaskInstance.dag_run)
    .where(DagRun.state == DagRunState.RUNNING)
    .join(TaskInstance.dag_model)
    .where(~DagModel.is_paused)
    .where(TaskInstance.state == TaskInstanceState.SCHEDULED)
    .where(DagModel.bundle_name.is_not(None))
    .options(selectinload(TaskInstance.dag_model))
    .order_by(*_PRIORITY_ORDER)
)


def get_pool_stats(session: Session) -> tuple[dict[str, PoolStats], int]:
    # Get the pool settings. We get a lock on the pool rows, treating this as a "critical section"
    # Throws an exception if lock cannot be obtained, rather than blocking
    pools = Pool.slots_stats(lock_rows=True, session=session)

    # If the pools are full, there is no point doing anything!
    # If _somehow_ the pool is overfull, don't let the limit go negative - it breaks SQL
    pool_slots_free = sum(max(0, pool["open"]) for pool in pools.values())

    return pools, pool_slots_free


def set_tis_failed_for_dag(session: Session, dag_id: str) -> None:
    """
    Set all scheduled task instances for the given DAG to failed.

    Exists for compatibility with old, explicit scheduler.
    """
    session.execute(
        update(TaskInstance)
        .where(TaskInstance.dag_id == dag_id, TaskInstance.state == TaskInstanceState.SCHEDULED)
        .values(state=TaskInstanceState.FAILED)
        .execution_options(synchronize_session="fetch")
    )
