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

from sqlalchemy import text
from sqlalchemy.orm import selectinload

from airflow.models import TaskInstance
from airflow.task.task_selector_strategy import TaskSelectorStrategy

if TYPE_CHECKING:
    from collections.abc import Callable
    from sqlalchemy.orm import Query, Session

TI = TaskInstance


def _select_tasks_with_locks_pgsql(session: Session, **additional_params) -> list[TI]:
    raise NotImplementedError("PostgreSQL implementation is not provided yet.")


def _select_tasks_with_locks_mysql(session: Session, **additional_params) -> list[TI]:
    query = text("CALL select_scheduled_tis_to_queue(:max_tis)")
    task_ids = (
        session.execute(statement=query, params={"max_tis": additional_params["max_tis"]}).scalars().all()
    )
    return (
        session.query(TI)
        .join(TI.dag_run)
        .filter(TI.id.in_(task_ids))
        .options(selectinload(TI.dag_model))
        .all()
    )


def _select_tasks_with_locks_sqlite(session: Session, **additional_params) -> list[TI]:
    raise NotImplementedError("SQLite implementation is not provided yet.")


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
        selector = self._SELECTOR_BY_DB_VENDOR.get(session.get_bind().dialect.name)
        return selector(session, **additional_params)
