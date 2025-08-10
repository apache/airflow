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

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from airflow.models import TaskInstance
from airflow.ti_deps.dependencies_states import EXECUTION_STATES


class ConcurrencyMap:
    """
    Dataclass to represent concurrency maps.

    It contains a map from (dag_id, task_id) to # of task instances, a map from (dag_id, task_id)
    to # of task instances in the given state list and a map from (dag_id, run_id, task_id)
    to # of task instances in the given state list in each DAG run.
    """

    def __init__(self):
        self.dag_run_active_tasks_map: Counter[tuple[str, str]] = Counter()
        self.task_concurrency_map: Counter[tuple[str, str]] = Counter()
        self.task_dagrun_concurrency_map: Counter[tuple[str, str, str]] = Counter()

    def load(self, session: Session) -> None:
        self.dag_run_active_tasks_map.clear()
        self.task_concurrency_map.clear()
        self.task_dagrun_concurrency_map.clear()
        query = session.execute(
            select(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.run_id, func.count("*"))
            .where(TaskInstance.state.in_(EXECUTION_STATES))
            .group_by(TaskInstance.task_id, TaskInstance.run_id, TaskInstance.dag_id)
        )
        for dag_id, task_id, run_id, c in query:
            self.dag_run_active_tasks_map[dag_id, run_id] += c
            self.task_concurrency_map[(dag_id, task_id)] += c
            self.task_dagrun_concurrency_map[(dag_id, run_id, task_id)] += c
