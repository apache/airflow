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
"""DAG parameters overflow validator."""
from __future__ import annotations

import functools
from typing import TYPE_CHECKING

from airflow import settings
from airflow.exceptions import AirflowDagTaskOutOfBoundsValue, AirflowException

if TYPE_CHECKING:
    from airflow.models.dag import DAG


_POSTGRES_PRIORITY_WEIGHT_UPPER_BOUND = 2147483647
_POSTGRES_PRIORITY_WEIGHT_LOWER_BOUND = -2147483648


@functools.lru_cache(maxsize=None)
def _is_metadatabase_postgres() -> bool:
    if settings.engine is None:
        raise AirflowException("Must initialize ORM first")
    return settings.engine.url.get_backend_name() == "postgresql"


def check_values_overflow(dag: DAG) -> None:
    """Validate priority weight values overflow."""
    if _is_metadatabase_postgres():
        task_dict = dag.task_dict

        for dag_task_id in dag.task_dict.keys():
            task = task_dict[dag_task_id]
            if (task.priority_weight_total > _POSTGRES_PRIORITY_WEIGHT_UPPER_BOUND) or (
                task.priority_weight_total < _POSTGRES_PRIORITY_WEIGHT_LOWER_BOUND
            ):
                msg = (
                    f"DAG: {dag.dag_id}. Faulty task: {task} with total priority"
                    f"weight: {task.priority_weight_total} exceeds max/min db value"
                )
                raise AirflowDagTaskOutOfBoundsValue(msg)
    return None
