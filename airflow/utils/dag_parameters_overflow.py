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

from typing import TYPE_CHECKING

from airflow.configuration import conf
from airflow.exceptions import AirflowDagTaskOutOfBoundsValue

if TYPE_CHECKING:
    from airflow.models.dag import DAG

_WEIGHT_UPPER_BOUND = 2147483647  # 2 ** 31 -1
_WEIGHT_LOWER_BOUND = -2147483648  # -(2 ** 31)


def check_values_overflow(dag: DAG) -> None:
    """Validate priority weight values overflow."""
    if conf.get_mandatory_value("core", "priority_weight_check_rule") == "ignore":
        return

    errors = []
    for task in dag.task_dict.values():
        if not (_WEIGHT_UPPER_BOUND >= (weight_total := task.priority_weight_total) >= _WEIGHT_LOWER_BOUND):
            errors.append(f"Task {task.task_id!r} has priority weight {weight_total}.")

    if not errors:
        return

    error_msg = (
        f"Tasks in dag {dag.dag_id!r} exceeds allowed priority weight "
        f"[{_WEIGHT_LOWER_BOUND}..{_WEIGHT_UPPER_BOUND}] range: \n * "
    )
    error_msg += "\n * ".join(errors)
    raise AirflowDagTaskOutOfBoundsValue(error_msg)
