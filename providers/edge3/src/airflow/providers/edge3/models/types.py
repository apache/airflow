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

from typing import TYPE_CHECKING, TypeAlias, TypeGuard

from airflow.executors.workloads import ExecuteTask
from airflow.providers.edge3.version_compat import AIRFLOW_V_3_3_PLUS

if TYPE_CHECKING:
    from airflow.executors import workloads
    from airflow.executors.workloads import ExecuteCallback

if not AIRFLOW_V_3_3_PLUS:
    ExecuteTypeBody: TypeAlias = ExecuteTask
else:
    from airflow.executors.workloads import ExecutorWorkload

    ExecuteTypeBody: TypeAlias = ExecutorWorkload  # type: ignore[no-redef,misc]


def is_callback_execute(workload: workloads.All) -> TypeGuard[ExecuteCallback]:
    if AIRFLOW_V_3_3_PLUS:
        from airflow.executors.workloads import ExecuteCallback

        return isinstance(workload, ExecuteCallback)
    return False


# This is the key used to identify execute_callback jobs.
# Changing this value may break compatibility with existing data in the edge_job table.
EXECUTE_CALLBACK_TAG = "ExecuteCallback"

# The rest of the identity queue_workload() writes for a callback row. "ExecuteCallback" is a valid
# Dag id, so a row is a callback only when all four fields match.
CALLBACK_JOB_TRY_NUMBER = 0
CALLBACK_JOB_MAP_INDEX = -1


def build_callback_run_id(callback_id: str) -> str:
    return f"{EXECUTE_CALLBACK_TAG}-{callback_id}"


def is_callback_job(dag_id: str, task_id: str, run_id: str, try_number: int, map_index: int) -> bool:
    """Return whether a job row matches the identity ``queue_workload()`` writes for a callback."""
    return (
        dag_id == EXECUTE_CALLBACK_TAG
        and run_id == build_callback_run_id(task_id)
        and try_number == CALLBACK_JOB_TRY_NUMBER
        and map_index == CALLBACK_JOB_MAP_INDEX
    )
