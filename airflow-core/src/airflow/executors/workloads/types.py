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
"""Type aliases for Workloads."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

from airflow.models.callback import CallbackKey, ExecutorCallback
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils.state import CallbackState, TaskInstanceState

if TYPE_CHECKING:
    from airflow.executors.workloads.callback import ExecuteCallback
    from airflow.executors.workloads.task import ExecuteTask

    # Type aliases for workload keys and states (used by executor layer)
    WorkloadKey: TypeAlias = TaskInstanceKey | CallbackKey
    WorkloadState: TypeAlias = TaskInstanceState | CallbackState

    # Type alias for executor workload results (used by executor implementations)
    WorkloadResultType: TypeAlias = tuple[WorkloadKey, WorkloadState, Exception | None]

    # Workload types that flow through executor queues (have key and sort_key).
    # Update this union when adding a new queueable workload type.
    QueueableWorkload: TypeAlias = ExecuteTask | ExecuteCallback

# Type alias for scheduler workloads (ORM models that can be routed to executors)
# Must be outside TYPE_CHECKING for use in function signatures
SchedulerWorkload: TypeAlias = TaskInstance | ExecutorCallback


def state_class_for_key(key: WorkloadKey) -> type[TaskInstanceState] | type[CallbackState]:
    if isinstance(key, TaskInstanceKey):
        return TaskInstanceState
    if isinstance(key, CallbackKey):
        return CallbackState
    raise TypeError(f"Unknown workload key type: {type(key)!r}")
