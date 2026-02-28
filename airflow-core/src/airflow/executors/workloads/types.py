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

from airflow.models.callback import ExecutorCallback
from airflow.models.taskinstance import TaskInstance

if TYPE_CHECKING:
    from airflow.models.callback import CallbackKey
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.state import CallbackState, TaskInstanceState

    # Type aliases for workload keys and states (used by executor layer)
    WorkloadKey: TypeAlias = TaskInstanceKey | CallbackKey
    WorkloadState: TypeAlias = TaskInstanceState | CallbackState

    # Type alias for executor workload results (used by executor implementations)
    WorkloadResultType: TypeAlias = tuple[WorkloadKey, WorkloadState, Exception | None]

# Type alias for scheduler workloads (ORM models that can be routed to executors)
# Must be outside TYPE_CHECKING for use in function signatures
SchedulerWorkload: TypeAlias = TaskInstance | ExecutorCallback
