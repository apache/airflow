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

from airflow.ti_deps.dependencies_states import (
    QUEUEABLE_STATES,
    RUNNABLE_STATES,
)
from airflow.ti_deps.deps.dag_ti_slots_available_dep import DagTISlotsAvailableDep
from airflow.ti_deps.deps.dag_unpaused_dep import DagUnpausedDep
from airflow.ti_deps.deps.dagrun_exists_dep import DagrunRunningDep
from airflow.ti_deps.deps.exec_date_after_start_date_dep import ExecDateAfterStartDateDep
from airflow.ti_deps.deps.pool_slots_available_dep import PoolSlotsAvailableDep
from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from airflow.ti_deps.deps.task_concurrency_dep import TaskConcurrencyDep
from airflow.ti_deps.deps.task_not_running_dep import TaskNotRunningDep
from airflow.ti_deps.deps.valid_state_dep import ValidStateDep

# Dependencies that if met, task instance should be re-queued.
REQUEUEABLE_DEPS = {
    DagTISlotsAvailableDep(),
    TaskConcurrencyDep(),
    PoolSlotsAvailableDep(),
}

# Dependencies that need to be met for a given task instance to be set to 'RUNNING' state.
RUNNING_DEPS = {
    RunnableExecDateDep(),
    ValidStateDep(RUNNABLE_STATES),
    DagTISlotsAvailableDep(),
    TaskConcurrencyDep(),
    PoolSlotsAvailableDep(),
    TaskNotRunningDep(),
}

# TODO(aoen): SCHEDULER_QUEUED_DEPS is not coupled to actual scheduling/execution
# in any way and could easily be modified or removed from the scheduler causing
# this dependency to become outdated and incorrect. This coupling should be created
# (e.g. via a dag_deps analog of ti_deps that will be used in the scheduler code,
# or allow batch deps checks) to ensure that the logic here is equivalent to the logic
# in the scheduler.
# Right now there's one discrepancy between this context and how scheduler schedule tasks:
# Scheduler will check if the executor has the task instance--it is not possible
# to check the executor outside scheduler main process.

# Dependencies that need to be met for a given task instance to be set to 'queued' state
# by the scheduler.
# This context has more DEPs than RUNNING_DEPS, as we can have task triggered by
# components other than scheduler, e.g. webserver.
SCHEDULER_QUEUED_DEPS = {
    RunnableExecDateDep(),
    ValidStateDep(QUEUEABLE_STATES),
    DagTISlotsAvailableDep(),
    TaskConcurrencyDep(),
    PoolSlotsAvailableDep(),
    DagrunRunningDep(),
    DagUnpausedDep(),
    ExecDateAfterStartDateDep(),
    TaskNotRunningDep(),
}
