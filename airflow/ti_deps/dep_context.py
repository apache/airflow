#
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

from airflow.ti_deps.deps.dag_ti_slots_available_dep import DagTISlotsAvailableDep
from airflow.ti_deps.deps.dag_unpaused_dep import DagUnpausedDep
from airflow.ti_deps.deps.dagrun_exists_dep import DagrunRunningDep
from airflow.ti_deps.deps.dagrun_id_dep import DagrunIdDep
from airflow.ti_deps.deps.exec_date_after_start_date_dep import ExecDateAfterStartDateDep
from airflow.ti_deps.deps.pool_slots_available_dep import PoolSlotsAvailableDep
from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from airflow.ti_deps.deps.task_concurrency_dep import TaskConcurrencyDep
from airflow.ti_deps.deps.task_not_running_dep import TaskNotRunningDep
from airflow.ti_deps.deps.valid_state_dep import ValidStateDep
from airflow.utils.state import State


class DepContext:
    """
    A base class for contexts that specifies which dependencies should be evaluated in
    the context for a task instance to satisfy the requirements of the context. Also
    stores state related to the context that can be used by dependency classes.

    For example there could be a SomeRunContext that subclasses this class which has
    dependencies for:

    - Making sure there are slots available on the infrastructure to run the task instance
    - A task-instance's task-specific dependencies are met (e.g. the previous task
      instance completed successfully)
    - ...

    :param deps: The context-specific dependencies that need to be evaluated for a
        task instance to run in this execution context.
    :type deps: set(airflow.ti_deps.deps.base_ti_dep.BaseTIDep)
    :param flag_upstream_failed: This is a hack to generate the upstream_failed state
        creation while checking to see whether the task instance is runnable. It was the
        shortest path to add the feature. This is bad since this class should be pure (no
        side effects).
    :type flag_upstream_failed: bool
    :param ignore_all_deps: Whether or not the context should ignore all ignoreable
        dependencies. Overrides the other ignore_* parameters
    :type ignore_all_deps: bool
    :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
        Backfills)
    :type ignore_depends_on_past: bool
    :param ignore_in_retry_period: Ignore the retry period for task instances
    :type ignore_in_retry_period: bool
    :param ignore_in_reschedule_period: Ignore the reschedule period for task instances
    :type ignore_in_reschedule_period: bool
    :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past and
        trigger rule
    :type ignore_task_deps: bool
    :param ignore_ti_state: Ignore the task instance's previous failure/success
    :type ignore_ti_state: bool
    :param finished_tasks: A list of all the finished tasks of this run
    :type finished_tasks: list[airflow.models.TaskInstance]
    """
    def __init__(
            self,
            deps=None,
            flag_upstream_failed=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_in_retry_period=False,
            ignore_in_reschedule_period=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            finished_tasks=None):
        self.deps = deps or set()
        self.flag_upstream_failed = flag_upstream_failed
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_in_retry_period = ignore_in_retry_period
        self.ignore_in_reschedule_period = ignore_in_reschedule_period
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.finished_tasks = finished_tasks


# In order to be able to get queued a task must have one of these states
SCHEDULEABLE_STATES = {
    State.NONE,
    State.UP_FOR_RETRY,
    State.UP_FOR_RESCHEDULE,
}

RUNNABLE_STATES = {
    # For cases like unit tests and run manually
    State.NONE,
    State.UP_FOR_RETRY,
    State.UP_FOR_RESCHEDULE,
    # For normal scheduler/backfill cases
    State.QUEUED,
}

QUEUEABLE_STATES = {
    State.SCHEDULED,
}

BACKFILL_QUEUEABLE_STATES = {
    # For cases like unit tests and run manually
    State.NONE,
    State.UP_FOR_RESCHEDULE,
    State.UP_FOR_RETRY,
    # For normal backfill cases
    State.SCHEDULED,
}

# Context to get the dependencies that need to be met in order for a task instance to be
# set to 'scheduled' state.
SCHEDULED_DEPS = {
    RunnableExecDateDep(),
    ValidStateDep(SCHEDULEABLE_STATES),
    TaskNotRunningDep(),
}

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

BACKFILL_QUEUED_DEPS = {
    RunnableExecDateDep(),
    ValidStateDep(BACKFILL_QUEUEABLE_STATES),
    DagrunRunningDep(),
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
    DagrunIdDep(),
    DagUnpausedDep(),
    ExecDateAfterStartDateDep(),
    TaskNotRunningDep(),
}
