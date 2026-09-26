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

from __future__ import annotations

from typing import TYPE_CHECKING

from pluggy import HookspecMarker

if TYPE_CHECKING:
    # These imports are for type checking only - no runtime dependency
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import TaskInstanceState

hookspec = HookspecMarker("airflow")


@hookspec
def on_task_instance_running(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
):
    """Execute when task state changes to RUNNING. previous_state can be None."""


@hookspec
def on_task_instance_success(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
):
    """Execute when task state changes to SUCCESS. previous_state can be None."""


@hookspec
def on_task_instance_failed(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
    error: None | str | BaseException,
):
    """Execute when task state changes to FAIL. previous_state can be None."""


@hookspec
def on_task_instance_up_for_retry(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
    error: None | str | BaseException,
):
    """
    Execute when a task instance is set to UP_FOR_RETRY after a failure.

    This hook fires instead of (in addition to, for backward compatibility)
    ``on_task_instance_failed`` when the task instance is still eligible for an
    automatic retry, so listeners that only care about a task's *terminal*
    failure can subscribe here instead of filtering ``on_task_instance_failed``
    by ``task_instance.state``. ``on_task_instance_failed`` continues to fire
    for both the retry and terminal-failure cases, unchanged, to avoid breaking
    existing listeners.

    :param previous_state: Previous state of the task instance (can be None)
    :param task_instance: The task instance object (RuntimeTaskInstance when called
        from task execution context, TaskInstance when called from API server)
    :param error: The error that caused the retry, if any
    """


@hookspec
def on_task_instance_skipped(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
):
    """
    Execute when a task instance skips itself during execution.

    This hook is called only when a task has started execution and then
    intentionally skips itself (e.g., by raising AirflowSkipException).

    Note: This function will NOT cover tasks that were skipped by scheduler, before execution began, such as:
        - Skips due to trigger rules (e.g., upstream failures)
        - Skips from operators like BranchPythonOperator, ShortCircuitOperator, or similar mechanisms
        - Any other situation in which the scheduler decides not to schedule a task for execution

    For comprehensive tracking of skipped tasks, use DAG-level listeners
    (on_dag_run_success/on_dag_run_failed) which may have access to all task states.

    :param previous_state: Previous state of the task instance (can be None)
    :param task_instance: The task instance object (RuntimeTaskInstance when called
        from task execution context, TaskInstance when called from API server)
    """
