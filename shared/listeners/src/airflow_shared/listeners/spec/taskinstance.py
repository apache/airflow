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
    msg: str,
):
    """
    Execute when task state changes to RUNNING. previous_state can be None.

    :param previous_state: Previous state of the task instance (can be None)
    :param task_instance: The task instance object
    :param msg: Short canonical context for the state change. Always ``"started"``
        for this hook. Mirrors the DagRun listener pattern so listeners can route
        or filter events without re-deriving intent from other fields.
    """


@hookspec
def on_task_instance_success(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
    msg: str,
):
    """
    Execute when task state changes to SUCCESS. previous_state can be None.

    :param previous_state: Previous state of the task instance (can be None)
    :param task_instance: The task instance object (RuntimeTaskInstance when called
        from task execution context, TaskInstance when called from API server)
    :param msg: Short canonical context for the state change. ``"success"`` when
        emitted from the worker; ``"manually_set_to_success"`` when the state was
        changed via the API.
    """


@hookspec
def on_task_instance_failed(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
    error: None | str | BaseException,
    msg: str,
):
    """
    Execute when task state changes to FAIL. previous_state can be None.

    :param previous_state: Previous state of the task instance (can be None)
    :param task_instance: The task instance object (RuntimeTaskInstance when called
        from task execution context, TaskInstance when called from API server)
    :param error: The exception or error message that caused the failure
    :param msg: Short canonical context distinguishing failure paths without
        inspecting ``error``. ``"failed"`` (terminal), ``"up_for_retry"`` (will
        retry), or ``"manually_set_to_failed"`` (API-driven state change).
    """


@hookspec
def on_task_instance_skipped(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
    msg: str,
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
    :param msg: Short canonical context for the state change. ``"skipped"`` when
        emitted from the worker; ``"manually_set_to_skipped"`` when the state was
        changed via the API.
    """
