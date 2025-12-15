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
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import TaskInstanceState

hookspec = HookspecMarker("airflow")


@hookspec
def on_task_instance_running(previous_state: TaskInstanceState | None, task_instance: RuntimeTaskInstance):
    """Execute when task state changes to RUNNING. previous_state can be None."""


@hookspec
def on_task_instance_success(
    previous_state: TaskInstanceState | None, task_instance: RuntimeTaskInstance | TaskInstance
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
def on_task_instance_skipped(
    previous_state: TaskInstanceState | None,
    task_instance: RuntimeTaskInstance | TaskInstance,
):
    """
    Execute when a task instance skips itself by raising AirflowSkipException.

    This hook is called only when a task has started execution and then
    intentionally skips itself by raising AirflowSkipException.

    Note: This does NOT cover tasks skipped by:
        - Trigger rules (e.g., upstream failures)
        - BranchPythonOperator (tasks not in selected branch)
        - ShortCircuitOperator
        - Scheduler-level decisions

    For comprehensive skip tracking, use DAG-level listeners
    (on_dag_run_success/on_dag_run_failed) which provide complete task state.

    :param previous_state: Previous state of the task instance (can be None)
    :param task_instance: The task instance object (RuntimeTaskInstance when called
        from task execution context, TaskInstance when called from API server)
    """
