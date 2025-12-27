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

from airflow.models.taskinstance import TaskInstance
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.serialized_objects import create_scheduler_operator

from tests_common.test_utils.dag import create_scheduler_dag

if TYPE_CHECKING:
    from uuid import UUID

    from airflow.sdk.types import Operator as SdkOperator
    from airflow.serialization.definitions.mappedoperator import Operator as SerializedOperator

__all__ = ["create_task_instance", "run_task_instance"]


def create_task_instance(
    task: SdkOperator | SerializedOperator,
    *,
    dag_version_id: UUID,
    run_id: str | None = None,
    state: str | None = None,
    map_index: int = -1,
) -> TaskInstance:
    if isinstance(task, (SerializedBaseOperator, SerializedMappedOperator)):
        serialized_task = task
    elif sdk_dag := task.get_dag():
        serialized_task = create_scheduler_dag(sdk_dag).get_task(task.task_id)
    else:
        serialized_task = create_scheduler_operator(task)
    return TaskInstance(
        serialized_task,
        dag_version_id=dag_version_id,
        run_id=run_id,
        state=state,
        map_index=map_index,
    )


def run_task_instance(
    ti: TaskInstance,
    task: SdkOperator,
    *,
    ignore_depends_on_past: bool = False,
    ignore_ti_state: bool = False,
    mark_success: bool = False,
    session=None,
) -> None:
    kwargs = {"session": session} if session else {}
    if not ti.check_and_change_state_before_execution(
        ignore_depends_on_past=ignore_depends_on_past,
        ignore_ti_state=ignore_ti_state,
        mark_success=mark_success,
        **kwargs,
    ):
        return

    from airflow.sdk.definitions.dag import _run_task

    if (taskrun_result := _run_task(ti=ti, task=task)) and (error := taskrun_result.error):
        raise error from error
    return
