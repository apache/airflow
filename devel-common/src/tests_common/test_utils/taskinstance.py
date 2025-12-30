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

import copy
from typing import TYPE_CHECKING

from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import NEW_SESSION

from tests_common.test_utils.compat import SerializedBaseOperator, SerializedMappedOperator
from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS

try:
    from airflow.serialization.serialized_objects import create_scheduler_operator
except ImportError:
    create_scheduler_operator = lambda t: t

if TYPE_CHECKING:
    from uuid import UUID

    from jinja2 import Environment
    from sqlalchemy.orm import Session

    from airflow.sdk import Context
    from airflow.sdk.types import Operator as SdkOperator
    from airflow.serialization.definitions.mappedoperator import Operator as SerializedOperator

__all__ = ["TaskInstanceWrapper", "create_task_instance", "render_template_fields", "run_task_instance"]


class TaskInstanceWrapper:
    """Compat wrapper for TaskInstance to support ``run()``."""

    def __init__(self, ti: TaskInstance, task: SdkOperator) -> None:
        self.__dict__.update(__ti=ti, __task=task)

    def __delattr__(self, name):
        delattr(self.__dict__["__ti"], name)

    def __setattr__(self, name, value):
        setattr(self.__dict__["__ti"], name, value)

    def __getattr__(self, name):
        return getattr(self.__dict__["__ti"], name)

    def __copy__(self):
        return TaskInstanceWrapper(copy.copy(self.__dict__["__ti"]), copy.copy(self.__dict__["__task"]))

    def run(self, **kwargs) -> None:
        run_task_instance(self.__dict__["__ti"], self.__dict__["__task"], **kwargs)

    def render_templates(self, **kwargs) -> SdkOperator:
        return render_template_fields(self.__dict__["__ti"], self.__dict__["__task"], **kwargs)

    def get_template_context(self) -> Context:
        return get_template_context(self.__dict__["__ti"], self.__dict__["__task"])


def create_task_instance(
    task: SdkOperator | SerializedOperator,
    *,
    dag_version_id: UUID,
    run_id: str | None = None,
    state: str | None = None,
    map_index: int = -1,
    ti_type: type[TaskInstance] = TaskInstance,
) -> TaskInstance:
    if isinstance(task, (SerializedBaseOperator, SerializedMappedOperator)):
        serialized_task = task
    elif sdk_dag := task.get_dag():
        serialized_task = create_scheduler_dag(sdk_dag).get_task(task.task_id)
    else:
        serialized_task = create_scheduler_operator(task)
    if AIRFLOW_V_3_0_PLUS:
        return ti_type(
            serialized_task,
            dag_version_id=dag_version_id,
            run_id=run_id,
            state=state,
            map_index=map_index,
        )
    return ti_type(  # type: ignore[call-arg]
        serialized_task,
        run_id=run_id,
        state=state,
        map_index=map_index,
    )


def run_task_instance(
    ti: TaskInstance,
    task: SdkOperator,
    *,
    ignore_depends_on_past: bool = False,
    ignore_task_deps: bool = False,
    ignore_ti_state: bool = False,
    mark_success: bool = False,
    session=None,
):
    if not AIRFLOW_V_3_2_PLUS:
        ti.refresh_from_task(task)  # type: ignore[arg-type]
        ti.run()
        return ti

    kwargs = {"session": session} if session else {}
    if not ti.check_and_change_state_before_execution(
        ignore_depends_on_past=ignore_depends_on_past,
        ignore_task_deps=ignore_task_deps,
        ignore_ti_state=ignore_ti_state,
        mark_success=mark_success,
        **kwargs,
    ):
        return ti

    from airflow.sdk.definitions.dag import _run_task

    taskrun_result = _run_task(ti=ti, task=task)
    if not taskrun_result:
        return None
    if error := taskrun_result.error:
        raise error
    return taskrun_result.ti


def get_template_context(ti: TaskInstance, task: SdkOperator, *, session: Session = NEW_SESSION) -> Context:
    if not AIRFLOW_V_3_2_PLUS:
        ti.refresh_from_task(task)  # type: ignore[arg-type]
        return ti.get_template_context(session=session)

    from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun, TaskInstance, TIRunContext
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    runtime_ti = RuntimeTaskInstance.model_construct(
        **TaskInstance.model_validate(ti, from_attributes=True).model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=TIRunContext(
            dag_run=DagRun.model_validate(ti.dag_run, from_attributes=True),
            max_tries=ti.max_tries,
            variables=[],
            connections=[],
            xcom_keys_to_clear=[],
        ),
    )
    # TODO: Move these functions to test_utils too.
    runtime_ti.__dict__["xcom_push"] = ti.xcom_push
    runtime_ti.__dict__["xcom_pull"] = ti.xcom_pull

    return runtime_ti.get_template_context()


def render_template_fields(
    ti: TaskInstance,
    task: SdkOperator,
    *,
    context: Context | None = None,
    jinja_env: Environment | None = None,
) -> SdkOperator:
    if AIRFLOW_V_3_2_PLUS:
        task.render_template_fields(context or get_template_context(ti, task), jinja_env)
        return task
    ti.refresh_from_task(task)  # type: ignore[arg-type]
    ti.render_templates(context, jinja_env)
    return ti.task  # type: ignore[return-value]
