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

from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import BaseOperator, Context
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.sdk.types import Operator


def _are_dependencies_met(ti: TaskInstance, dep_context: DepContext, *, session: Session) -> bool:
    if ti.task is None:
        raise ValueError("must supply ti.task")
    for dep in dep_context.deps | ti.task.deps:
        for dep_status in dep.get_dep_statuses(ti, session, dep_context):
            if not dep_status.passed:
                return False
    return True


@provide_session
def _check_task_dependencies(ti: TaskInstance, *, session: Session = NEW_SESSION) -> bool:
    """Minimally re-implement dependency-checking for tests."""
    # Firstly find non-runnable and non-requeueable tis.
    # Since mark_success is not set, we do nothing.
    non_requeueable_dep_context = DepContext(
        deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
        description="non-requeueable deps",
    )
    if not _are_dependencies_met(ti, non_requeueable_dep_context, session=session):
        return False

    # Secondly we find non-runnable but requeueable tis. We reset its state.
    # This is because we might have hit concurrency limits,
    # e.g. because of backfilling.
    dep_context = DepContext(
        deps=REQUEUEABLE_DEPS,
        description="requeueable deps",
    )
    if not _are_dependencies_met(ti, dep_context, session=session):
        return False

    return True


def run_ti(ti: TaskInstance, task: Operator) -> None:
    if not AIRFLOW_V_3_2_PLUS:
        ti.refresh_from_task(task)  # type: ignore[arg-type]
        ti.run()
        return

    from airflow.sdk.definitions.dag import _run_task

    if ti.state in State.finished:
        return
    if not _check_task_dependencies(ti):
        return
    if not (result := _run_task(ti=ti, task=task)):
        return
    ti.state = TaskInstanceState(result.state)
    if result.error:
        raise result.error


def _as_runtime_task_instance(ti: TaskInstance, task: Operator) -> RuntimeTaskInstance:
    from airflow.sdk.api.datamodels._generated import TIRunContext
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    return RuntimeTaskInstance.model_construct(
        id=ti.id,
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        try_number=ti.try_number,
        map_index=ti.map_index,
        dag_version_id=ti.dag_version_id,
        task=task,
        # This is not exactly right (ti.dag_run is an ORM object, but
        # TIRunContext expects a data model), but let's not fuss about it...
        _ti_context_from_server=TIRunContext.model_construct(dag_run=ti.dag_run),
    )


def get_template_context(ti: TaskInstance, task: Operator) -> Context:
    if not AIRFLOW_V_3_2_PLUS:
        ti.refresh_from_task(task)  # type: ignore[arg-type]
        return ti.get_template_context()
    return _as_runtime_task_instance(ti, task).get_template_context()


def render_templates(ti: TaskInstance, task: Operator) -> BaseOperator:
    """
    Render templates for given ti.

    :return: The fully rendered task object. If *task* is a mapped operator,
        the returned object would be its unmapped value. If *task* is not
        mapped, the same object is returned.
    """
    if not AIRFLOW_V_3_2_PLUS:
        ti.refresh_from_task(task)  # type: ignore[arg-type]
        ti.render_templates()
        return ti.task  # type: ignore[return-value]
    rti = _as_runtime_task_instance(ti, task)
    rti.render_templates()
    return rti.task
