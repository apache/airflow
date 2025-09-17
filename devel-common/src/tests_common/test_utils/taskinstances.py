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

from airflow.sdk.definitions.dag import _run_task
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.execution_time.supervisor import TaskRunResult
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


def run_ti(ti: TaskInstance, task: Operator) -> TaskRunResult | None:
    if ti.state in State.finished:
        return None
    if not _check_task_dependencies(ti):
        return None
    if not (result := _run_task(ti=ti, task=task)):
        return None
    ti.state = TaskInstanceState(result.state)
    if result.error:
        raise result.error
    return result
