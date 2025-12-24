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

from collections.abc import Iterator
from typing import TYPE_CHECKING, TypeAlias

from sqlalchemy import select

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.mappedoperator import MappedOperator
    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.deps.base_ti_dep import TIDepStatus

    Operator: TypeAlias = MappedOperator | SerializedBaseOperator


class MappedTaskUpstreamDep(BaseTIDep):
    """
    Determines if the task, if mapped, is allowed to run based on its mapped dependencies.

    In particular, check if upstream tasks that provide XComs used by this task for task mapping are in
    states that allow the task instance to run.
    """

    NAME = "Mapped dependencies have succeeded"
    IGNORABLE = True
    IS_TASK_DEP = True

    def _get_dep_statuses(
        self,
        ti: TaskInstance,
        session: Session,
        dep_context: DepContext,
    ) -> Iterator[TIDepStatus]:
        from airflow.models.mappedoperator import is_mapped
        from airflow.models.taskinstance import TaskInstance

        if ti.task is None:
            return
        elif is_mapped(ti.task):
            mapped_dependencies = ti.task.iter_mapped_dependencies()
        elif (task_group := ti.task.get_closest_mapped_task_group()) is not None:
            mapped_dependencies = task_group.iter_mapped_dependencies()
        else:
            return

        # Get the tis of all mapped dependencies. In case a mapped dependency is itself mapped, we are
        # only interested in it if it hasn't been expanded yet, i.e., we filter by map_index=-1. This is
        # because if it has been expanded, it did not fail and was not skipped outright which is all we need
        # to know for the purposes of this check.
        mapped_dependency_tis = (
            session.scalars(
                select(TaskInstance).where(
                    TaskInstance.task_id.in_(operator.task_id for operator in mapped_dependencies),
                    TaskInstance.dag_id == ti.dag_id,
                    TaskInstance.run_id == ti.run_id,
                    TaskInstance.map_index == -1,
                )
            ).all()
            if mapped_dependencies
            else []
        )
        if not mapped_dependency_tis:
            yield self._passing_status(reason="There are no (unexpanded) mapped dependencies!")
            return

        finished_states = {ti.state for ti in mapped_dependency_tis if ti.state in State.finished}
        if not finished_states:
            return
        if finished_states == {TaskInstanceState.SUCCESS}:
            # Mapped dependencies are at least partially done and only feature successes
            return

        # At least one mapped dependency was not successful
        if ti.state not in {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}:
            # If another dependency (such as the trigger rule dependency) has not already marked the task as
            # FAILED or UPSTREAM_FAILED then we update the state
            new_state = None
            if (
                TaskInstanceState.FAILED in finished_states
                or TaskInstanceState.UPSTREAM_FAILED in finished_states
            ):
                new_state = TaskInstanceState.UPSTREAM_FAILED
            elif TaskInstanceState.SKIPPED in finished_states:
                new_state = TaskInstanceState.SKIPPED
            if new_state is not None and ti.set_state(new_state, session):
                dep_context.have_changed_ti_states = True
        yield self._failing_status(reason="At least one of task's mapped dependencies has not succeeded!")
