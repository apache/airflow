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
from typing import TYPE_CHECKING

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance
    from airflow.ti_deps.dep_context import DepContext
    from airflow.ti_deps.deps.base_ti_dep import TIDepStatus


class MappedTaskUpstreamDep(BaseTIDep):
    """
    Determines if a mapped task's upstream tasks that provide XComs used by this task for task mapping are in
    a state that allows a given task instance to run.
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
        from airflow.models.mappedoperator import MappedOperator

        if isinstance(ti.task, MappedOperator):
            mapped_dependencies = ti.task.iter_mapped_dependencies()
        elif (task_group := ti.task.get_closest_mapped_task_group()) is not None:
            mapped_dependencies = task_group.iter_mapped_dependencies()
        else:
            return

        mapped_dependency_tis = [
            ti.get_dagrun(session).get_task_instance(operator.task_id, session=session)
            for operator in mapped_dependencies
        ]
        if not mapped_dependency_tis:
            yield self._passing_status(reason="There are no mapped dependencies!")
            return
        # ti can be None if the mapped dependency is a mapped operator, and it has already been expanded. In
        # this case, we don't need to check it any further as it didn't fail or was skipped altogether
        finished_tis = [ti for ti in mapped_dependency_tis if ti is not None and ti.state in State.finished]
        if not finished_tis:
            return

        finished_states = {finished_ti.state for finished_ti in finished_tis}
        if finished_states == {TaskInstanceState.SUCCESS}:
            # Mapped dependencies are at least partially done and only feature successes
            return

        # At least one mapped dependency was not successful
        # - If another dependency (such as the trigger rule dependency) has not already marked the task as
        # FAILED or UPSTREAM_FAILED then we update the state
        if ti.state not in {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}:
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
        # - Return a failing status
        yield self._failing_status(reason="At least one of task's mapped dependencies has not succeeded!")
