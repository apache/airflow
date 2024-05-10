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

import warnings
from typing import TYPE_CHECKING, Iterable, Sequence

from sqlalchemy import select, update

from airflow.exceptions import AirflowException, RemovedInAirflow3Warning
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import tuple_in_condition
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from pendulum import DateTime
    from sqlalchemy import Session

    from airflow.models.dagrun import DagRun
    from airflow.models.operator import Operator
    from airflow.models.taskmixin import DAGNode
    from airflow.serialization.pydantic.dag_run import DagRunPydantic
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic

# The key used by SkipMixin to store XCom data.
XCOM_SKIPMIXIN_KEY = "skipmixin_key"

# The dictionary key used to denote task IDs that are skipped
XCOM_SKIPMIXIN_SKIPPED = "skipped"

# The dictionary key used to denote task IDs that are followed
XCOM_SKIPMIXIN_FOLLOWED = "followed"


def _ensure_tasks(nodes: Iterable[DAGNode]) -> Sequence[Operator]:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.mappedoperator import MappedOperator

    return [n for n in nodes if isinstance(n, (BaseOperator, MappedOperator))]


class SkipMixin(LoggingMixin):
    """A Mixin to skip Tasks Instances."""

    def _set_state_to_skipped(
        self,
        dag_run: DagRun | DagRunPydantic,
        tasks: Sequence[str] | Sequence[tuple[str, int]],
        session: Session,
    ) -> None:
        """Set state of task instances to skipped from the same dag run."""
        if tasks:
            now = timezone.utcnow()

            if isinstance(tasks[0], tuple):
                session.execute(
                    update(TaskInstance)
                    .where(
                        TaskInstance.dag_id == dag_run.dag_id,
                        TaskInstance.run_id == dag_run.run_id,
                        tuple_in_condition((TaskInstance.task_id, TaskInstance.map_index), tasks),
                    )
                    .values(state=TaskInstanceState.SKIPPED, start_date=now, end_date=now)
                    .execution_options(synchronize_session=False)
                )
            else:
                session.execute(
                    update(TaskInstance)
                    .where(
                        TaskInstance.dag_id == dag_run.dag_id,
                        TaskInstance.run_id == dag_run.run_id,
                        TaskInstance.task_id.in_(tasks),
                    )
                    .values(state=TaskInstanceState.SKIPPED, start_date=now, end_date=now)
                    .execution_options(synchronize_session=False)
                )

    @provide_session
    def skip(
        self,
        dag_run: DagRun | DagRunPydantic,
        execution_date: DateTime,
        tasks: Iterable[DAGNode],
        session: Session = NEW_SESSION,
        map_index: int = -1,
    ):
        """
        Set tasks instances to skipped from the same dag run.

        If this instance has a `task_id` attribute, store the list of skipped task IDs to XCom
        so that NotPreviouslySkippedDep knows these tasks should be skipped when they
        are cleared.

        :param dag_run: the DagRun for which to set the tasks to skipped
        :param execution_date: execution_date
        :param tasks: tasks to skip (not task_ids)
        :param session: db session to use
        :param map_index: map_index of the current task instance
        """
        task_list = _ensure_tasks(tasks)
        if not task_list:
            return

        if execution_date and not dag_run:
            from airflow.models.dagrun import DagRun

            warnings.warn(
                "Passing an execution_date to `skip()` is deprecated in favour of passing a dag_run",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )

            dag_run = session.scalars(
                select(DagRun).where(
                    DagRun.dag_id == task_list[0].dag_id, DagRun.execution_date == execution_date
                )
            ).one()

        elif execution_date and dag_run and execution_date != dag_run.execution_date:
            raise ValueError(
                "execution_date has a different value to  dag_run.execution_date -- please only pass dag_run"
            )

        if dag_run is None:
            raise ValueError("dag_run is required")

        task_ids_list = [d.task_id for d in task_list]
        self._set_state_to_skipped(dag_run, task_ids_list, session)
        session.commit()

        # SkipMixin may not necessarily have a task_id attribute. Only store to XCom if one is available.
        task_id: str | None = getattr(self, "task_id", None)
        if task_id is not None:
            from airflow.models.xcom import XCom

            XCom.set(
                key=XCOM_SKIPMIXIN_KEY,
                value={XCOM_SKIPMIXIN_SKIPPED: task_ids_list},
                task_id=task_id,
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                map_index=map_index,
                session=session,
            )

    def skip_all_except(
        self,
        ti: TaskInstance | TaskInstancePydantic,
        branch_task_ids: None | str | Iterable[str],
    ):
        """
        Implement the logic for a branching operator.

        Given a single task ID or list of task IDs to follow, this skips all other tasks
        immediately downstream of this operator.

        branch_task_ids is stored to XCom so that NotPreviouslySkippedDep knows skipped tasks or
        newly added tasks should be skipped when they are cleared.
        """
        self.log.info("Following branch %s", branch_task_ids)
        if isinstance(branch_task_ids, str):
            branch_task_id_set = {branch_task_ids}
        elif isinstance(branch_task_ids, Iterable):
            branch_task_id_set = set(branch_task_ids)
            invalid_task_ids_type = {
                (bti, type(bti).__name__) for bti in branch_task_ids if not isinstance(bti, str)
            }
            if invalid_task_ids_type:
                raise AirflowException(
                    f"'branch_task_ids' expected all task IDs are strings. "
                    f"Invalid tasks found: {invalid_task_ids_type}."
                )
        elif branch_task_ids is None:
            branch_task_id_set = set()
        else:
            raise AirflowException(
                "'branch_task_ids' must be either None, a task ID, or an Iterable of IDs, "
                f"but got {type(branch_task_ids).__name__!r}."
            )

        dag_run = ti.get_dagrun()
        if TYPE_CHECKING:
            assert isinstance(dag_run, DagRun)
            assert ti.task

        # TODO(potiuk): Handle TaskInstancePydantic case differently - we need to figure out the way to
        # pass task that has been set in LocalTaskJob but in the way that TaskInstancePydantic definition
        # does not attempt to serialize the field from/to ORM
        task = ti.task
        dag = task.dag
        if TYPE_CHECKING:
            assert dag

        valid_task_ids = set(dag.task_ids)
        invalid_task_ids = branch_task_id_set - valid_task_ids
        if invalid_task_ids:
            raise AirflowException(
                "'branch_task_ids' must contain only valid task_ids. "
                f"Invalid tasks found: {invalid_task_ids}."
            )

        downstream_tasks = _ensure_tasks(task.downstream_list)

        if downstream_tasks:
            # For a branching workflow that looks like this, when "branch" does skip_all_except("task1"),
            # we intuitively expect both "task1" and "join" to execute even though strictly speaking,
            # "join" is also immediately downstream of "branch" and should have been skipped. Therefore,
            # we need a special case here for such empty branches: Check downstream tasks of branch_task_ids.
            # In case the task to skip is also downstream of branch_task_ids, we add it to branch_task_ids and
            # exclude it from skipping.
            #
            # branch  ----->  join
            #   \            ^
            #     v        /
            #       task1
            #
            for branch_task_id in list(branch_task_id_set):
                branch_task_id_set.update(dag.get_task(branch_task_id).get_flat_relative_ids(upstream=False))

            skip_tasks = [
                (t.task_id, downstream_ti.map_index)
                for t in downstream_tasks
                if (downstream_ti := dag_run.get_task_instance(t.task_id, map_index=ti.map_index))
                and t.task_id not in branch_task_id_set
            ]

            follow_task_ids = [t.task_id for t in downstream_tasks if t.task_id in branch_task_id_set]
            self.log.info("Skipping tasks %s", skip_tasks)
            with create_session() as session:
                self._set_state_to_skipped(dag_run, skip_tasks, session=session)
                # For some reason, session.commit() needs to happen before xcom_push.
                # Otherwise the session is not committed.
                session.commit()
                ti.xcom_push(key=XCOM_SKIPMIXIN_KEY, value={XCOM_SKIPMIXIN_FOLLOWED: follow_task_ids})
