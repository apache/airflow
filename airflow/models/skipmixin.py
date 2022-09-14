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

from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import State

if TYPE_CHECKING:
    from pendulum import DateTime
    from sqlalchemy import Session

    from airflow.models.dagrun import DagRun
    from airflow.models.operator import Operator
    from airflow.models.taskmixin import DAGNode

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
    """A Mixin to skip Tasks Instances"""

    def _set_state_to_skipped(
        self,
        dag_run: DagRun,
        tasks: Iterable[Operator],
        session: Session,
    ) -> None:
        """Used internally to set state of task instances to skipped from the same dag run."""
        now = timezone.utcnow()

        session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.run_id == dag_run.run_id,
            TaskInstance.task_id.in_(d.task_id for d in tasks),
        ).update(
            {
                TaskInstance.state: State.SKIPPED,
                TaskInstance.start_date: now,
                TaskInstance.end_date: now,
            },
            synchronize_session=False,
        )

    @provide_session
    def skip(
        self,
        dag_run: DagRun,
        execution_date: DateTime,
        tasks: Iterable[DAGNode],
        session: Session = NEW_SESSION,
    ):
        """
        Sets tasks instances to skipped from the same dag run.

        If this instance has a `task_id` attribute, store the list of skipped task IDs to XCom
        so that NotPreviouslySkippedDep knows these tasks should be skipped when they
        are cleared.

        :param dag_run: the DagRun for which to set the tasks to skipped
        :param execution_date: execution_date
        :param tasks: tasks to skip (not task_ids)
        :param session: db session to use
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

            dag_run = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == task_list[0].dag_id,
                    DagRun.execution_date == execution_date,
                )
                .one()
            )
        elif execution_date and dag_run and execution_date != dag_run.execution_date:
            raise ValueError(
                "execution_date has a different value to  dag_run.execution_date -- please only pass dag_run"
            )

        if dag_run is None:
            raise ValueError("dag_run is required")

        self._set_state_to_skipped(dag_run, task_list, session)
        session.commit()

        # SkipMixin may not necessarily have a task_id attribute. Only store to XCom if one is available.
        task_id: str | None = getattr(self, "task_id", None)
        if task_id is not None:
            from airflow.models.xcom import XCom

            XCom.set(
                key=XCOM_SKIPMIXIN_KEY,
                value={XCOM_SKIPMIXIN_SKIPPED: [d.task_id for d in task_list]},
                task_id=task_id,
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                session=session,
            )

    def skip_all_except(self, ti: TaskInstance, branch_task_ids: None | str | Iterable[str]):
        """
        This method implements the logic for a branching operator; given a single
        task ID or list of task IDs to follow, this skips all other tasks
        immediately downstream of this operator.

        branch_task_ids is stored to XCom so that NotPreviouslySkippedDep knows skipped tasks or
        newly added tasks should be skipped when they are cleared.
        """
        self.log.info("Following branch %s", branch_task_ids)
        if isinstance(branch_task_ids, str):
            branch_task_id_set = {branch_task_ids}
        elif branch_task_ids is None:
            branch_task_id_set = set()
        else:
            branch_task_id_set = set(branch_task_ids)

        dag_run = ti.get_dagrun()
        task = ti.task
        dag = task.dag
        if TYPE_CHECKING:
            assert dag

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

            skip_tasks = [t for t in downstream_tasks if t.task_id not in branch_task_id_set]
            follow_task_ids = [t.task_id for t in downstream_tasks if t.task_id in branch_task_id_set]

            self.log.info("Skipping tasks %s", [t.task_id for t in skip_tasks])
            with create_session() as session:
                self._set_state_to_skipped(dag_run, skip_tasks, session=session)
                # For some reason, session.commit() needs to happen before xcom_push.
                # Otherwise the session is not committed.
                session.commit()
                ti.xcom_push(key=XCOM_SKIPMIXIN_KEY, value={XCOM_SKIPMIXIN_FOLLOWED: follow_task_ids})
