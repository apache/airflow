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

from typing import Iterable, Set, Union

from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State

# The key used by SkipMixin to store XCom data.
XCOM_SKIPMIXIN_KEY = "skipmixin_key"

# The dictionary key used to denote task IDs that are skipped
XCOM_SKIPMIXIN_SKIPPED = "skipped"

# The dictionary key used to denote task IDs that are followed
XCOM_SKIPMIXIN_FOLLOWED = "followed"


class SkipMixin(LoggingMixin):
    """A Mixin to skip Tasks Instances"""

    def _set_state_to_skipped(self, dag_run, execution_date, tasks, session):
        """
        Used internally to set state of task instances to skipped from the same dag run.
        """
        task_ids = [d.task_id for d in tasks]
        now = timezone.utcnow()

        if dag_run:
            session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.execution_date == dag_run.execution_date,
                TaskInstance.task_id.in_(task_ids),
            ).update(
                {
                    TaskInstance.state: State.SKIPPED,
                    TaskInstance.start_date: now,
                    TaskInstance.end_date: now,
                },
                synchronize_session=False,
            )
        else:
            if execution_date is None:
                raise ValueError("Execution date is None and no dag run")

            self.log.warning("No DAG RUN present this should not happen")
            # this is defensive against dag runs that are not complete
            for task in tasks:
                ti = TaskInstance(task, execution_date=execution_date)
                ti.state = State.SKIPPED
                ti.start_date = now
                ti.end_date = now
                session.merge(ti)

    @provide_session
    def skip(
        self, dag_run, execution_date, tasks, session=None,
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
        if not tasks:
            return

        self._set_state_to_skipped(dag_run, execution_date, tasks, session)
        session.commit()

        # SkipMixin may not necessarily have a task_id attribute. Only store to XCom if one is available.
        try:
            task_id = self.task_id  # noqa
        except AttributeError:
            task_id = None

        if task_id is not None:
            from airflow.models.xcom import XCom

            XCom.set(
                key=XCOM_SKIPMIXIN_KEY,
                value={XCOM_SKIPMIXIN_SKIPPED: [d.task_id for d in tasks]},
                task_id=task_id,
                dag_id=dag_run.dag_id,
                execution_date=dag_run.execution_date,
                session=session
            )

    def skip_all_except(
        self, ti: TaskInstance, branch_task_ids: Union[str, Iterable[str]]
    ):
        """
        This method implements the logic for a branching operator; given a single
        task ID or list of task IDs to follow, this skips all other tasks
        immediately downstream of this operator.

        branch_task_ids is stored to XCom so that NotPreviouslySkippedDep knows skipped tasks or
        newly added tasks should be skipped when they are cleared.
        """
        self.log.info("Following branch %s", branch_task_ids)
        if isinstance(branch_task_ids, str):
            branch_task_ids = [branch_task_ids]

        dag_run = ti.get_dagrun()
        task = ti.task
        dag = task.dag

        downstream_tasks = task.downstream_list

        if downstream_tasks:
            # Also check downstream tasks of the branch task. In case the task to skip
            # is also a downstream task of the branch task, we exclude it from skipping.
            branch_downstream_task_ids = set()  # type: Set[str]
            for branch_task_id in branch_task_ids:
                branch_downstream_task_ids.update(
                    dag.get_task(branch_task_id).get_flat_relative_ids(upstream=False)
                )

            skip_tasks = [
                t
                for t in downstream_tasks
                if t.task_id not in branch_task_ids
                and t.task_id not in branch_downstream_task_ids
            ]

            self.log.info("Skipping tasks %s", [t.task_id for t in skip_tasks])
            with create_session() as session:
                self._set_state_to_skipped(
                    dag_run, ti.execution_date, skip_tasks, session=session
                )
                ti.xcom_push(
                    key=XCOM_SKIPMIXIN_KEY, value={XCOM_SKIPMIXIN_FOLLOWED: branch_task_ids}
                )
