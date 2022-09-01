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

from typing import TYPE_CHECKING, List, Optional

import attr
from sqlalchemy.orm.session import Session

from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance


@attr.define
class DepContext:
    """
    A base class for contexts that specifies which dependencies should be evaluated in
    the context for a task instance to satisfy the requirements of the context. Also
    stores state related to the context that can be used by dependency classes.

    For example there could be a SomeRunContext that subclasses this class which has
    dependencies for:

    - Making sure there are slots available on the infrastructure to run the task instance
    - A task-instance's task-specific dependencies are met (e.g. the previous task
      instance completed successfully)
    - ...

    :param deps: The context-specific dependencies that need to be evaluated for a
        task instance to run in this execution context.
    :param flag_upstream_failed: This is a hack to generate the upstream_failed state
        creation while checking to see whether the task instance is runnable. It was the
        shortest path to add the feature. This is bad since this class should be pure (no
        side effects).
    :param ignore_all_deps: Whether or not the context should ignore all ignorable
        dependencies. Overrides the other ignore_* parameters
    :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs (e.g. for
        Backfills)
    :param ignore_in_retry_period: Ignore the retry period for task instances
    :param ignore_in_reschedule_period: Ignore the reschedule period for task instances
    :param ignore_unmapped_tasks: Ignore errors about mapped tasks not yet being expanded
    :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past and
        trigger rule
    :param ignore_ti_state: Ignore the task instance's previous failure/success
    :param finished_tis: A list of all the finished task instances of this run
    """

    deps: set = attr.ib(factory=set)
    flag_upstream_failed: bool = False
    ignore_all_deps: bool = False
    ignore_depends_on_past: bool = False
    ignore_in_retry_period: bool = False
    ignore_in_reschedule_period: bool = False
    ignore_task_deps: bool = False
    ignore_ti_state: bool = False
    ignore_unmapped_tasks: bool = False
    finished_tis: Optional[List["TaskInstance"]] = None

    have_changed_ti_states: bool = False
    """Have any of the TIs state's been changed as a result of evaluating dependencies"""

    def ensure_finished_tis(self, dag_run: "DagRun", session: Session) -> List["TaskInstance"]:
        """
        This method makes sure finished_tis is populated if it's currently None.
        This is for the strange feature of running tasks without dag_run.

        :param dag_run: The DagRun for which to find finished tasks
        :return: A list of all the finished tasks of this DAG and execution_date
        :rtype: list[airflow.models.TaskInstance]
        """
        if self.finished_tis is None:
            finished_tis = dag_run.get_task_instances(state=State.finished, session=session)
            self.finished_tis = finished_tis
        else:
            finished_tis = self.finished_tis
        return finished_tis
