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

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import State


class TaskConcurrencyDep(BaseTIDep):
    """This restricts the number of running task instances for a particular task."""

    NAME = "Task Concurrency"
    IGNORABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session: Session, dep_context):
        task = ti.task
        task_group = task.task_group

        if (
            task.max_active_tis_per_dag is None
            and task.max_active_tis_per_dagrun is None
            and (task_group is None or task_group.max_active_groups_per_dagrun is None)
        ):
            yield self._passing_status(reason="Task concurrency is not set.")
            return

        if (
            task.max_active_tis_per_dag is not None
            and ti.get_num_running_task_instances(session) >= task.max_active_tis_per_dag
        ):
            yield self._failing_status(reason="The max task concurrency per DAG has been reached.")
            return

        if (
            task.max_active_tis_per_dagrun is not None
            and ti.get_num_running_task_instances(session, same_dagrun=True) >= task.max_active_tis_per_dagrun
        ):
            yield self._failing_status(reason="The max task concurrency per run has been reached.")
            return

        if task_group is not None and (group_limit := task_group.max_active_groups_per_dagrun) is not None:
            unfinished_indexes_query = session.scalars(
                select(TaskInstance.map_index)
                .where(
                    TaskInstance.dag_id == ti.dag_id,
                    TaskInstance.run_id == ti.run_id,
                    TaskInstance.task_id.in_(task_group.children),
                    # Special NULL treatment is needed because 'state' can be NULL.
                    # The "IN" part would produce "NULL NOT IN ..." and eventually
                    # "NULl = NULL", which is a big no-no in SQL.
                    or_(
                        TaskInstance.state.is_(None),
                        TaskInstance.state.in_(s.value for s in State.unfinished if s is not None),
                    ),
                )
                .order_by(TaskInstance.map_index)
                .limit(group_limit)
            )
            valid_indexes = set(unfinished_indexes_query)

            if len(valid_indexes) >= group_limit and ti.map_index not in valid_indexes:
                yield self._failing_status(reason="The max task group concurrency has been reached.")
                return

        yield self._passing_status(reason="The max task concurrency has not been reached.")
        return
