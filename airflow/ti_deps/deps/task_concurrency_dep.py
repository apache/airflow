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

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.task_group import MappedTaskGroup


class TaskConcurrencyDep(BaseTIDep):
    """This restricts the number of running task instances for a particular task."""

    NAME = "Task Concurrency"
    IGNORABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        if (
            ti.task.max_active_tis_per_dag is None
            and ti.task.max_active_tis_per_dagrun is None
            and ti.task.task_group is not MappedTaskGroup
        ):
            yield self._passing_status(reason="Task concurrency is not set.")
            return

        # active task limit per dag
        if (
            ti.task.max_active_tis_per_dag is not None
            and ti.get_num_running_task_instances(session) >= ti.task.max_active_tis_per_dag
        ):
            yield self._failing_status(reason="The max task concurrency has been reached.")
            return

        # active task limit per dag run
        if (
            ti.task.max_active_tis_per_dagrun is not None
            and ti.get_num_running_task_instances(session, same_dagrun=True)
            >= ti.task.max_active_tis_per_dagrun
        ):
            yield self._failing_status(reason="The max task concurrency per run has been reached.")
            return

        # active task group limit per dag run
        if (
            ti.task.task_group is MappedTaskGroup
            and ti.task.task_group.concurrency_limit is not None
        ):
            limit = ti.task.task_group.concurrency_limit
            valid_idx = ti.get_valid_map_index(session, limit)
            accept = (ti.map_index in valid_idx) or (len(valid_idx) < limit)

            if not accept:
                yield self._failing_status(reason="The max task group concurrency has been reached.")
                return

        yield self._passing_status(reason="The max task concurrency has not been reached.")
        return
