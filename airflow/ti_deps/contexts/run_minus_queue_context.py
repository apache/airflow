# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.ti_deps.contexts.base_dep_context import BaseDepContext
from airflow.ti_deps.contexts.queue_context import QueueContext
from airflow.ti_deps.contexts.run_context import RunContext


class RunMinusQueueContext(BaseDepContext):
    """
    Returns the dependencies of the RunContext that are not included in the QueueContext.
    This class is only needed because the QueueContext has some slow queries at the moment
    and we can't afford to run them twice so for efficiency we use:
    ti.are_dependencies_met(QueueContext()) +
    ti.are_dependencies_met(RunMinusQueueContext())

    instead of

    ti.are_dependencies_met(QueueContext()) +
    ti.are_dependencies_met(RunContext())

    :param ti: The task instance for which to get the dependencies for in the given
        context.
    :type ti: TaskInstance
    """
    def __init__(
            self,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False):
        super(RunMinusQueueContext, self).__init__(
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps)
        self._ignore_ti_state = ignore_ti_state

    def get_ignoreable_deps(self, ti):
        run_context = RunContext(
            ignore_depends_on_past=self.ignore_depends_on_past,
            flag_upstream_failed=self.flag_upstream_failed,
            ignore_all_deps=self._ignore_all_deps)
        run_context_deps = run_context.get_ignoreable_deps(ti)

        queue_context = QueueContext(
            ignore_depends_on_past=self.ignore_depends_on_past,
            flag_upstream_failed=self.flag_upstream_failed,
            ignore_all_deps=self._ignore_all_deps,
            ignore_ti_state=self._ignore_ti_state)
        queue_context_deps = queue_context.get_ignoreable_deps(ti)

        return (run_context_deps - queue_context_deps)
