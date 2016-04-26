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
from airflow.ti_deps.deps.dag_unpaused_dep import DagUnpausedDep
from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from airflow.ti_deps.deps.not_running_dep import NotRunningDep
from airflow.ti_deps.deps.not_skipped_dep import NotSkippedDep
from airflow.ti_deps.deps.runnable_state_dep import RunnableStateDep


# TODODAN go over comments and docstrings since this was a copy-and-paste (also in run_context and backfill_run_context and others)
# TODODAN think of a better name
# TODODAN shouldn't __init__ have docstring? other contexts might need it too
class MinimumRunContext(BaseDepContext):
    def __init__(
            self,
            flag_upstream_failed=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False):
        super(MinimumRunContext, self).__init__(
            flag_upstream_failed=flag_upstream_failed,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps)
        self._ignore_ti_state = ignore_ti_state

    def get_ignoreable_deps(self, ti):
        """
        Context to get the dependencies that need to be met for a given task instance to be
        able to get queued for execution once resources become available.

        :param ti: The task instance for which to get the dependencies for in the given
            context.
        :type ti: TaskInstance
        """
        deps = super(MinimumRunContext, self).get_ignoreable_deps(ti) | {
            DagUnpausedDep(),
            RunnableExecDateDep(),
            NotSkippedDep(),
        }

        if not self._ignore_ti_state:
            deps.add(RunnableStateDep())

        return deps

    def get_nonignoreable_deps(self):
        """
        Task instances must not already be running, as running two copies of the same task
        instance at the same time (AKA double-trigger) should be avoided at all costs,
        even if for example the context has the "ignore_all_deps" attribute.
        """
        return super(MinimumRunContext, self).get_nonignoreable_deps() | {
            NotRunningDep()
        }
