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
from airflow.ti_deps.contexts.run_context import RunContext
from airflow.ti_deps.deps.dagrun_exists_dep import DagrunExistsDep


# TODODAN docstrings and comments - basically the requirements for a task to get scheduled with the scheduler job and then run on a worker
# ideally this context would also be used by the scheduler job (right now there is no coupling so it can diverge)
# SchedulerEndToEndContext extends [Dagrun context + RunContext] (has DagRunExitsDep, TODO that scheduler context isn't actually to use this like running a task instance is coupled with the runcontext)
class SchedulerEndToEndContext(RunContext):
    """
    Context to get the dependencies that need to be met for a given task instance to be
    able to get run by an executor. This class just extends QueueContext by adding
    dependencies for resources.
    """

    def get_ignoreable_deps(self, ti):
        """
        TODODAN header

        :param ti: The task instance for which to get the dependencies for in the given
            context.
        :type ti: TaskInstance
        """
        return super(RunContext, self).get_ignoreable_deps(ti) | {
            DagrunExistsDep(),
        }
