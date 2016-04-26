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
from airflow.ti_deps.contexts.minimum_run_context import MinimumRunContext
from airflow.ti_deps.deps.dag_ti_slots_available_dep import DagTISlotsAvailableDep
from airflow.ti_deps.deps.pool_has_space_dep import PoolHasSpaceDep


class RunContext(MinimumRunContext):
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
            DagTISlotsAvailableDep(),
            PoolHasSpaceDep(),
        }
