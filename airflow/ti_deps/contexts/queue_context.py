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
from airflow.ti_deps.deps.not_queued_dep import NotQueuedDep


# TODODAN look over run and backfill and runminusqueue contexts to make sure there isn't wrong comments since now runcontext et al
# TODODAN shouldn't __init__ have docstring? other contexts might need it too
class QueueContext(MinimumRunContext):
    def get_ignoreable_deps(self, ti):
        """
        Context to get the dependencies that need to be met for a given task instance to be
        able to get queued for execution once resources become available.

        :param ti: The task instance for which to get the dependencies for in the given
            context.
        :type ti: TaskInstance
        """
        return super(QueueContext, self).get_ignoreable_deps(ti) | {
            NotQueuedDep(),
        }
