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


class BackfillContext(RunContext):
    """
    TODODAN main part of docstring (not just different with run context but where this is used

    The main difference between backfilling and a regular task run's dependencies is
    that a backfill ignores a task instance's previous state (e.g. if the task
     succeeded already)
    """
    def __init__(
            self,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False):
        super(BackfillContext, self).__init__(
            flag_upstream_failed=True,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=True)
