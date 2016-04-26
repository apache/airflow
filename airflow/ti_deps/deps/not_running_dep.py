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
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.db import provide_session
from airflow.utils.state import State


class NotRunningDep(BaseTIDep):
    NAME = "Task Instance Not Already Running"

    # Task instances must not already be running, as running two copies of the same
    # task instance at the same time (AKA double-trigger) should be avoided at all
    # costs, even if the context specifies that all dependencies should be ignored.
    IGNOREABLE = False

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        if ti.state == State.RUNNING:
            yield self._failing_status(
                reason="Task is already running, it started on {0}.".format(
                    ti.start_date))
