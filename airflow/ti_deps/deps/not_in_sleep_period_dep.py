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
from datetime import datetime

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.db import provide_session
from airflow.utils.state import State


class NotInSleepPeriodDep(BaseTIDep):
    NAME = "Not In Sleep Period"
    IGNOREABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        if dep_context.ignore_in_sleep_period:
            yield self._passing_status(
                reason="The context specified that being in a sleep period was "
                       "permitted.")
            return

        if ti.state != State.SLEEP:
            yield self._passing_status(
                reason="The task instance was not sleeping.")
            return

        # Calculate the date first so that it is always smaller than the timestamp used by
        # ready_for_wake_up
        cur_date = datetime.now()
        next_task_wake_up_date = ti.next_wake_up_datetime()
        if ti.is_premature:
            yield self._failing_status(
                reason="Task is not ready to wake up yet but will be waked up "
                       "automatically. Current date is {0} and task will be waked up "
                       "at {1}.".format(cur_date.isoformat(),
                                        next_task_wake_up_date.isoformat()))
