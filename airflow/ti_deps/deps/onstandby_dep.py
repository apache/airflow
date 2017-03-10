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


class OnStandByDep(BaseTIDep):
    """
    Determines if a task's upstream tasks are in a state that allows a given task instance
    to run.
    """
    NAME = "On Standby"
    IGNOREABLE = True
    IS_TASK_DEP = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        if dep_context.ignore_ti_state:
            yield self._passing_status(
                reason="Context specified that state should be ignored.")
            return

        if ti.state is not State.ON_STANDBY:
            yield self._passing_status(
                reason="The TaskInstance is not ON_STANDBY"
            )
            return

        cur_date = datetime.now()
        timeout = ti.end_date + ti.on_standby_timeout
        if cur_date < timeout:
            yield self._failing_status(
                reason="Task in on stand by and will expire on {}".format(timeout.isoformat())
            )
            return

        # if we get here the external trigger has not fired
        ti.set_state(state=State.FAILED, session=session)

        yield self._failing_status(
            reason="External trigger time window expired"
        )
        return
