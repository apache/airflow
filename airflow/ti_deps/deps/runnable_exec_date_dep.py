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


class RunnableExecDateDep(BaseTIDep):
    NAME = "Execution Date"
    IGNOREABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        cur_date = datetime.utcnow()

        if ti.execution_date > cur_date:
            yield self._failing_status(
                reason="Execution date {0} is in the future (the current "
                       "date is {1}).".format(ti.execution_date.isoformat(),
                                              cur_date.isoformat()))

        if ti.task.end_date and ti.execution_date > ti.task.end_date:
            yield self._failing_status(
                reason="The execution date is {0} but this is after the task's end date "
                "{1}.".format(
                    ti.execution_date.isoformat(),
                    ti.task.end_date.isoformat()))

        if (ti.task.dag and
                ti.task.dag.end_date and
                ti.execution_date > ti.task.dag.end_date):
            yield self._failing_status(
                reason="The execution date is {0} but this is after the task's DAG's "
                "end date {1}.".format(
                    ti.execution_date.isoformat(),
                    ti.task.dag.end_date.isoformat()))
