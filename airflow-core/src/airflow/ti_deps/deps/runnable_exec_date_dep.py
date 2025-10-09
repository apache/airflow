#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from airflow._shared.timezones import timezone
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session


class RunnableExecDateDep(BaseTIDep):
    """Determines whether a task's logical date is valid."""

    NAME = "Logical Date"
    IGNORABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        logical_date = ti.get_dagrun(session).logical_date
        if logical_date is None:
            return

        cur_date = timezone.utcnow()

        if logical_date > cur_date:
            yield self._failing_status(
                reason=(
                    f"Logical date {logical_date.isoformat()} is in the future "
                    f"(the current date is {cur_date.isoformat()})."
                )
            )

        if ti.task.end_date and logical_date > ti.task.end_date:
            yield self._failing_status(
                reason=(
                    f"The logical date is {logical_date.isoformat()} but this is "
                    f"after the task's end date {ti.task.end_date.isoformat()}."
                )
            )

        if ti.task.dag and ti.task.dag.end_date and logical_date > ti.task.dag.end_date:
            yield self._failing_status(
                reason=(
                    f"The logical date is {logical_date.isoformat()} but this is after "
                    f"the task's DAG's end date {ti.task.dag.end_date.isoformat()}."
                )
            )
