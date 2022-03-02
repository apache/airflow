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

"""This module defines dep for making sure DagRun not a backfill."""

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType


class DagRunNotBackfillDep(BaseTIDep):
    """Dep for valid DagRun run_id to schedule from scheduler"""

    NAME = "DagRun is not backfill job"
    IGNORABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context=None):
        """
        Determines if the DagRun is valid for scheduling from scheduler.

        :param ti: the task instance to get the dependency status for
        :param session: database session
        :param dep_context: the context for which this dependency should be evaluated for
        :return: True if DagRun is valid for scheduling from scheduler.
        """
        dagrun = ti.get_dagrun(session)

        if dagrun.run_type == DagRunType.BACKFILL_JOB:
            yield self._failing_status(
                reason=f"Task's DagRun run_type is {dagrun.run_type} and cannot be run by the scheduler"
            )
