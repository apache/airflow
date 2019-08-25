# -*- coding: utf-8 -*-
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

"""This module defines dep for DagRun ID validation"""

from re import match

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.db import provide_session


class DagrunIdDep(BaseTIDep):
    """
    Dep for valid DagRun ID to schedule from scheduler
    """
    NAME = "Dagrun run_id is not backfill job ID"
    IGNOREABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context=None):
        """
        Determines if the DagRun ID is valid for scheduling from scheduler.
        :param ti: the task instance to get the dependency status for
        :type ti: airflow.models.TaskInstance
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dep_context: the context for which this dependency should be evaluated for
        :type dep_context: DepContext
        :return: True if DagRun ID is valid for scheduling from scheduler.
        """
        from airflow.jobs import BackfillJob  # To avoid a circular dependency
        dagrun = ti.get_dagrun(session)

        if not dagrun.run_id or not match(BackfillJob.ID_PREFIX + '.*', dagrun.run_id):
            yield self._passing_status(
                reason="Task's DagRun run_id is either NULL "
                       "or doesn't start with {}".format(BackfillJob.ID_PREFIX))
        else:
            yield self._failing_status(
                reason="Task's DagRun run_id is not NULL "
                       "and starts with {}".format(BackfillJob.ID_PREFIX))
