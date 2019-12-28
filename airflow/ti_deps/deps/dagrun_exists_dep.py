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

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import State


class DagrunRunningDep(BaseTIDep):
    NAME = "Dagrun Running"
    IGNOREABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        dag = ti.task.dag
        dagrun = ti.get_dagrun(session)
        if not dagrun:
            # The import is needed here to avoid a circular dependency
            from airflow.models import DagRun
            running_dagruns = DagRun.find(
                dag_id=dag.dag_id,
                state=State.RUNNING,
                external_trigger=False,
                session=session
            )

            if len(running_dagruns) >= dag.max_active_runs:
                reason = ("The maximum number of active dag runs ({0}) for this task "
                          "instance's DAG '{1}' has been reached.".format(
                              dag.max_active_runs,
                              ti.dag_id))
            else:
                reason = "Unknown reason"
            yield self._failing_status(
                reason="Task instance's dagrun did not exist: {0}.".format(reason))
        else:
            if dagrun.state != State.RUNNING:
                yield self._failing_status(
                    reason="Task instance's dagrun was not in the 'running' state but in "
                           "the state '{}'.".format(dagrun.state))
