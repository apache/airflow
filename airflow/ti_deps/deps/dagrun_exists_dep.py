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

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import State


class DagrunRunningDep(BaseTIDep):
    """Determines whether a task's DagRun is in valid state."""

    NAME = "Dagrun Running"
    IGNORABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context):
        dr = ti.get_dagrun(session)
        if dr.state != State.RUNNING:
            yield self._failing_status(
                reason=f"Task instance's dagrun was not in the 'running' state but in the state '{dr.state}'."
            )
