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
"""DAG Slots available dependencies."""
from typing import Any, Generator, Optional

from sqlalchemy.orm import Session

from airflow.ti_deps.dep_context import BaseTIDep, DepContext, TIDepStatus
from airflow.utils.db import provide_session


class DagTISlotsAvailableDep(BaseTIDep):
    """Task instance slots available"""
    NAME = "Task Instance Slots Available"
    IGNOREABLE = True

    @provide_session
    def _get_dep_statuses(self, ti: Any,
                          session: Session,
                          dep_context: Optional[DepContext] = None) -> \
            Generator[TIDepStatus, None, None]:
        if ti.task.dag.concurrency_reached:
            yield self._failing_status(
                reason="The maximum number of running tasks ({0}) for this task's DAG "
                       "'{1}' has been reached.".format(ti.task.dag.concurrency,
                                                        ti.dag_id))
