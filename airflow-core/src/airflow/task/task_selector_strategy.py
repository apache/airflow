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

from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol

from airflow.models.taskinstance import TaskInstance
from airflow.utils.sqlalchemy import with_row_locks

if TYPE_CHECKING:
    from sqlalchemy.orm import Query, Session


class TaskSelectorStrategy(Protocol):
    """
    Query tasks to be examined for scheduling.

    This protocol is used to query the tasks that need to be examined
    for setting them to running, splits the scheduler logic a little to be more
    flexible and for it to be changed in a simpler manner.
    """

    @abstractmethod
    def query_tasks_with_locks(
        self,
        session: Session,
        **additional_params,
    ) -> list[TaskInstance]:
        """
        Get the tasks ready for execution, that need to be scheduled.

        Expects getting a priority_order to know how to priorotize the TI's correctly.
        """
