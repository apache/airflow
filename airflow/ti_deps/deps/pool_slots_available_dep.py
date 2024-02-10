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
"""This module defines dep for pool slots availability."""
from __future__ import annotations

from sqlalchemy import select

from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session


class PoolSlotsAvailableDep(BaseTIDep):
    """Dep for pool slots availability."""

    NAME = "Pool Slots Available"
    IGNORABLE = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context=None):
        """
        Determine if the pool task instance is in has available slots.

        :param ti: the task instance to get the dependency status for
        :param session: database session
        :param dep_context: the context for which this dependency should be evaluated for
        :return: True if there are available slots in the pool.
        """
        from airflow.models.pool import Pool  # To avoid a circular dependency

        pool_name = ti.pool

        # Controlled by UNIQUE key in slot_pool table, only (at most) one result can be returned.
        pool: Pool | None = session.scalar(select(Pool).where(Pool.pool == pool_name))
        if pool is None:
            yield self._failing_status(
                reason=f"Tasks using non-existent pool '{pool_name}' will not be scheduled"
            )
            return

        open_slots = pool.open_slots(session=session)
        if ti.state in pool.get_occupied_states():
            open_slots += ti.pool_slots

        if open_slots <= (ti.pool_slots - 1):
            yield self._failing_status(
                reason=f"Not scheduling since there are {open_slots} open slots in pool {pool_name} "
                f"and require {ti.pool_slots} pool slots"
            )
        else:
            yield self._passing_status(
                reason=f"There are enough open slots in {pool_name} to execute the task",
            )
