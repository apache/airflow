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
"""Ensure tasks wait for TaskGroup retry delay before scheduling."""

from __future__ import annotations

from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.taskgroupinstance import TaskGroupInstance, iter_retryable_task_group_ids
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session


class TaskGroupRetryDep(BaseTIDep):
    """Blocks tasks that belong to a TaskGroup awaiting retry delay."""

    NAME = "Task Group Retry Delay"
    IGNORABLE = False

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other)

    def __hash__(self):
        return hash(type(self))

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context=None):
        if dep_context and getattr(dep_context, "ignore_task_group_retry_delay", False):
            yield self._passing_status(reason="TaskGroup retry delay ignored.")
            return

        task = getattr(ti, "task", None)
        if not task:
            yield self._passing_status(reason="Task is not attached yet.")
            return

        task_group = getattr(task, "task_group", None)
        if not task_group:
            yield self._passing_status(reason="Task is not part of a TaskGroup.")
            return

        group_ids = list(iter_retryable_task_group_ids(task_group))

        if not group_ids:
            yield self._passing_status(reason="No TaskGroup retry delay applies.")
            return

        now = timezone.utcnow()
        pending = session.scalar(
            select(TaskGroupInstance)
            .where(
                TaskGroupInstance.dag_id == ti.dag_id,
                TaskGroupInstance.run_id == ti.run_id,
                TaskGroupInstance.task_group_id.in_(group_ids),
                TaskGroupInstance.next_retry_at.is_not(None),
                TaskGroupInstance.next_retry_at > now,
            )
            .limit(1)
        )
        if pending:
            yield self._failing_status(
                reason=(
                    "Waiting for TaskGroup retry delay "
                    f"({pending.task_group_id}) until {pending.next_retry_at}."
                )
            )
        else:
            yield self._passing_status(reason="TaskGroup retry delay satisfied.")
