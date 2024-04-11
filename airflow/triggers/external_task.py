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

import asyncio
import typing
from typing import Any

from asgiref.sync import sync_to_async
from deprecated import deprecated
from sqlalchemy import func

from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models import DagRun, TaskInstance
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.sensor_helper import _get_count
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import utcnow

if typing.TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

    from airflow.utils.state import DagRunState


class WorkflowTrigger(BaseTrigger):
    """
    A trigger to monitor tasks, task group and dag execution in Apache Airflow.

    :param external_dag_id: The ID of the external DAG.
    :param execution_dates: A list of execution dates for the external DAG.
    :param external_task_ids: A collection of external task IDs to wait for.
    :param external_task_group_id: The ID of the external task group to wait for.
    :param failed_states: States considered as failed for external tasks.
    :param skipped_states: States considered as skipped for external tasks.
    :param allowed_states: States considered as successful for external tasks.
    :param poke_interval: The interval (in seconds) for poking the external tasks.
    :param soft_fail: If True, the trigger will not fail the entire DAG on external task failure.
    """

    def __init__(
        self,
        external_dag_id: str,
        execution_dates: list,
        external_task_ids: typing.Collection[str] | None = None,
        external_task_group_id: str | None = None,
        failed_states: typing.Iterable[str] | None = None,
        skipped_states: typing.Iterable[str] | None = None,
        allowed_states: typing.Iterable[str] | None = None,
        poke_interval: float = 2.0,
        soft_fail: bool = False,
        **kwargs,
    ):
        self.external_dag_id = external_dag_id
        self.external_task_ids = external_task_ids
        self.external_task_group_id = external_task_group_id
        self.failed_states = failed_states
        self.skipped_states = skipped_states
        self.allowed_states = allowed_states
        self.execution_dates = execution_dates
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        super().__init__(**kwargs)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger param and module path."""
        return (
            "airflow.triggers.external_task.WorkflowTrigger",
            {
                "external_dag_id": self.external_dag_id,
                "external_task_ids": self.external_task_ids,
                "external_task_group_id": self.external_task_group_id,
                "failed_states": self.failed_states,
                "skipped_states": self.skipped_states,
                "allowed_states": self.allowed_states,
                "execution_dates": self.execution_dates,
                "poke_interval": self.poke_interval,
                "soft_fail": self.soft_fail,
            },
        )

    async def run(self) -> typing.AsyncIterator[TriggerEvent]:
        """Check periodically tasks, task group or dag status."""
        while True:
            if self.failed_states:
                failed_count = await self._get_count(self.failed_states)
                if failed_count > 0:
                    yield TriggerEvent({"status": "failed"})
                    return
                else:
                    yield TriggerEvent({"status": "success"})
                    return
            if self.skipped_states:
                skipped_count = await self._get_count(self.skipped_states)
                if skipped_count > 0:
                    yield TriggerEvent({"status": "skipped"})
                    return
            allowed_count = await self._get_count(self.allowed_states)
            if allowed_count == len(self.execution_dates):
                yield TriggerEvent({"status": "success"})
                return
            self.log.info("Sleeping for %s seconds", self.poke_interval)
            await asyncio.sleep(self.poke_interval)

    @sync_to_async
    def _get_count(self, states: typing.Iterable[str] | None) -> int:
        """
        Get the count of records against dttm filter and states. Async wrapper for _get_count.

        :param states: task or dag states
        :return The count of records.
        """
        return _get_count(
            dttm_filter=self.execution_dates,
            external_task_ids=self.external_task_ids,
            external_task_group_id=self.external_task_group_id,
            external_dag_id=self.external_dag_id,
            states=states,
        )


@deprecated(
    reason="TaskStateTrigger has been deprecated and will be removed in future.",
    category=RemovedInAirflow3Warning,
)
class TaskStateTrigger(BaseTrigger):
    """
    Waits asynchronously for a task in a different DAG to complete for a specific logical date.

    :param dag_id: The dag_id that contains the task you want to wait for
    :param task_id: The task_id that contains the task you want to
        wait for.
    :param states: allowed states, default is ``['success']``
    :param execution_dates: task execution time interval
    :param poll_interval: The time interval in seconds to check the state.
        The default value is 5 sec.
    :param trigger_start_time: time in Datetime format when the trigger was started. Is used
        to control the execution of trigger to prevent infinite loop in case if specified name
        of the dag does not exist in database. It will wait period of time equals _timeout_sec parameter
        from the time, when the trigger was started and if the execution lasts more time than expected,
        the trigger will terminate with 'timeout' status.
    """

    def __init__(
        self,
        dag_id: str,
        execution_dates: list[datetime],
        trigger_start_time: datetime,
        states: list[str] | None = None,
        task_id: str | None = None,
        poll_interval: float = 2.0,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.task_id = task_id
        self.states = states
        self.execution_dates = execution_dates
        self.poll_interval = poll_interval
        self.trigger_start_time = trigger_start_time
        self.states = states or [TaskInstanceState.SUCCESS.value]
        self._timeout_sec = 60

    def serialize(self) -> tuple[str, dict[str, typing.Any]]:
        """Serialize TaskStateTrigger arguments and classpath."""
        return (
            "airflow.triggers.external_task.TaskStateTrigger",
            {
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
                "trigger_start_time": self.trigger_start_time,
            },
        )

    async def run(self) -> typing.AsyncIterator[TriggerEvent]:
        """
        Check periodically in the database to see if the dag exists and is in the running state.

        If found, wait until the task specified will reach one of the expected states.
        If dag with specified name was not in the running state after _timeout_sec seconds
        after starting execution process of the trigger, terminate with status 'timeout'.
        """
        try:
            while True:
                delta = utcnow() - self.trigger_start_time
                if delta.total_seconds() < self._timeout_sec:
                    # mypy confuses typing here
                    if await self.count_running_dags() == 0:  # type: ignore[call-arg]
                        self.log.info("Waiting for DAG to start execution...")
                        await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "timeout"})
                    return
                # mypy confuses typing here
                if await self.count_tasks() == len(self.execution_dates):  # type: ignore[call-arg]
                    yield TriggerEvent({"status": "success"})
                    return
                self.log.info("Task is still running, sleeping for %s seconds...", self.poll_interval)
                await asyncio.sleep(self.poll_interval)
        except Exception:
            yield TriggerEvent({"status": "failed"})

    @sync_to_async
    @provide_session
    def count_running_dags(self, session: Session):
        """Count how many dag instances in running state in the database."""
        dags = (
            session.query(func.count("*"))
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.execution_date.in_(self.execution_dates),
                TaskInstance.state.in_(["running", "success"]),
            )
            .scalar()
        )
        return dags

    @sync_to_async
    @provide_session
    def count_tasks(self, *, session: Session = NEW_SESSION) -> int | None:
        """Count how many task instances in the database match our criteria."""
        count = (
            session.query(func.count("*"))  # .count() is inefficient
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.state.in_(self.states),
                TaskInstance.execution_date.in_(self.execution_dates),
            )
            .scalar()
        )
        return typing.cast(int, count)


class DagStateTrigger(BaseTrigger):
    """
    Waits asynchronously for a DAG to complete for a specific logical date.

    :param dag_id: The dag_id that contains the task you want to wait for
    :param states: allowed states, default is ``['success']``
    :param execution_dates: The logical date at which DAG run.
    :param poll_interval: The time interval in seconds to check the state.
        The default value is 5.0 sec.
    """

    def __init__(
        self,
        dag_id: str,
        states: list[DagRunState],
        execution_dates: list[datetime],
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.states = states
        self.execution_dates = execution_dates
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, typing.Any]]:
        """Serialize DagStateTrigger arguments and classpath."""
        return (
            "airflow.triggers.external_task.DagStateTrigger",
            {
                "dag_id": self.dag_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> typing.AsyncIterator[TriggerEvent]:
        """Check periodically if the dag run exists, and has hit one of the states yet, or not."""
        while True:
            # mypy confuses typing here
            num_dags = await self.count_dags()  # type: ignore[call-arg]
            if num_dags == len(self.execution_dates):
                yield TriggerEvent(self.serialize())
                return
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    @provide_session
    def count_dags(self, *, session: Session = NEW_SESSION) -> int | None:
        """Count how many dag runs in the database match our criteria."""
        count = (
            session.query(func.count("*"))  # .count() is inefficient
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.state.in_(self.states),
                DagRun.execution_date.in_(self.execution_dates),
            )
            .scalar()
        )
        return typing.cast(int, count)
