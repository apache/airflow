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

from asgiref.sync import sync_to_async
from sqlalchemy import func

from airflow.models import DagRun, TaskInstance
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import utcnow

if typing.TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

    from airflow.utils.state import DagRunState


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
