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
import datetime
import typing
from typing import Any

from asgiref.sync import sync_to_async
from sqlalchemy import func
from sqlalchemy.orm import Session

from airflow.models import DagRun, TaskInstance
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import provide_session


class TaskStateTrigger(BaseTrigger):
    """
    Waits asynchronously for a task in a different DAG to complete for a
    specific logical date.

    :param dag_id: The dag_id that contains the task you want to wait for
    :param task_id: The task_id that contains the task you want to
        wait for. If ``None`` (default value) the sensor waits for the DAG
    :param states: allowed states, default is ``['success']``
    :param execution_dates:
    :param poll_interval: The time interval in seconds to check the state.
        The default value is 5 sec.
    """

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        states: list[str],
        execution_dates: list[datetime.datetime],
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.task_id = task_id
        self.states = states
        self.execution_dates = execution_dates
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes TaskStateTrigger arguments and classpath."""
        return (
            "airflow.triggers.external_task.TaskStateTrigger",
            {
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> typing.AsyncIterator["TriggerEvent"]:
        """
        Checks periodically in the database to see if the task exists, and has
        hit one of the states yet, or not.
        """
        while True:
            num_tasks = await self.count_tasks()
            if num_tasks == len(self.execution_dates):
                yield TriggerEvent(True)
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    @provide_session
    def count_tasks(self, session: Session) -> int | None:
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
        states: list[str],
        execution_dates: list[datetime.datetime],
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.states = states
        self.execution_dates = execution_dates
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes DagStateTrigger arguments and classpath."""
        return (
            "airflow.triggers.external_task.DagStateTrigger",
            {
                "dag_id": self.dag_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> typing.AsyncIterator["TriggerEvent"]:
        """
        Checks periodically in the database to see if the dag run exists, and has
        hit one of the states yet, or not.
        """
        while True:
            num_dags = await self.count_dags()
            if num_dags == len(self.execution_dates):
                yield TriggerEvent(True)
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    @provide_session
    def count_dags(self, session: Session) -> int | None:
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
