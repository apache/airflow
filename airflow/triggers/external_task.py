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
import os
import typing
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag

from asgiref.sync import sync_to_async
from sqlalchemy import func

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import DagRun, TaskInstance
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.file import correct_maybe_zipped
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState
from airflow.utils.sqlalchemy import tuple_in_condition
from airflow.utils.timezone import utcnow

if typing.TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Query, Session

    from airflow.utils.state import DagRunState

    from logging import Logger


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
        execution_dates: list[datetime],
        trigger_start_time: datetime,
        external_task_group_id: str | None,
        external_task_ids: typing.Collection[str] | None,
        external_dag_id: str | None,
        allowed_states: typing.Iterable[str] | None = None,
        skipped_states: typing.Iterable[str] | None = None,
        failed_states: typing.Iterable[str] | None = None,
        task_id: str | None = None,
        poll_interval: float = 2.0,
    ):
        super().__init__()
        self.task_id = task_id
        self.execution_dates = execution_dates
        self.external_task_group_id = external_task_group_id
        self.external_task_ids = external_task_ids
        self.external_dag_id = external_dag_id
        self.allowed_states = allowed_states
        self.skipped_states = skipped_states
        self.failed_states = failed_states
        self.poll_interval = poll_interval
        self.trigger_start_time = trigger_start_time
        self._timeout_sec = 60

    def serialize(self) -> tuple[str, dict[str, typing.Any]]:
        """Serialize TaskStateTrigger arguments and classpath."""
        return (
            "airflow.triggers.external_task.TaskStateTrigger",
            {
                "task_id": self.task_id,
                "execution_dates": self.execution_dates,
                "external_task_group_id": self.external_task_group_id,
                "external_task_ids": self.external_task_ids,
                "external_dag_id": self.external_dag_id,
                "allowed_states": self.allowed_states,
                "skipped_states": self.skipped_states,
                "failed_states": self.failed_states,
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
            while self.get_count(self.external_task_group_id, self.external_task_ids, self.external_dag_id,
                               self.execution_dates, ["running", "success"]) == 0:
                self.log.info("Waiting for DAG to start execution...")

                delta = utcnow() - self.trigger_start_time
                if delta.total_seconds() > self._timeout_sec:
                    yield TriggerEvent({"status": "timeout"})
                    return
                
                await asyncio.sleep(self.poll_interval)

            while True:
                if self.check_external_dag(self.execution_dates, self.external_task_group_id, self.external_task_ids,
                                                self.external_dag_id, self.skipped_states, self.allowed_states,
                                                self.failed_states, False, self.log):
                    yield TriggerEvent({"status": "success"})
                    return

                await asyncio.sleep(self.poll_interval)
        except Exception:
            yield TriggerEvent({"status": "failed"})

    @staticmethod
    @provide_session
    def check_external_dag(dttm_filter: typing.List[datetime.datetime], external_task_group_id: str | None, external_task_ids: typing.Collection[str] | None,
                           external_dag_id: str | None, skipped_states: typing.Iterable[str] | None, allowed_states: typing.Iterable[str] | None,
                           failed_states: typing.Iterable[str] | None, soft_fail: bool, log: Logger, session: Session) -> bool:
        # delay check to poke rather than __init__ in case it was supplied as XComArgs
        if external_task_ids and len(external_task_ids) > len(set(external_task_ids)):
            raise ValueError("Duplicate task_ids passed in external_task_ids parameter")

        serialized_dttm_filter = ",".join(dt.isoformat() for dt in dttm_filter)

        if external_task_ids:
            log.info(
                "Poking for tasks %s in dag %s on %s ... ",
                external_task_ids,
                external_dag_id,
                serialized_dttm_filter,
            )

        if external_task_group_id:
            log.info(
                "Poking for task_group '%s' in dag '%s' on %s ... ",
                external_task_group_id,
                external_dag_id,
                serialized_dttm_filter,
            )

        if external_dag_id and not external_task_group_id and not external_task_ids:
            log.info(
                "Poking for DAG '%s' on %s ... ",
                external_dag_id,
                serialized_dttm_filter,
            )

        # TODO: Decide if we should avoid checking this every time
        TaskStateTrigger._check_for_existence(external_task_group_id=external_task_group_id,
                                              external_task_ids=external_task_ids,
                                              external_dag_id=external_dag_id,
                                              session=session)

        count_failed = -1
        if failed_states:
            count_failed = TaskStateTrigger.get_count(external_task_group_id, external_task_ids, external_dag_id,
                                                      dttm_filter, failed_states, session)

        # Fail if anything in the list has failed.
        if count_failed > 0:
            if external_task_ids:
                if soft_fail:
                    raise AirflowSkipException(
                        f"Some of the external tasks {external_task_ids} "
                        f"in DAG {external_dag_id} failed. Skipping due to soft_fail."
                    )
                raise AirflowException(
                    f"Some of the external tasks {external_task_ids} "
                    f"in DAG {external_dag_id} failed."
                )
            elif external_task_group_id:
                if soft_fail:
                    raise AirflowSkipException(
                        f"The external task_group '{external_task_group_id}' "
                        f"in DAG '{external_dag_id}' failed. Skipping due to soft_fail."
                    )
                raise AirflowException(
                    f"The external task_group '{external_task_group_id}' "
                    f"in DAG '{external_dag_id}' failed."
                )

            else:
                if soft_fail:
                    raise AirflowSkipException(
                        f"The external DAG {external_dag_id} failed. Skipping due to soft_fail."
                    )
                raise AirflowException(f"The external DAG {external_dag_id} failed.")

        count_skipped = -1
        if skipped_states:
            count_skipped = TaskStateTrigger.get_count(external_task_group_id, external_task_ids, external_dag_id,
                                                       dttm_filter, skipped_states, session)

        # Skip if anything in the list has skipped. Note if we are checking multiple tasks and one skips
        # before another errors, we'll skip first.
        if count_skipped > 0:
            if external_task_ids:
                raise AirflowSkipException(
                    f"Some of the external tasks {external_task_ids} "
                    f"in DAG {external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            elif external_task_group_id:
                raise AirflowSkipException(
                    f"The external task_group '{external_task_group_id}' "
                    f"in DAG {external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            else:
                raise AirflowSkipException(
                    f"The external DAG {external_dag_id} reached a state in our states-to-skip-on list. "
                    "Skipping."
                )

        # only go green if every single task has reached an allowed state
        count_allowed = TaskStateTrigger.get_count(external_task_group_id, external_task_ids, external_dag_id,
                                                   dttm_filter, allowed_states, session)
        log.info('COUNT ALLOWED ' + str(count_allowed))
        log.info('external_task_group_id %s external_task_ids %s external_dag_id %s, dtm_filter %s allowed_states %s',
                 str(external_task_group_id), str(external_task_ids), str(external_dag_id), str(dttm_filter),
                 str(allowed_states))
        return count_allowed == len(dttm_filter)
    

    @staticmethod
    @provide_session
    def get_count(external_task_group_id: str | None, external_task_ids: typing.Collection[str] | None,
                  external_dag_id: str | None, dttm_filter, states: typing.Iterable[TaskInstanceState], session: Session) -> int:
        """
        Get the count of records against dttm filter and states.

        :param dttm_filter: date time filter for execution date
        :param session: airflow session object
        :param states: task or dag states
        :return: count of record against the filters
        """
        TI = TaskInstance
        DR = DagRun
        if not dttm_filter:
            return 0

        if external_task_ids:
            count = (
                TaskStateTrigger._count_query(TI, session, states, dttm_filter, external_dag_id)
                .filter(TI.task_id.in_(external_task_ids))
                .scalar()
            ) / len(external_task_ids)
        elif external_task_group_id:
            external_task_group_task_ids = TaskStateTrigger.get_external_task_group_task_ids(session, dttm_filter,
                                                                                             external_dag_id, external_task_group_id)
            if not external_task_group_task_ids:
                count = 0
            else:
                count = (
                    TaskStateTrigger._count_query(TI, session, states, dttm_filter)
                    .filter(tuple_in_condition((TI.task_id, TI.map_index), external_task_group_task_ids))
                    .scalar()
                ) / len(external_task_group_task_ids)
        else:
            count = TaskStateTrigger._count_query(DR, session, states, dttm_filter).scalar()
        return count

    @staticmethod
    def _count_query(model, session, states: typing.Iterable[TaskInstanceState], dttm_filter, external_dag_id: str | None) -> Query:
        query = session.query(func.count()).filter(
            model.dag_id == external_dag_id,
            model.state.in_(states),
            model.execution_date.in_(dttm_filter),
        )
        return query

    @staticmethod
    def get_external_task_group_task_ids(session, dttm_filter, external_dag_id: str | None, external_task_group_id: str | None):
        refreshed_dag_info = DagBag(read_dags_from_db=True).get_dag(external_dag_id, session)
        task_group = refreshed_dag_info.task_group_dict.get(external_task_group_id)

        if task_group:
            group_tasks = session.query(TaskInstance).filter(
                TaskInstance.dag_id == external_dag_id,
                TaskInstance.task_id.in_(task.task_id for task in task_group),
                TaskInstance.execution_date.in_(dttm_filter),
            )

            return [(t.task_id, t.map_index) for t in group_tasks]

        # returning default task_id as group_id itself, this will avoid any failure in case of
        # 'check_existence=False' and will fail on timeout
        return [(external_task_group_id, -1)]


    @staticmethod
    def _check_for_existence(external_task_group_id: str | None, external_task_ids: typing.Collection[str] | None,
                           external_dag_id: str | None, session: Session) -> None:
        dag_to_wait = DagModel.get_current(external_dag_id, session)

        if not dag_to_wait:
            raise AirflowException(f"The external DAG {external_dag_id} does not exist.")

        if not os.path.exists(correct_maybe_zipped(dag_to_wait.fileloc)):
            raise AirflowException(f"The external DAG {external_dag_id} was deleted.")

        if external_task_ids:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(external_dag_id)
            for external_task_id in external_task_ids:
                if not refreshed_dag_info.has_task(external_task_id):
                    raise AirflowException(
                        f"The external task {external_task_id} in "
                        f"DAG {external_dag_id} does not exist."
                    )

        if external_task_group_id:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(external_dag_id)
            if not refreshed_dag_info.has_task_group(external_task_group_id):
                raise AirflowException(
                    f"The external task group '{external_task_group_id}' in "
                    f"DAG '{external_dag_id}' does not exist."
                )


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
