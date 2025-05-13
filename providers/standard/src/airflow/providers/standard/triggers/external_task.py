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
from sqlalchemy import func

from airflow.models import DagRun
from airflow.providers.standard.utils.sensor_helper import _get_count
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.triggers.base import BaseTrigger, TriggerEvent

if typing.TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session

    from airflow.utils.state import DagRunState


class WorkflowTrigger(BaseTrigger):
    """
    A trigger to monitor tasks, task group and dag execution in Apache Airflow.

    :param external_dag_id: The ID of the external dag.
    :param run_ids: A list of run ids for the external dag.
    :param external_task_ids: A collection of external task IDs to wait for.
    :param external_task_group_id: The ID of the external task group to wait for.
    :param failed_states: States considered as failed for external tasks.
    :param skipped_states: States considered as skipped for external tasks.
    :param allowed_states: States considered as successful for external tasks.
    :param poke_interval: The interval (in seconds) for poking the external tasks.
    :param soft_fail: If True, the trigger will not fail the entire dag on external task failure.
    :param logical_dates: A list of logical dates for the external dag.
    """

    def __init__(
        self,
        external_dag_id: str,
        run_ids: list[str] | None = None,
        execution_dates: list[datetime] | None = None,
        logical_dates: list[datetime] | None = None,
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
        self.run_ids = run_ids
        self.poke_interval = poke_interval
        self.soft_fail = soft_fail
        self.execution_dates = execution_dates
        self.logical_dates = logical_dates
        super().__init__(**kwargs)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger param and module path."""
        data: dict[str, typing.Any] = {
            "external_dag_id": self.external_dag_id,
            "external_task_ids": self.external_task_ids,
            "external_task_group_id": self.external_task_group_id,
            "failed_states": self.failed_states,
            "skipped_states": self.skipped_states,
            "allowed_states": self.allowed_states,
            "poke_interval": self.poke_interval,
            "soft_fail": self.soft_fail,
        }
        if AIRFLOW_V_3_0_PLUS:
            data["run_ids"] = self.run_ids
            data["logical_dates"] = self.logical_dates
        else:
            data["execution_dates"] = self.execution_dates

        return "airflow.providers.standard.triggers.external_task.WorkflowTrigger", data

    async def run(self) -> typing.AsyncIterator[TriggerEvent]:
        """Check periodically tasks, task group or dag status."""
        if AIRFLOW_V_3_0_PLUS:
            get_count_func = self._get_count_af_3
            run_id_or_dates = (self.run_ids or self.logical_dates) or []
        else:
            get_count_func = self._get_count
            run_id_or_dates = self.execution_dates or []

        while True:
            if self.failed_states:
                failed_count = await get_count_func(self.failed_states)
                if failed_count > 0:
                    yield TriggerEvent({"status": "failed"})
                    return

            if self.skipped_states:
                skipped_count = await get_count_func(self.skipped_states)
                if skipped_count > 0:
                    yield TriggerEvent({"status": "skipped"})
                    return
            allowed_count = await get_count_func(self.allowed_states)

            if allowed_count == len(run_id_or_dates):  # type: ignore[arg-type]
                yield TriggerEvent({"status": "success"})
                return
            self.log.info("Sleeping for %s seconds", self.poke_interval)
            await asyncio.sleep(self.poke_interval)

    async def _get_count_af_3(self, states):
        from airflow.providers.standard.utils.sensor_helper import _get_count_by_matched_states
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        params = {
            "dag_id": self.external_dag_id,
            "logical_dates": self.logical_dates,
            "run_ids": self.run_ids,
        }
        if self.external_task_ids:
            count = await sync_to_async(RuntimeTaskInstance.get_ti_count)(
                task_ids=self.external_task_ids,  # type: ignore[arg-type]
                states=states,
                **params,
            )
        elif self.external_task_group_id:
            run_id_task_state_map = await sync_to_async(RuntimeTaskInstance.get_task_states)(
                task_group_id=self.external_task_group_id,
                **params,
            )
            count = await sync_to_async(_get_count_by_matched_states)(
                run_id_task_state_map=run_id_task_state_map,
                states=states,
            )
        else:
            count = await sync_to_async(RuntimeTaskInstance.get_dr_count)(
                dag_id=self.external_dag_id,
                logical_dates=self.logical_dates,
                run_ids=self.run_ids,
                states=states,
            )
        if self.external_task_ids:
            return count / len(self.external_task_ids)
        return count

    @sync_to_async
    def _get_count(self, states: typing.Iterable[str] | None) -> int:
        """
        Get the count of records against dttm filter and states. Async wrapper for _get_count.

        :param states: task or dag states
        :return The count of records.
        """
        return _get_count(
            dttm_filter=self.run_ids if AIRFLOW_V_3_0_PLUS else self.execution_dates,
            external_task_ids=self.external_task_ids,
            external_task_group_id=self.external_task_group_id,
            external_dag_id=self.external_dag_id,
            states=states,
        )


class DagStateTrigger(BaseTrigger):
    """
    Waits asynchronously for a dag to complete for a specific run_id.

    :param dag_id: The dag_id that contains the task you want to wait for
    :param states: allowed states, default is ``['success']``
    :param run_ids: The run_id of dag run.
    :param poll_interval: The time interval in seconds to check the state.
        The default value is 5.0 sec.
    """

    def __init__(
        self,
        dag_id: str,
        states: list[DagRunState],
        run_ids: list[str] | None = None,
        execution_dates: list[datetime] | None = None,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.states = states
        self.run_ids = run_ids
        self.execution_dates = execution_dates
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, typing.Any]]:
        """Serialize DagStateTrigger arguments and classpath."""
        data = {
            "dag_id": self.dag_id,
            "states": self.states,
            "poll_interval": self.poll_interval,
            "run_ids": self.run_ids,
            "execution_dates": self.execution_dates,
        }

        return "airflow.providers.standard.triggers.external_task.DagStateTrigger", data

    async def run(self) -> typing.AsyncIterator[TriggerEvent]:
        """Check periodically if the dag run exists, and has hit one of the states yet, or not."""
        runs_ids_or_dates = 0
        if self.run_ids:
            runs_ids_or_dates = len(self.run_ids)
        elif self.execution_dates:
            runs_ids_or_dates = len(self.execution_dates)

        if AIRFLOW_V_3_0_PLUS:
            event = await self.validate_count_dags_af_3(runs_ids_or_dates_len=runs_ids_or_dates)
            yield TriggerEvent(event)
            return
        else:
            while True:
                num_dags = await self.count_dags()  # type: ignore[call-arg]
                if num_dags == runs_ids_or_dates:
                    yield TriggerEvent(self.serialize())
                    return
                await asyncio.sleep(self.poll_interval)

    async def validate_count_dags_af_3(self, runs_ids_or_dates_len: int = 0) -> tuple[str, dict[str, Any]]:
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        cls_path, data = self.serialize()

        while True:
            num_dags = await sync_to_async(RuntimeTaskInstance.get_dr_count)(
                dag_id=self.dag_id,
                run_ids=self.run_ids,
                states=self.states,  # type: ignore[arg-type]
                logical_dates=self.execution_dates,
            )
            if num_dags == runs_ids_or_dates_len:
                if isinstance(self.run_ids, list):
                    for run_id in self.run_ids:
                        state = await sync_to_async(RuntimeTaskInstance.get_dagrun_state)(
                            dag_id=self.dag_id,
                            run_id=run_id,
                        )
                        data[run_id] = state
                        return cls_path, data
            await asyncio.sleep(self.poll_interval)

    if not AIRFLOW_V_3_0_PLUS:
        from airflow.utils.session import NEW_SESSION, provide_session  # type: ignore[misc]

        @sync_to_async
        @provide_session
        def count_dags(self, *, session: Session = NEW_SESSION) -> int:
            """Count how many dag runs in the database match our criteria."""
            _dag_run_date_condition = (
                DagRun.run_id.in_(self.run_ids)
                if AIRFLOW_V_3_0_PLUS
                else DagRun.execution_date.in_(self.execution_dates)
            )
            count = (
                session.query(func.count("*"))  # .count() is inefficient
                .filter(
                    DagRun.dag_id == self.dag_id,
                    DagRun.state.in_(self.states),
                    _dag_run_date_condition,
                )
                .scalar()
            )
            return typing.cast("int", count)
