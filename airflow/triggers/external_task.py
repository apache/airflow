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

import asyncio
from datetime import datetime
from typing import Any, AsyncIterator, Collection, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func

from airflow.exceptions import AirflowException
from airflow.models import DagRun as DR, TaskInstance as TI
from airflow.settings import SASession
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State


class ExternalTaskMixin(LoggingMixin):
    """Mixin class to check external tasks with-in states"""

    external_dag_id: str
    external_task_ids: Optional[Collection[str]]
    allowed_states: List[str]
    failed_states: List[str]

    def _set_external_task(
        self,
        *,
        external_dag_id: str,
        external_task_ids: Optional[Collection[str]] = None,
        allowed_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
    ):
        self.external_dag_id = external_dag_id
        self.external_task_ids = external_task_ids
        self.allowed_states = list(allowed_states) if allowed_states else [State.SUCCESS]
        self.failed_states = list(failed_states) if failed_states else []

        total_states = set(self.allowed_states + self.failed_states)

        if set(self.failed_states).intersection(set(self.allowed_states)):
            raise AirflowException(
                f"Duplicate values provided as allowed "
                f"`{self.allowed_states}` and failed states `{self.failed_states}`"
            )

        if external_task_ids:
            if not total_states <= set(State.task_states):
                raise ValueError(
                    f'Valid values for `allowed_states` and `failed_states` '
                    f'when `external_task_ids` is not `None`: {State.task_states}'
                )
            if len(external_task_ids) > len(set(external_task_ids)):
                raise ValueError('Duplicate task_ids passed in external_task_ids parameter')
        elif not total_states <= set(State.dag_states):
            raise ValueError(
                f'Valid values for `allowed_states` and `failed_states` '
                f'when `external_task_ids` is `None`: {State.dag_states}'
            )

    @provide_session
    def _check_for_states(self, dttm: List[datetime], session: SASession) -> bool:
        """
        :return:
            True if the external tasks succeed.
            Fails if the external tasks are in progress.
        :raise AirflowException: if the external tasks failed
        """
        serialized_dttm_filter = ','.join(dt.isoformat() for dt in dttm)

        self.log.info(
            'Poking for tasks %s in dag %s on %s ... ',
            self.external_task_ids,
            self.external_dag_id,
            serialized_dttm_filter,
        )

        count_allowed = self._get_count(dttm, self.allowed_states, session)

        count_failed = -1
        if self.failed_states:
            count_failed = self._get_count(dttm, self.failed_states, session)

        if count_failed == len(dttm):
            if self.external_task_ids:
                raise AirflowException(
                    f'Some of the external tasks {self.external_task_ids} '
                    f'in DAG {self.external_dag_id} failed.'
                )
            else:
                raise AirflowException(f'The external DAG {self.external_dag_id} failed.')

        return count_allowed == len(dttm)

    def _get_count(self, dttm: List[datetime], states: List[str], session: SASession) -> int:
        """
        Get the count of records against dttm filter and states

        :param dttm: date time filter for execution date
        :param states: task or dag states
        :param session: airflow session object
        :return: count of record against the filters
        """
        if self.external_task_ids:
            count = (
                session.query(func.count())  # .count() is inefficient
                .filter(
                    TI.dag_id == self.external_dag_id,
                    TI.task_id.in_(self.external_task_ids),
                    TI.state.in_(states),
                    TI.execution_date.in_(dttm),
                )
                .scalar()
            )
            count = count / len(self.external_task_ids)
        else:
            count = (
                session.query(func.count())
                .filter(
                    DR.dag_id == self.external_dag_id,
                    DR.state.in_(states),
                    DR.execution_date.in_(dttm),
                )
                .scalar()
            )
        return count


class ExternalTaskTrigger(BaseTrigger, ExternalTaskMixin):
    """
    A trigger that fires when external tasks is in allowed_states or
    raise an error when it's in failed_states.
    """

    def __init__(
        self,
        *,
        dttm: List[datetime],
        external_dag_id: str,
        external_task_ids: List[str] = None,
        allowed_states: List[str] = None,
        failed_states: List[str] = None,
        poke_interval: float = 60,
        **kwargs,
    ):
        super().__init__(**kwargs)
        super()._set_external_task(
            external_dag_id=external_dag_id,
            external_task_ids=external_task_ids,
            allowed_states=allowed_states,
            failed_states=failed_states,
        )
        self.dttm = dttm
        self.poke_interval = poke_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "airflow.triggers.external_task.ExternalTaskTrigger",
            {
                "dttm": self.dttm,
                "external_dag_id": self.external_dag_id,
                "external_task_ids": self.external_task_ids,
                "allowed_states": self.allowed_states,
                "failed_states": self.failed_states,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while not self._check_for_states(dttm=self.dttm):
            await asyncio.sleep(self.poke_interval)

        yield TriggerEvent(True)
