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
from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NoReturn

from airflow.providers.common.compat.sdk import BaseSensorOperator, timezone
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.triggers.base import StartTriggerArgs

if TYPE_CHECKING:
    from airflow.sdk import Context


class DateTimeSensor(BaseSensorOperator):
    """
    Waits until the specified datetime.

    A major advantage of this sensor is idempotence for the ``target_time``.
    It handles some cases for which ``TimeSensor`` and ``TimeDeltaSensor`` are not suited.

    **Example** 1 :
        If a task needs to wait for 11am on each ``logical_date``. Using
        ``TimeSensor`` or ``TimeDeltaSensor``, all backfill tasks started at
        1am have to wait for 10 hours. This is unnecessary, e.g. a backfill
        task with ``{{ ds }} = '1970-01-01'`` does not need to wait because
        ``1970-01-01T11:00:00`` has already passed.

    **Example** 2 :
        If a DAG is scheduled to run at 23:00 daily, but one of the tasks is
        required to run at 01:00 next day, using ``TimeSensor`` will return
        ``True`` immediately because 23:00 > 01:00. Instead, we can do this:

        .. code-block:: python

            DateTimeSensor(
                task_id="wait_for_0100",
                target_time="{{ data_interval_end.tomorrow().replace(hour=1) }}",
            )

    :param target_time: datetime after which the job succeeds. (templated)
    """

    template_fields: Sequence[str] = ("target_time",)

    def __init__(self, *, target_time: str | datetime.datetime, **kwargs) -> None:
        super().__init__(**kwargs)
        # target_time is a template field, so it is rendered after __init__ runs. Store it
        # verbatim and defer validation/normalization to _moment, which sees the rendered value.
        self.target_time = target_time

    def poke(self, context: Context) -> bool:
        self.log.info("Checking if the time (%s) has come", self.target_time)
        return timezone.utcnow() > self._moment

    @property
    def _moment(self) -> datetime.datetime:
        # target_time is a template field: after rendering it is usually a string, but with
        # render_template_as_native_obj=True it can already be a datetime.
        target_time: Any = self.target_time
        if isinstance(target_time, datetime.datetime):
            return target_time
        if isinstance(target_time, str):
            return timezone.parse(target_time)
        raise TypeError(f"Expected str or datetime.datetime type for target_time. Got {type(target_time)}")


class DateTimeSensorAsync(DateTimeSensor):
    """
    Wait until the specified datetime occurs.

    Deferring itself to avoid taking up a worker slot while it is waiting.
    It is a drop-in replacement for DateTimeSensor.

    :param target_time: datetime after which the job succeeds. (templated)
    :param start_from_trigger: Start the task directly from the triggerer without going into the worker.
    :param trigger_kwargs: The keyword arguments passed to the trigger when start_from_trigger is set to True
        during dynamic task mapping. This argument is not used in standard usage.
    :param end_from_trigger: End the task directly from the triggerer without going into the worker.
    """

    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
        trigger_kwargs={"moment": "", "end_from_trigger": False},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )
    start_from_trigger = False

    def __init__(
        self,
        *,
        start_from_trigger: bool = False,
        end_from_trigger: bool = False,
        trigger_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.end_from_trigger = end_from_trigger

        self.start_from_trigger = start_from_trigger
        if self.start_from_trigger:
            self.start_trigger_args.trigger_kwargs = dict(
                moment=self._moment,
                end_from_trigger=self.end_from_trigger,
            )

    def execute(self, context: Context) -> NoReturn:
        self.defer(
            method_name="execute_complete",
            trigger=DateTimeTrigger(
                moment=self._moment,
                end_from_trigger=self.end_from_trigger,
            )
            if AIRFLOW_V_3_0_PLUS
            else DateTimeTrigger(moment=self._moment),
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """Handle the event when the trigger fires and return immediately."""
        return None
