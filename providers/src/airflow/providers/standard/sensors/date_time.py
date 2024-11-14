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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn, Sequence

from airflow.providers.standard.utils.version_references import AIRFLOW_V_3_0_PLUS
from airflow.sensors.base import BaseSensorOperator

try:
    from airflow.triggers.base import StartTriggerArgs
except ImportError:
    # TODO: Remove this when min airflow version is 2.10.0 for standard provider
    @dataclass
    class StartTriggerArgs:  # type: ignore[no-redef]
        """Arguments required for start task execution from triggerer."""

        trigger_cls: str
        next_method: str
        trigger_kwargs: dict[str, Any] | None = None
        next_kwargs: dict[str, Any] | None = None
        timeout: datetime.timedelta | None = None


from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils import timezone

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DateTimeSensor(BaseSensorOperator):
    """
    Waits until the specified datetime.

    A major advantage of this sensor is idempotence for the ``target_time``.
    It handles some cases for which ``TimeSensor`` and ``TimeDeltaSensor`` are not suited.

    **Example** 1 :
        If a task needs to wait for 11am on each ``execution_date``. Using
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
                target_time="{{ next_execution_date.tomorrow().replace(hour=1) }}",
            )

    :param target_time: datetime after which the job succeeds. (templated)
    """

    template_fields: Sequence[str] = ("target_time",)

    def __init__(self, *, target_time: str | datetime.datetime, **kwargs) -> None:
        super().__init__(**kwargs)

        # self.target_time can't be a datetime object as it is a template_field
        if isinstance(target_time, datetime.datetime):
            self.target_time = target_time.isoformat()
        elif isinstance(target_time, str):
            self.target_time = target_time
        else:
            raise TypeError(
                f"Expected str or datetime.datetime type for target_time. Got {type(target_time)}"
            )

    def poke(self, context: Context) -> bool:
        self.log.info("Checking if the time (%s) has come", self.target_time)
        return timezone.utcnow() > timezone.parse(self.target_time)


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
        trigger_cls="airflow.triggers.temporal.DateTimeTrigger",
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
                moment=timezone.parse(self.target_time),
                end_from_trigger=self.end_from_trigger,
            )

    def execute(self, context: Context) -> NoReturn:
        self.defer(
            method_name="execute_complete",
            trigger=DateTimeTrigger(
                moment=timezone.parse(self.target_time),
                end_from_trigger=self.end_from_trigger,
            )
            if AIRFLOW_V_3_0_PLUS
            else DateTimeTrigger(moment=timezone.parse(self.target_time)),
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """Handle the event when the trigger fires and return immediately."""
        return None
