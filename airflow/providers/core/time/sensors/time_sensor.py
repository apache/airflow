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
from typing import TYPE_CHECKING, Any, NoReturn

from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import StartTriggerArgs
from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils import timezone

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TimeSensor(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    :param target_time: time after which the job succeeds

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeSensor`

    """

    def __init__(self, *, target_time: datetime.time, **kwargs) -> None:
        super().__init__(**kwargs)
        self.target_time = target_time

    def poke(self, context: Context) -> bool:
        self.log.info("Checking if the time (%s) has come", self.target_time)
        return timezone.make_naive(timezone.utcnow(), self.dag.timezone).time() > self.target_time


class TimeSensorAsync(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    This frees up a worker slot while it is waiting.

    :param target_time: time after which the job succeeds
    :param start_from_trigger: Start the task directly from the triggerer without going into the worker.
    :param end_from_trigger: End the task directly from the triggerer without going into the worker.
    :param trigger_kwargs: The keyword arguments passed to the trigger when start_from_trigger is set to True
        during dynamic task mapping. This argument is not used in standard usage.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeSensorAsync`
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
        target_time: datetime.time,
        start_from_trigger: bool = False,
        trigger_kwargs: dict[str, Any] | None = None,
        end_from_trigger: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.start_from_trigger = start_from_trigger
        self.end_from_trigger = end_from_trigger
        self.target_time = target_time

        aware_time = timezone.coerce_datetime(
            datetime.datetime.combine(datetime.datetime.today(), self.target_time, self.dag.timezone)
        )

        self.target_datetime = timezone.convert_to_utc(aware_time)
        if self.start_from_trigger:
            self.start_trigger_args.trigger_kwargs = dict(
                moment=self.target_datetime, end_from_trigger=self.end_from_trigger
            )

    def execute(self, context: Context) -> NoReturn:
        self.defer(
            trigger=DateTimeTrigger(moment=self.target_datetime, end_from_trigger=self.end_from_trigger),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """Handle the event when the trigger fires and return immediately."""
        return None
