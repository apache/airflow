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
from typing import TYPE_CHECKING, NoReturn

from airflow.sensors.base import BaseSensorOperator
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

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeSensorAsync`
    """

    def __init__(self, *, target_time: datetime.time, **kwargs) -> None:
        super().__init__(**kwargs)
        self.target_time = target_time

        aware_time = timezone.coerce_datetime(
            datetime.datetime.combine(datetime.datetime.today(), self.target_time, self.dag.timezone)
        )

        self.target_datetime = timezone.convert_to_utc(aware_time)

    def execute(self, context: Context) -> NoReturn:
        trigger = DateTimeTrigger(moment=self.target_datetime)
        self.defer(
            trigger=trigger,
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None) -> None:
        """Execute when the trigger fires - returns immediately."""
        return None
