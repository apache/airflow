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

from datetime import timedelta
from time import sleep
from typing import TYPE_CHECKING, Any, NoReturn

from airflow.configuration import conf
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.utils.version_references import AIRFLOW_V_3_0_PLUS
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.utils import timezone

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TimeDeltaSensor(BaseSensorOperator):
    """
    Waits for a timedelta after the run's data interval.

    :param delta: time length to wait after the data interval before succeeding.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeDeltaSensor`


    """

    def __init__(self, *, delta, **kwargs):
        super().__init__(**kwargs)
        self.delta = delta

    def poke(self, context: Context):
        target_dttm = context["data_interval_end"]
        target_dttm += self.delta
        self.log.info("Checking if the time (%s) has come", target_dttm)
        return timezone.utcnow() > target_dttm


class TimeDeltaSensorAsync(TimeDeltaSensor):
    """
    A deferrable drop-in replacement for TimeDeltaSensor.

    Will defers itself to avoid taking up a worker slot while it is waiting.

    :param delta: time length to wait after the data interval before succeeding.
    :param end_from_trigger: End the task directly from the triggerer without going into the worker.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeDeltaSensorAsync`

    """

    def __init__(self, *, end_from_trigger: bool = False, delta, **kwargs) -> None:
        super().__init__(delta=delta, **kwargs)
        self.end_from_trigger = end_from_trigger

    def execute(self, context: Context) -> bool | NoReturn:
        target_dttm = context["data_interval_end"]
        target_dttm += self.delta
        if timezone.utcnow() > target_dttm:
            # If the target datetime is in the past, return immediately
            return True
        try:
            if AIRFLOW_V_3_0_PLUS:
                trigger = DateTimeTrigger(moment=target_dttm, end_from_trigger=self.end_from_trigger)
            else:
                trigger = DateTimeTrigger(moment=target_dttm)
        except (TypeError, ValueError) as e:
            if self.soft_fail:
                raise AirflowSkipException("Skipping due to soft_fail is set to True.") from e
            raise

        self.defer(trigger=trigger, method_name="execute_complete")

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """Handle the event when the trigger fires and return immediately."""
        return None


class WaitSensor(BaseSensorOperator):
    """
    A sensor that waits a specified period of time before completing.

    This differs from TimeDeltaSensor because the time to wait is measured from the start of the task, not
    the data_interval_end of the DAG run.

    :param time_to_wait: time length to wait after the task starts before succeeding.
    :param deferrable: Run sensor in deferrable mode
    """

    def __init__(
        self,
        time_to_wait: timedelta | int,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.deferrable = deferrable
        if isinstance(time_to_wait, int):
            self.time_to_wait = timedelta(minutes=time_to_wait)
        else:
            self.time_to_wait = time_to_wait

    def execute(self, context: Context) -> None:
        if self.deferrable:
            self.defer(
                trigger=TimeDeltaTrigger(self.time_to_wait, end_from_trigger=True)
                if AIRFLOW_V_3_0_PLUS
                else TimeDeltaTrigger(self.time_to_wait),
                method_name="execute_complete",
            )
        else:
            sleep(int(self.time_to_wait.total_seconds()))
