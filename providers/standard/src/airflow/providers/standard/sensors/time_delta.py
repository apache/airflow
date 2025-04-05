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

from datetime import datetime, timedelta
from time import sleep
from typing import TYPE_CHECKING, Any, NoReturn

from packaging.version import Version

from airflow.configuration import conf
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.sensors.base import BaseSensorOperator
from airflow.utils import timezone

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


def _get_airflow_version():
    from airflow import __version__ as airflow_version

    return Version(Version(airflow_version).base_version)


class TimeDeltaSensor(BaseSensorOperator):
    """
    Waits for a timedelta.

    The delta will be evaluated against data_interval_end if present for the dag run,
    otherwise run_after will be used.

    :param delta: time to wait before succeeding.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeDeltaSensor`

    """

    def __init__(self, *, delta, **kwargs):
        super().__init__(**kwargs)
        self.delta = delta

    def _derive_base_time(self, context: Context) -> datetime:
        """
        Get the "base time" against which the delta should be calculated.

        If data_interval_end is populated, use it; else use run_after.
        """
        data_interval_end = context.get("data_interval_end")
        if data_interval_end:
            if not isinstance(data_interval_end, datetime):
                raise ValueError("`data_interval_end` returned non-datetime object")

            return data_interval_end

        if not data_interval_end and not AIRFLOW_V_3_0_PLUS:
            raise ValueError("`data_interval_end` not found in task context.")

        dag_run = context.get("dag_run")
        if not dag_run:
            raise ValueError("`dag_run` not found in task context")
        return dag_run.run_after

    def poke(self, context: Context) -> bool:
        base_time = self._derive_base_time(context=context)
        target_dttm = base_time + self.delta
        self.log.info("Checking if the delta has elapsed base_time=%s, delta=%s", base_time, self.delta)
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
        base_time = self._derive_base_time(context=context)
        target_dttm: datetime = base_time + self.delta

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

        # todo: remove backcompat when min airflow version greater than 2.11
        timeout: int | float | timedelta
        if _get_airflow_version() >= Version("2.11.0"):
            timeout = self.timeout
        else:
            timeout = timedelta(seconds=self.timeout)

        self.defer(
            trigger=trigger,
            method_name="execute_complete",
            timeout=timeout,
        )

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
