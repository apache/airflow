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
import warnings
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import BaseSensorOperator, conf, timezone
from airflow.providers.standard.triggers.temporal import (
    DateTimeTrigger,
    resolve_time_of_day_moment,
    serializable_timezone,
)
from airflow.triggers.base import StartTriggerArgs

if TYPE_CHECKING:
    from airflow.sdk import Context


class TimeSensor(BaseSensorOperator):
    """
    Waits until the specified time of the day.

    The time is evaluated against the wall-clock date in the Dag's timezone at
    execution time (poke / deferral / trigger start), not at Dag-parse time.
    This avoids dag_version churn from baking an absolute ``moment`` into
    serialized ``start_trigger_args``.

    When ``start_from_trigger=True``, the sensor starts directly on the triggerer
    via :class:`~airflow.providers.standard.triggers.temporal.TimeOfDayTrigger`,
    which stores only the parse-stable ``target_time`` + tz and resolves
    the concrete moment when the trigger actually starts.

    :param target_time: time after which the job succeeds
    :param deferrable: whether to defer execution
    :param start_from_trigger: Start the task directly from the triggerer without
        going into the worker.
    :param end_from_trigger: End the task directly from the triggerer without
        going into the worker.
    :param trigger_kwargs: Accepted for API compatibility with other sensors that
        support dynamic task mapping into start-from-trigger; not used by TimeSensor.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:TimeSensor`

    """

    start_trigger_args = None
    start_from_trigger = False

    def __init__(
        self,
        *,
        target_time: datetime.time,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        start_from_trigger: bool = False,
        end_from_trigger: bool = False,
        trigger_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        # Wall-clock only; tzinfo is stripped so serialized target_time is deterministic.
        if isinstance(target_time, datetime.time) and target_time.tzinfo is not None:
            self.target_time = target_time.replace(tzinfo=None)
        else:
            self.target_time = target_time
        self.deferrable = deferrable
        self.start_from_trigger = start_from_trigger
        self.end_from_trigger = end_from_trigger
        # Cached for this task attempt so a local-date rollover does not change the target.
        self._cached_target_datetime: datetime.datetime | None = None

        if self.start_from_trigger:
            dag = self._dag
            if dag is None:
                raise ValueError(
                    "TimeSensor(start_from_trigger=True) requires the sensor to be attached to a Dag "
                    "so the timezone is known."
                )
            # Parse-stable kwargs only (no datetime.now()); moment is resolved when the trigger starts.
            self.start_trigger_args = StartTriggerArgs(
                trigger_cls="airflow.providers.standard.triggers.temporal.TimeOfDayTrigger",
                trigger_kwargs={
                    "target_time": self.target_time.isoformat(),
                    "tz": serializable_timezone(dag.timezone),
                    "end_from_trigger": self.end_from_trigger,
                },
                next_method="execute_complete",
                next_kwargs=None,
                timeout=None,
            )

    def _resolve_target_datetime(self) -> datetime.datetime:
        """Compute the UTC moment for target_time on today's date in the Dag timezone."""
        dag = self._dag
        # Unattached sensors (unit tests / early construction) use UTC.
        tz: str | int | datetime.tzinfo = "UTC" if dag is None else dag.timezone
        return resolve_time_of_day_moment(self.target_time, tz=tz)

    def _get_target_datetime(self) -> datetime.datetime:
        """Return the target moment, computing and caching it once per attempt."""
        if self._cached_target_datetime is None:
            self._cached_target_datetime = self._resolve_target_datetime()
        return self._cached_target_datetime

    @property
    def target_datetime(self) -> datetime.datetime:
        """
        Resolved target datetime in UTC.

        Computed on first access (or first execute/poke) from ``target_time`` and
        the Dag timezone, then cached for the life of this instance. Two reads on
        either side of midnight therefore return the *same* date for a given
        attempt; a fresh task instance re-resolves against "today".
        """
        return self._get_target_datetime()

    def execute(self, context: Context) -> None:
        moment = self._get_target_datetime()
        if self.deferrable:
            self.defer(
                trigger=DateTimeTrigger(
                    moment=moment,
                    end_from_trigger=self.end_from_trigger,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context)

    def execute_complete(self, context: Context, event: Any = None) -> None:
        return None

    def poke(self, context: Context) -> bool:
        target_datetime = self._get_target_datetime()
        self.log.info("Checking if the time (%s) has come", target_datetime)
        return timezone.utcnow() > target_datetime


class TimeSensorAsync(TimeSensor):
    """
    Deprecated. Use TimeSensor with deferrable=True instead.

    :sphinx-autoapi-skip:
    """

    def __init__(self, **kwargs) -> None:
        warnings.warn(
            "TimeSensorAsync is deprecated and will be removed in a future version. Use `TimeSensor` with deferrable=True instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
