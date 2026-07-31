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
    which stores only the parse-stable ``target_time`` + timezone and resolves
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

    # Class-level defaults are read-only templates. Instances that use
    # start_from_trigger=True replace start_trigger_args with a *new* StartTriggerArgs
    # so they never mutate this shared object (that mutation was a separate bug:
    # every TimeSensor would inherit the last-constructed sensor's kwargs).
    start_trigger_args = StartTriggerArgs(
        trigger_cls="airflow.providers.standard.triggers.temporal.TimeOfDayTrigger",
        trigger_kwargs={"target_time": "00:00:00", "timezone": "UTC", "end_from_trigger": False},
        next_method="execute_complete",
        next_kwargs=None,
        timeout=None,
    )
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
        # Store wall-clock time only; strip tzinfo so serialization is deterministic.
        # Timezone comes from the Dag (or UTC when unattached).
        if isinstance(target_time, datetime.time) and target_time.tzinfo is not None:
            self.target_time = target_time.replace(tzinfo=None)
        else:
            self.target_time = target_time
        self.deferrable = deferrable
        self.start_from_trigger = start_from_trigger
        self.end_from_trigger = end_from_trigger
        # Cached for the life of this task attempt (execute/poke/trigger-start).
        # Avoids midnight drift if poke is re-entered after the local date rolls.
        self._cached_target_datetime: datetime.datetime | None = None

        if self.start_from_trigger:
            # Per-instance StartTriggerArgs — never mutate the class attribute.
            # All values are parse-stable (no datetime.now()), so serialized Dag
            # bytes stay identical across Dag-processor parses.
            self.start_trigger_args = StartTriggerArgs(
                trigger_cls="airflow.providers.standard.triggers.temporal.TimeOfDayTrigger",
                trigger_kwargs={
                    "target_time": self.target_time.isoformat(),
                    "timezone": self._serializable_dag_timezone(),
                    "end_from_trigger": self.end_from_trigger,
                },
                next_method="execute_complete",
                next_kwargs=None,
                timeout=None,
            )

    def _serializable_dag_timezone(self) -> str | int:
        """
        Encode the Dag timezone for trigger kwargs.

        Uses ``self._dag`` (does not raise) so constructing a sensor before it is
        attached to a Dag falls back to UTC instead of crashing. When a Dag is
        present its timezone is encoded as an IANA name or fixed offset seconds.
        """
        dag = self._dag
        if dag is None:
            return "UTC"
        return serializable_timezone(dag.timezone)

    def _resolve_target_datetime(self) -> datetime.datetime:
        """Compute the UTC moment for target_time on today's date in the Dag timezone."""
        dag = self._dag
        # Fall back to UTC when unattached (unit tests / early construction). When a
        # Dag is present, use its timezone so wall-clock semantics match the Dag.
        tz: str | int | datetime.tzinfo
        if dag is None:
            tz = "UTC"
        else:
            tz = dag.timezone
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
        # Resolve once for this attempt and cache (E2: no midnight recompute drift).
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
