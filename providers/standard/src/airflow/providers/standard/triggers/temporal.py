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
import datetime
from collections.abc import AsyncIterator
from typing import Any

import pendulum

from airflow.providers.common.compat.sdk import timezone
from airflow.triggers.base import BaseTrigger, TaskSuccessEvent, TriggerEvent


class DateTimeTrigger(BaseTrigger):
    """
    Trigger based on a datetime.

    A trigger that fires exactly once, at the given datetime, give or take
    a few seconds.

    The provided datetime MUST be in UTC.

    :param moment: when to yield event
    :param target_time: Templated ``target_time`` value to resolve when the trigger runs. Used
        instead of ``moment`` when the target time cannot be determined during operator
        initialization. Mutually exclusive with ``moment``.
    :param end_from_trigger: whether the trigger should mark the task successful after time condition
        reached or resume the task after time condition reached.
    """

    def __init__(
        self,
        moment: datetime.datetime | None = None,
        *,
        target_time: str | None = None,
        end_from_trigger: bool = False,
    ) -> None:
        super().__init__()
        if moment is None and target_time is None:
            raise TypeError("DateTimeTrigger requires either 'moment' or 'target_time' to be set")
        if moment is not None and target_time is not None:
            raise TypeError("DateTimeTrigger accepts only one of 'moment' or 'target_time', not both")

        self.end_from_trigger = end_from_trigger
        self.target_time = target_time

        self.moment: pendulum.DateTime | None
        if moment is None:
            self.moment = None
            return
        if not isinstance(moment, datetime.datetime):
            raise TypeError(f"Expected datetime.datetime type for moment. Got {type(moment)}")
        # Make sure it's in UTC
        if moment.tzinfo is None:
            raise ValueError("You cannot pass naive datetimes")
        self.moment = timezone.convert_to_utc(moment)

    def _resolve_moment(self) -> pendulum.DateTime:
        """Return ``moment``, parsing it from a (by now rendered) ``target_time`` if needed."""
        if self.moment is not None:
            return self.moment
        if not self.target_time:
            raise TypeError("DateTimeTrigger requires either 'moment' or 'target_time' to be set")
        try:
            parsed = timezone.parse(self.target_time)
        except ValueError as e:
            raise ValueError(
                f"Could not parse target_time {self.target_time!r} as a datetime after template "
                "rendering. start_from_trigger requires target_time to render to a static datetime "
                "or ISO-8601 string."
            ) from e
        self.moment = timezone.convert_to_utc(parsed)
        return self.moment

    def serialize(self) -> tuple[str, dict[str, Any]]:
        # `target_time` folds into `moment` here, same as `TimeDeltaTrigger` folds `delta` into
        # `moment` (see the exclusion for this class in check_trigger_serialize_init.py).
        return (
            "airflow.providers.standard.triggers.temporal.DateTimeTrigger",
            {"moment": self._resolve_moment(), "end_from_trigger": self.end_from_trigger},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Loop until the relevant time is met.

        We do have a two-phase delay to save some cycles, but sleeping is so
        cheap anyway that it's pretty loose. We also don't just sleep for
        "the number of seconds until the time" in case the system clock changes
        unexpectedly, or handles a DST change poorly.
        """
        moment = self._resolve_moment()
        # Sleep in successively smaller increments starting from 1 hour down to 10 seconds at a time
        self.log.info("trigger starting")
        for step in 3600, 60, 10:
            seconds_remaining = (moment - pendulum.instance(timezone.utcnow())).total_seconds()
            while seconds_remaining > 2 * step:
                self.log.info("%d seconds remaining; sleeping %s seconds", seconds_remaining, step)
                await asyncio.sleep(step)
                seconds_remaining = (moment - pendulum.instance(timezone.utcnow())).total_seconds()
        # Sleep a second at a time otherwise
        while moment > pendulum.instance(timezone.utcnow()):
            self.log.info("sleeping 1 second...")
            await asyncio.sleep(1)
        if self.end_from_trigger:
            self.log.info("Sensor time condition reached; marking task successful and exiting")
            yield TaskSuccessEvent()
        else:
            self.log.info("yielding event with payload %r", moment)
            yield TriggerEvent(moment)


class TimeDeltaTrigger(DateTimeTrigger):
    """
    Create DateTimeTriggers based on delays.

    Subclass to create DateTimeTriggers based on time delays rather
    than exact moments.

    While this is its own distinct class here, it will serialise to a
    DateTimeTrigger class, since they're operationally the same.

    :param delta: how long to wait
    :param end_from_trigger: whether the trigger should mark the task successful after time condition
        reached or resume the task after time condition reached.
    """

    def __init__(self, delta: datetime.timedelta, *, end_from_trigger: bool = False) -> None:
        super().__init__(moment=timezone.utcnow() + delta, end_from_trigger=end_from_trigger)
