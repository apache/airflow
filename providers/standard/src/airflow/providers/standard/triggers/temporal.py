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

    Pass either ``moment`` (a tz-aware datetime) or ``target_time`` (a string, possibly a Jinja
    template). ``target_time`` is listed in ``template_fields`` so that, with ``start_from_trigger``,
    the triggerer renders it in place before ``run()`` and it is parsed into ``moment`` on first use.

    :param moment: when to yield event
    :param target_time: raw (possibly templated) datetime string, an alternative to ``moment``
    :param end_from_trigger: whether the trigger should mark the task successful after time condition
        reached or resume the task after time condition reached.
    """

    template_fields = ("target_time",)

    def __init__(
        self,
        moment: datetime.datetime | None = None,
        *,
        target_time: datetime.datetime | str | None = None,
        end_from_trigger: bool = False,
    ) -> None:
        super().__init__()
        if (moment is None) == (target_time is None):
            raise TypeError("DateTimeTrigger requires exactly one of 'moment' or 'target_time'")
        self.target_time = target_time
        self._moment: pendulum.DateTime | None = None
        if moment is not None:
            if not isinstance(moment, datetime.datetime):
                raise TypeError(f"Expected datetime.datetime type for moment. Got {type(moment)}")
            # Make sure it's in UTC
            if moment.tzinfo is None:
                raise ValueError("You cannot pass naive datetimes")
            self._moment = timezone.convert_to_utc(moment)
        self.end_from_trigger = end_from_trigger

    @property
    def moment(self) -> pendulum.DateTime:
        if self._moment is None:
            # Resolved lazily: by now the triggerer has rendered target_time in place.
            if not isinstance(self.target_time, str) or not self.target_time:
                raise TypeError("DateTimeTrigger has neither a 'moment' nor a usable 'target_time'")
            self._moment = timezone.convert_to_utc(timezone.parse(self.target_time))
        return self._moment

    def serialize(self) -> tuple[str, dict[str, Any]]:
        if self._moment is None:
            kwargs: dict[str, Any] = {"target_time": self.target_time}
        else:
            kwargs = {"moment": self._moment}
        kwargs["end_from_trigger"] = self.end_from_trigger
        return ("airflow.providers.standard.triggers.temporal.DateTimeTrigger", kwargs)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Loop until the relevant time is met.

        We do have a two-phase delay to save some cycles, but sleeping is so
        cheap anyway that it's pretty loose. We also don't just sleep for
        "the number of seconds until the time" in case the system clock changes
        unexpectedly, or handles a DST change poorly.
        """
        # Sleep in successively smaller increments starting from 1 hour down to 10 seconds at a time
        self.log.info("trigger starting")
        for step in 3600, 60, 10:
            seconds_remaining = (self.moment - pendulum.instance(timezone.utcnow())).total_seconds()
            while seconds_remaining > 2 * step:
                self.log.info("%d seconds remaining; sleeping %s seconds", seconds_remaining, step)
                await asyncio.sleep(step)
                seconds_remaining = (self.moment - pendulum.instance(timezone.utcnow())).total_seconds()
        # Sleep a second at a time otherwise
        while self.moment > pendulum.instance(timezone.utcnow()):
            self.log.info("sleeping 1 second...")
            await asyncio.sleep(1)
        if self.end_from_trigger:
            self.log.info("Sensor time condition reached; marking task successful and exiting")
            yield TaskSuccessEvent()
        else:
            self.log.info("yielding event with payload %r", self.moment)
            yield TriggerEvent(self.moment)


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
