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
from typing import Any, AsyncIterator

import pendulum

from airflow.triggers.base import BaseTrigger, TaskSuccessEvent, TriggerEvent
from airflow.utils import timezone


class DateTimeTrigger(BaseTrigger):
    """
    Trigger based on a datetime.

    A trigger that fires exactly once, at the given datetime, give or take
    a few seconds.

    The provided datetime MUST be in UTC.

    :param moment: when to yield event
    :param end_from_trigger: whether the trigger should mark the task successful after time condition
        reached or resume the task after time condition reached.
    """

    def __init__(self, moment: datetime.datetime, *, end_from_trigger: bool = False) -> None:
        super().__init__()
        if not isinstance(moment, datetime.datetime):
            raise TypeError(f"Expected datetime.datetime type for moment. Got {type(moment)}")
        # Make sure it's in UTC
        elif moment.tzinfo is None:
            raise ValueError("You cannot pass naive datetimes")
        else:
            self.moment: pendulum.DateTime = timezone.convert_to_utc(moment)
        self.end_from_trigger = end_from_trigger

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.triggers.temporal.DateTimeTrigger",
            {"moment": self.moment, "end_from_trigger": self.end_from_trigger},
        )

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
