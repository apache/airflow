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
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone


class DateTimeTrigger(BaseTrigger):
    """
    Trigger based on a datetime.

    A trigger that fires exactly once, at the given datetime, give or take
    a few seconds.

    The provided datetime MUST be in UTC.
    """

    def __init__(self, moment: datetime.datetime):
        super().__init__()
        if not isinstance(moment, datetime.datetime):
            raise TypeError(f"Expected datetime.datetime type for moment. Got {type(moment)}")
        # Make sure it's in UTC
        elif moment.tzinfo is None:
            raise ValueError("You cannot pass naive datetimes")
        else:
            self.moment = timezone.convert_to_utc(moment)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return ("airflow.triggers.temporal.DateTimeTrigger", {"moment": self.moment})

    async def run(self):
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
            seconds_remaining = (self.moment - timezone.utcnow()).total_seconds()
            while seconds_remaining > 2 * step:
                self.log.info(f"{int(seconds_remaining)} seconds remaining; sleeping {step} seconds")
                await asyncio.sleep(step)
                seconds_remaining = (self.moment - timezone.utcnow()).total_seconds()
        # Sleep a second at a time otherwise
        while self.moment > timezone.utcnow():
            self.log.info("sleeping 1 second...")
            await asyncio.sleep(1)
        # Send our single event and then we're done
        self.log.info("yielding event with payload %r", self.moment)
        yield TriggerEvent(self.moment)


class TimeDeltaTrigger(DateTimeTrigger):
    """
    Create DateTimeTriggers based on delays.

    Subclass to create DateTimeTriggers based on time delays rather
    than exact moments.

    While this is its own distinct class here, it will serialise to a
    DateTimeTrigger class, since they're operationally the same.
    """

    def __init__(self, delta: datetime.timedelta):
        super().__init__(moment=timezone.utcnow() + delta)
