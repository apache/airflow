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
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow.providers.common.compat.sdk import timezone
from airflow.triggers.base import BaseTrigger, TaskSuccessEvent, TriggerEvent


def _parse_timezone(value: str | int | datetime.tzinfo) -> Timezone | FixedTimezone:
    """Return a pendulum timezone from an IANA name, fixed offset (seconds), or existing tzinfo."""
    if isinstance(value, (Timezone, FixedTimezone)):
        return value
    if isinstance(value, datetime.tzinfo):
        # Generic tzinfo (zoneinfo, datetime.timezone): rebuild a pendulum zone from its
        # IANA name or fixed offset so pendulum's DST arithmetic gets a native zone type.
        return pendulum.timezone(serializable_timezone(value))
    return pendulum.timezone(value)


def serializable_timezone(tzinfo: datetime.tzinfo | None) -> str | int:
    """
    Encode a tzinfo as a value that round-trips through ``pendulum.timezone`` / parse_timezone.

    Named zones become their IANA name (e.g. ``Asia/Singapore``). Fixed-offset zones become
    the offset in seconds (int), which is what Airflow's own timezone serializer uses.
    UTC / zero-offset is always the string ``UTC`` for stable serialization.
    """
    if tzinfo is None:
        return "UTC"
    if isinstance(tzinfo, FixedTimezone):
        if tzinfo.offset == 0:
            return "UTC"
        return tzinfo.offset
    name = getattr(tzinfo, "name", None) or getattr(tzinfo, "key", None) or getattr(tzinfo, "zone", None)
    if name:
        if name in ("UTC", "utc", "+00:00"):
            return "UTC"
        return name
    offset = tzinfo.utcoffset(None)
    if offset is not None:
        total = int(offset.total_seconds())
        return "UTC" if total == 0 else total
    return "UTC"


def _coerce_target_time(target_time: datetime.time | str) -> datetime.time:
    """Accept ``datetime.time`` or an ISO time string (Airflow serializes time as str)."""
    if isinstance(target_time, str):
        return datetime.time.fromisoformat(target_time)
    if isinstance(target_time, datetime.time):
        # Drop tzinfo so storage / comparison is wall-clock-only; zone is separate.
        if target_time.tzinfo is not None:
            return target_time.replace(tzinfo=None)
        return target_time
    raise TypeError(f"Expected datetime.time or str for target_time. Got {type(target_time)}")


def resolve_time_of_day_moment(
    target_time: datetime.time | str,
    *,
    tz: str | int | datetime.tzinfo = "UTC",
    as_of: datetime.datetime | None = None,
) -> pendulum.DateTime:
    """
    Resolve ``target_time`` on "today" in ``tz`` to a UTC-aware moment.

    Semantics:

    - **Already passed today**: still returns today's occurrence (caller succeeds immediately).
      Does *not* roll forward to the next day.
    - **Non-existent local time** (spring-forward gap, e.g. 02:30 America/New_York on DST start):
      shifts forward to the next valid local time (e.g. 03:30).
    - **Ambiguous local time** (fall-back overlap, e.g. 01:30 on DST end): uses ``fold=0``
      (the first occurrence).
    - Moment is computed from ``as_of`` (default: now) so callers can cache per attempt and
      avoid midnight drift when re-checking within the same run.
    """
    wall_time = _coerce_target_time(target_time)
    tzinfo = _parse_timezone(tz)

    if as_of is None:
        as_of = timezone.utcnow()
    local_now = pendulum.instance(as_of).in_timezone(tzinfo)

    # pendulum.datetime shifts non-existent (gap) times forward to the next valid wall time.
    moment_local = pendulum.datetime(
        local_now.year,
        local_now.month,
        local_now.day,
        wall_time.hour,
        wall_time.minute,
        wall_time.second,
        wall_time.microsecond,
        tz=tzinfo,
    )

    # If the wall clock was preserved, check for ambiguous (fold) times and prefer fold=0.
    if (
        moment_local.hour,
        moment_local.minute,
        moment_local.second,
        moment_local.microsecond,
    ) == (
        wall_time.hour,
        wall_time.minute,
        wall_time.second,
        wall_time.microsecond,
    ):
        dt0 = datetime.datetime(
            local_now.year,
            local_now.month,
            local_now.day,
            wall_time.hour,
            wall_time.minute,
            wall_time.second,
            wall_time.microsecond,
            tzinfo=tzinfo,
            fold=0,
        )
        dt1 = dt0.replace(fold=1)
        if dt0.utcoffset() != dt1.utcoffset():
            moment_local = pendulum.instance(dt0)

    return timezone.convert_to_utc(moment_local)


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
        if moment.tzinfo is None:
            raise ValueError("You cannot pass naive datetimes")
        self.moment: pendulum.DateTime = timezone.convert_to_utc(moment)
        self.end_from_trigger = end_from_trigger

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.standard.triggers.temporal.DateTimeTrigger",
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


class TimeOfDayTrigger(BaseTrigger):
    """
    Trigger that fires once the wall-clock reaches ``target_time`` in ``timezone``.

    Unlike :class:`DateTimeTrigger`, the concrete moment is resolved when the trigger
    *starts* (not when the Dag is parsed). That keeps ``start_trigger_args`` parse-stable
    while preserving ``TimeSensor(start_from_trigger=True)``.

    ``target_time`` is stored as an ISO time string so trigger kwargs remain
    JSON/serde-safe (``datetime.time`` is not accepted by Airflow's trigger serde).

    :param target_time: wall-clock time of day (``datetime.time`` or ISO time string)
    :param timezone: IANA name (str) or fixed offset in seconds (int); must round-trip
        through ``pendulum.timezone``
    :param end_from_trigger: whether the trigger should mark the task successful after
        the time condition is reached
    """

    def __init__(
        self,
        target_time: datetime.time | str,
        *,
        timezone: str | int = "UTC",
        end_from_trigger: bool = False,
    ) -> None:
        super().__init__()
        self.target_time: str = _coerce_target_time(target_time).isoformat()
        self.timezone: str | int = timezone
        self.end_from_trigger = end_from_trigger
        # Resolved once at run() start and cached for this trigger attempt.
        self.moment: pendulum.DateTime | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.standard.triggers.temporal.TimeOfDayTrigger",
            {
                "target_time": self.target_time,
                "timezone": self.timezone,
                "end_from_trigger": self.end_from_trigger,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Resolve today's target moment, then reuse DateTimeTrigger's wait loop."""
        self.moment = resolve_time_of_day_moment(
            self.target_time,
            tz=self.timezone,
        )
        self.log.info(
            "TimeOfDayTrigger resolved target_time=%s timezone=%r -> moment=%s",
            self.target_time,
            self.timezone,
            self.moment,
        )
        # Delegate sleep/poll logic — do not duplicate the wait loop.
        delegate = DateTimeTrigger(moment=self.moment, end_from_trigger=self.end_from_trigger)
        async for event in delegate.run():
            yield event


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
