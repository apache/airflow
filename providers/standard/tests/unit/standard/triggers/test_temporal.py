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
from unittest import mock

import pendulum
import pytest

from airflow.providers.common.compat.sdk import timezone
from airflow.providers.standard.triggers.temporal import (
    DateTimeTrigger,
    TimeDeltaTrigger,
    TimeOfDayTrigger,
    resolve_time_of_day_moment,
    serializable_timezone,
)
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import TaskInstanceState

# Bound at import time so tests patching ``temporal.timezone.utcnow`` still read a real clock here.
utcnow = timezone.utcnow


def test_input_validation():
    """
    Tests that the DateTimeTrigger validates input to moment arg, it should only accept datetime.
    """
    with pytest.raises(TypeError, match="Expected datetime.datetime type for moment. Got <class 'str'>"):
        DateTimeTrigger("2012-01-01T03:03:03+00:00")


def test_input_validation_tz():
    """
    Tests that the DateTimeTrigger validates input to moment arg, it shouldn't accept naive datetime.
    """

    moment = datetime.datetime(2013, 3, 31, 0, 59, 59)
    with pytest.raises(ValueError, match="You cannot pass naive datetimes"):
        DateTimeTrigger(moment)


def test_datetime_trigger_serialization():
    """
    Tests that the DateTimeTrigger correctly serializes its arguments
    and classpath.
    """
    moment = pendulum.instance(datetime.datetime(2020, 4, 1, 13, 0), pendulum.UTC)
    trigger = DateTimeTrigger(moment)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.standard.triggers.temporal.DateTimeTrigger"
    assert kwargs == {"moment": moment, "end_from_trigger": False}


def test_timedelta_trigger_serialization():
    """
    Tests that the TimeDeltaTrigger correctly serializes its arguments
    and classpath (it turns into a DateTimeTrigger).
    """
    trigger = TimeDeltaTrigger(datetime.timedelta(seconds=10))
    expected_moment = timezone.utcnow() + datetime.timedelta(seconds=10)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.standard.triggers.temporal.DateTimeTrigger"
    # We need to allow for a little time difference to avoid this test being
    # flaky if it runs over the boundary of a single second
    assert -2 < (kwargs["moment"] - expected_moment).total_seconds() < 2


@pytest.mark.parametrize(
    ("tz", "end_from_trigger"),
    [
        (pendulum.timezone("UTC"), True),
        (pendulum.timezone("UTC"), False),  # only really need to test one
        (pendulum.timezone("Europe/Paris"), True),
        (pendulum.timezone("America/Toronto"), True),
    ],
)
@pytest.mark.asyncio
async def test_datetime_trigger_timing_airflow_2_10_plus(tz, end_from_trigger):
    """
    Tests that the DateTimeTrigger only goes off on or after the appropriate
    time.
    """
    past_moment = pendulum.instance((timezone.utcnow() - datetime.timedelta(seconds=60)).astimezone(tz))
    future_moment = pendulum.instance((timezone.utcnow() + datetime.timedelta(seconds=60)).astimezone(tz))

    # Create a task that runs the trigger for a short time then cancels it
    trigger = DateTimeTrigger(future_moment, end_from_trigger=end_from_trigger)
    trigger_task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # It should not have produced a result
    assert trigger_task.done() is False
    trigger_task.cancel()

    # Now, make one waiting for en event in the past and do it again
    trigger = DateTimeTrigger(past_moment, end_from_trigger=end_from_trigger)
    trigger_task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    assert trigger_task.done() is True
    result = trigger_task.result()
    assert isinstance(result, TriggerEvent)
    expected_payload = TaskInstanceState.SUCCESS if end_from_trigger else past_moment
    assert result.payload == expected_payload


@mock.patch("airflow.providers.standard.triggers.temporal.timezone.utcnow")
@mock.patch("airflow.providers.standard.triggers.temporal.asyncio.sleep")
@pytest.mark.asyncio
async def test_datetime_trigger_mocked(mock_sleep, mock_utcnow):
    """
    Tests DateTimeTrigger with time and asyncio mocks
    """
    start_moment = utcnow()
    trigger_moment = start_moment + datetime.timedelta(seconds=30)

    # returns the mock 'current time'. The first 3 calls report the initial time
    mock_utcnow.side_effect = [
        start_moment,
        start_moment,
        start_moment,
        start_moment + datetime.timedelta(seconds=20),
        start_moment + datetime.timedelta(seconds=25),
        start_moment + datetime.timedelta(seconds=30),
    ]

    trigger = DateTimeTrigger(trigger_moment)
    gen = trigger.run()
    trigger_task = asyncio.create_task(gen.__anext__())
    await trigger_task
    mock_sleep.assert_awaited()
    assert mock_sleep.await_count == 2
    assert trigger_task.done() is True
    result = trigger_task.result()
    assert isinstance(result, TriggerEvent)
    assert result.payload == trigger_moment


def test_time_of_day_trigger_serialization():
    trigger = TimeOfDayTrigger(
        target_time=datetime.time(10, 30, 45),
        timezone="Asia/Singapore",
        end_from_trigger=True,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.standard.triggers.temporal.TimeOfDayTrigger"
    assert kwargs == {
        "target_time": "10:30:45",
        "timezone": "Asia/Singapore",
        "end_from_trigger": True,
    }
    # Accepts ISO string (post-serde form)
    restored = TimeOfDayTrigger(**kwargs)
    assert restored.serialize() == (classpath, kwargs)


def test_time_of_day_trigger_fixed_offset_timezone():
    trigger = TimeOfDayTrigger(target_time="07:00:00", timezone=19800, end_from_trigger=False)
    _, kwargs = trigger.serialize()
    assert kwargs["timezone"] == 19800
    assert kwargs["target_time"] == "07:00:00"


def test_resolve_time_of_day_moment_already_passed():
    as_of = pendulum.datetime(2020, 7, 7, 15, 0, tz="UTC")
    moment = resolve_time_of_day_moment(datetime.time(10, 0), tz="UTC", as_of=as_of)
    assert moment == pendulum.datetime(2020, 7, 7, 10, 0, tz="UTC")


def test_resolve_time_of_day_moment_dst_spring_forward():
    as_of = pendulum.datetime(2024, 3, 10, 12, 0, tz="UTC")
    moment = resolve_time_of_day_moment(datetime.time(2, 30), tz="America/New_York", as_of=as_of)
    # Gap → shift forward to 03:30 EDT = 07:30 UTC
    assert moment == pendulum.datetime(2024, 3, 10, 7, 30, tz="UTC")


def test_resolve_time_of_day_moment_dst_fall_back_fold_zero():
    as_of = pendulum.datetime(2024, 11, 3, 12, 0, tz="UTC")
    moment = resolve_time_of_day_moment(datetime.time(1, 30), tz="America/New_York", as_of=as_of)
    # fold=0 → first occurrence (EDT, UTC-4) = 05:30 UTC
    assert moment == pendulum.datetime(2024, 11, 3, 5, 30, tz="UTC")


def test_serializable_timezone_named_and_fixed():
    assert serializable_timezone(pendulum.timezone("UTC")) == "UTC"
    assert serializable_timezone(pendulum.timezone("Asia/Singapore")) == "Asia/Singapore"
    assert serializable_timezone(pendulum.tz.timezone.FixedTimezone(19800)) == 19800
    assert serializable_timezone(None) == "UTC"


@pytest.mark.asyncio
async def test_time_of_day_trigger_fires_for_past_target():
    """If today's target is already past, the trigger should fire immediately."""
    past = (timezone.utcnow() - datetime.timedelta(hours=1)).time().replace(microsecond=0)
    trigger = TimeOfDayTrigger(target_time=past, timezone="UTC", end_from_trigger=False)
    trigger_task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)
    assert trigger_task.done() is True
    result = trigger_task.result()
    assert isinstance(result, TriggerEvent)
