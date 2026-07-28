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
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
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


def test_requires_moment_or_target_time():
    """DateTimeTrigger must be given exactly one of moment or target_time."""
    with pytest.raises(TypeError, match="requires either 'moment' or 'target_time'"):
        DateTimeTrigger()


def test_rejects_both_moment_and_target_time():
    moment = pendulum.instance(datetime.datetime(2020, 4, 1, 13, 0), pendulum.UTC)
    with pytest.raises(TypeError, match="only one of 'moment' or 'target_time'"):
        DateTimeTrigger(moment, target_time="2020-04-01T13:00:00+00:00")


def test_target_time_accepted_unrendered_at_init():
    """
    Regression test for #70284: constructing the trigger with a still-templated target_time
    (as happens on the triggerer, before BaseTrigger.render_template_fields runs) must not raise.
    """
    trigger = DateTimeTrigger(target_time="{{ data_interval_end.tomorrow().replace(hour=1) }}")
    assert trigger.moment is None
    assert trigger.target_time == "{{ data_interval_end.tomorrow().replace(hour=1) }}"


def test_target_time_resolved_after_rendering():
    """Once target_time has been rendered (e.g. by the triggerer) to a real value, it parses fine."""
    trigger = DateTimeTrigger(target_time="not-rendered-yet")
    # Simulate what BaseTrigger.render_template_fields does: it renders the template in place.
    trigger.target_time = "2020-04-01T13:00:00+00:00"
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.standard.triggers.temporal.DateTimeTrigger"
    assert kwargs == {"moment": pendulum.parse("2020-04-01T13:00:00+00:00"), "end_from_trigger": False}


def test_target_time_still_templated_raises_clear_error():
    """If target_time is still an unrendered template by the time it's needed, fail clearly."""
    trigger = DateTimeTrigger(target_time="{{ this_was_never_rendered }}")
    with pytest.raises(ValueError, match="Could not parse target_time"):
        trigger.serialize()


@pytest.mark.asyncio
async def test_run_resolves_target_time_rendered_by_triggerer():
    """
    End-to-end-ish regression test for #70284: a DateTimeTrigger built from a templated
    target_time, then rendered the way the triggerer renders it (BaseTrigger.render_template_fields
    -- see airflow.jobs.triggerer_job_runner.TriggererJobRunner.run_trigger), must resolve to the
    correct moment and fire, instead of crashing.
    """
    past_moment = pendulum.instance(timezone.utcnow() - datetime.timedelta(seconds=60))
    trigger = DateTimeTrigger(target_time="{{ rendered_moment }}")
    # BaseTrigger.task_instance's setter is what normally populates template_fields; set it
    # directly here to isolate the rendering behavior under test.
    trigger.template_fields = ("target_time",)
    trigger.render_template_fields({"rendered_moment": past_moment.isoformat()})

    trigger_task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    assert trigger_task.done() is True
    result = trigger_task.result()
    assert isinstance(result, TriggerEvent)
    assert result.payload == past_moment
