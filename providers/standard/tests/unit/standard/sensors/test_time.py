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

from datetime import datetime, time

import pendulum
import pytest
import time_machine

from airflow.exceptions import TaskDeferred
from airflow.models.dag import DAG
from airflow.providers.standard.sensors.time import TimeSensor
from airflow.providers.standard.triggers.temporal import DateTimeTrigger

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

DEFAULT_TIMEZONE = pendulum.timezone("Asia/Singapore")  # UTC+08:00
DEFAULT_DATE_WO_TZ = datetime(2015, 1, 1)
DEFAULT_DATE_WITH_TZ = datetime(2015, 1, 1, tzinfo=DEFAULT_TIMEZONE)


class TestTimeSensor:
    @pytest.mark.parametrize(
        ("tzinfo", "start_date", "target_time", "expected"),
        [
            (timezone.utc, DEFAULT_DATE_WO_TZ, time(10, 0), True),
            (timezone.utc, DEFAULT_DATE_WITH_TZ, time(16, 0), True),
            (timezone.utc, DEFAULT_DATE_WITH_TZ, time(23, 0), False),
            (DEFAULT_TIMEZONE, DEFAULT_DATE_WO_TZ, time(23, 0), False),
        ],
    )
    @time_machine.travel(timezone.datetime(2020, 1, 1, 13, 0).replace(tzinfo=timezone.utc))
    def test_timezone(self, tzinfo, start_date, target_time, expected, monkeypatch):
        monkeypatch.setattr("airflow.settings.TIMEZONE", tzinfo)
        dag = DAG("test_timezone", schedule=None, default_args={"start_date": start_date})
        op = TimeSensor(task_id="test", target_time=target_time, dag=dag)
        assert op.poke(None) == expected

    def test_target_time_aware_dag_timezone(self):
        # This behavior should be the same for both deferrable and non-deferrable
        with DAG("test_target_time_aware", schedule=None, start_date=datetime(2020, 1, 1, 13, 0)):
            aware_time = time(0, 1).replace(tzinfo=DEFAULT_TIMEZONE)
            op = TimeSensor(task_id="test", target_time=aware_time)
            assert op.target_datetime.tzinfo == timezone.utc

    @pytest.mark.parametrize(
        ("current_datetime", "server_timezone"),
        [
            ("2025-01-26 22:00:00", "UTC"),
            ("2025-01-27 07:00:00", "Asia/Seoul"),  # UTC+09:00
        ],
    )
    def test_target_date_aware_dag_timezone(self, current_datetime, server_timezone):
        travel_time = pendulum.parse(current_datetime, tz=pendulum.timezone(server_timezone))
        user_timezone = pendulum.timezone("Asia/Seoul")
        expected_target_datetime = pendulum.datetime(2025, 1, 26, 22, 0, 0, tz="UTC")

        with time_machine.travel(travel_time, tick=False):
            with DAG(
                "test_target_date_aware",
                schedule=None,
                start_date=datetime(2025, 1, 27, 7, tzinfo=user_timezone),
            ):
                aware_datetime = pendulum.datetime(2025, 1, 27, 7).replace(tzinfo=user_timezone)
                op = TimeSensor(task_id="test", target_time=aware_datetime.time())

                # In the old logic, if server's timezone is UTC op.target_datetime could be incorrectly.
                # For example, it might be set to `2025-01-25 22:00:00 UTC`, not `2025-01-26 22:00:00 UTC`.
                # This issue stems from using datetime.today() in an environment where the server timezone differs from the user's timezone
                # The local date '2025-01-26' is combined with the target time `07:00:00`,
                # resulting in `2025-01-26 07:00:00` in local time.
                # When this is converted to UTC by convert_to_utc, it becomes `2025-01-25 22:00:00 UTC`.
                assert op.target_datetime == expected_target_datetime

    def test_target_time_naive_dag_timezone(self):
        # Again, this behavior should be the same for both deferrable and non-deferrable
        with DAG(
            dag_id="test_target_time_naive_dag_timezone",
            schedule=None,
            start_date=datetime(2020, 1, 1, 23, 0, tzinfo=DEFAULT_TIMEZONE),
        ):
            op = TimeSensor(task_id="test", target_time=time(9, 0))

            # Since the DEFAULT_TIMEZONE is UTC+8:00, then hour 9 should be converted to hour 1
            assert op.target_datetime.time() == pendulum.time(1, 0)
            assert op.target_datetime.tzinfo == timezone.utc

    @time_machine.travel("2020-07-07 00:00:00", tick=False)
    def test_task_is_deferred(self):
        with DAG(
            dag_id="test_task_is_deferred",
            schedule=None,
            start_date=datetime(2020, 1, 1, 13, 0),
        ):
            op = TimeSensor(task_id="test", target_time=time(10, 0), deferrable=True)

        # This should be converted to UTC in the __init__. Since there is no default timezone, it will become
        # aware, but note changed
        assert op.target_datetime.utcoffset() is not None

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({})

        assert isinstance(exc_info.value.trigger, DateTimeTrigger)
        assert exc_info.value.trigger.moment == pendulum.datetime(2020, 7, 7, 10)
        assert exc_info.value.kwargs is None
        assert exc_info.value.method_name == "execute_complete"

    def test_execute_complete_accepts_event(self):
        """Ensure execute_complete supports the 'event' kwarg when deferrable=True."""
        with DAG(
            dag_id="test_execute_complete_accepts_event",
            schedule=None,
            start_date=datetime(2020, 1, 1),  # Matches above
        ):
            op = TimeSensor(task_id="test", target_time=time(10, 0), deferrable=True)

        try:
            op.execute_complete(context={}, event={"status": "success"})
        except TypeError as e:
            pytest.fail(f"TypeError raised: {e}")
