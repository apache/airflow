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
from unittest.mock import patch

import pendulum
import pytest
import time_machine
from pendulum.tz.timezone import UTC

from airflow.exceptions import TaskDeferred
from airflow.models.dag import DAG
from airflow.sensors.time_sensor import TimeSensor, TimeSensorAsync
from airflow.triggers.temporal import DateTimeTrigger
from airflow.utils import timezone

DEFAULT_TIMEZONE = "Asia/Singapore"  # UTC+08:00
DEFAULT_DATE_WO_TZ = datetime(2015, 1, 1)
DEFAULT_DATE_WITH_TZ = datetime(2015, 1, 1, tzinfo=pendulum.tz.timezone(DEFAULT_TIMEZONE))


class TestTimeSensor:
    @pytest.mark.parametrize(
        "default_timezone, start_date, expected",
        [
            ("UTC", DEFAULT_DATE_WO_TZ, True),
            ("UTC", DEFAULT_DATE_WITH_TZ, False),
            (DEFAULT_TIMEZONE, DEFAULT_DATE_WO_TZ, False),
        ],
    )
    @time_machine.travel(timezone.datetime(2020, 1, 1, 23, 0).replace(tzinfo=timezone.utc))
    def test_timezone(self, default_timezone, start_date, expected):
        with patch("airflow.settings.TIMEZONE", pendulum.timezone(default_timezone)):
            dag = DAG("test", default_args={"start_date": start_date})
            op = TimeSensor(task_id="test", target_time=time(10, 0), dag=dag)
            assert op.poke(None) == expected


class TestTimeSensorAsync:
    @time_machine.travel("2020-07-07 00:00:00", tick=False)
    def test_task_is_deferred(self):
        with DAG("test_task_is_deferred", start_date=timezone.datetime(2020, 1, 1, 23, 0)):
            op = TimeSensorAsync(task_id="test", target_time=time(10, 0))
        assert not timezone.is_naive(op.target_datetime)

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute({})

        assert isinstance(exc_info.value.trigger, DateTimeTrigger)
        assert exc_info.value.trigger.moment == timezone.datetime(2020, 7, 7, 10)
        assert exc_info.value.method_name == "execute_complete"
        assert exc_info.value.kwargs is None

    def test_target_time_aware(self):
        with DAG("test_target_time_aware", start_date=timezone.datetime(2020, 1, 1, 23, 0)):
            aware_time = time(0, 1).replace(tzinfo=pendulum.local_timezone())
            op = TimeSensorAsync(task_id="test", target_time=aware_time)
            assert hasattr(op.target_datetime.tzinfo, "offset")
            assert op.target_datetime.tzinfo.offset == 0

    def test_target_time_naive_dag_timezone(self):
        """
        Tests that naive target_time gets converted correctly using the DAG's timezone.
        """
        with DAG(
            "test_target_time_naive_dag_timezone",
            start_date=pendulum.datetime(2020, 1, 1, 0, 0, tz=DEFAULT_TIMEZONE),
        ):
            op = TimeSensorAsync(task_id="test", target_time=pendulum.time(9, 0))
            assert op.target_datetime.time() == pendulum.time(1, 0)
            assert op.target_datetime.tzinfo == UTC
