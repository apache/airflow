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

from datetime import datetime, time
from unittest.mock import patch

import pendulum
from parameterized import parameterized

from airflow.models.dag import DAG
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils import timezone

DEFAULT_TIMEZONE = "Asia/Singapore"  # UTC+08:00
DEFAULT_DATE_WO_TZ = datetime(2015, 1, 1)
DEFAULT_DATE_WITH_TZ = datetime(2015, 1, 1, tzinfo=pendulum.tz.timezone(DEFAULT_TIMEZONE))


@patch(
    "airflow.sensors.time_sensor.timezone.utcnow",
    return_value=timezone.datetime(2020, 1, 1, 23, 0).replace(tzinfo=timezone.utc),
)
class TestTimeSensor:
    @parameterized.expand(
        [
            ("UTC", DEFAULT_DATE_WO_TZ, True),
            ("UTC", DEFAULT_DATE_WITH_TZ, False),
            (DEFAULT_TIMEZONE, DEFAULT_DATE_WO_TZ, False),
        ]
    )
    def test_timezone(self, mock_utcnow, default_timezone, start_date, expected):
        with patch("airflow.settings.TIMEZONE", pendulum.timezone(default_timezone)):
            dag = DAG("test", default_args={"start_date": start_date})
            op = TimeSensor(task_id="test", target_time=time(10, 0), dag=dag)
            assert op.poke(None) == expected
