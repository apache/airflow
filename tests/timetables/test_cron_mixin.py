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

from pendulum import DateTime
from pendulum.tz.timezone import UTC

from airflow.timetables._cron import CronMixin


class TestCronMixin:
    def test_get_next_true(self):
        cron_mixin = CronMixin("0 7-21 * * *", timezone="Europe/Paris")
        print("\n")
        for day, hour in [
            (28, 20),
            (28, 21),
            (28, 22),
            (28, 23),
            (29, 0),
            (29, 1),
            (29, 2),
            (29, 3),
            (29, 4),
            (29, 5),
            (29, 6),
            (29, 7),
        ]:
            date = DateTime(year=2023, month=10, day=day, hour=hour, minute=0, second=0, tzinfo=UTC)
            align_prev_date = cron_mixin._align_to_prev(date)
            print(f"the {day} at {hour}: pre_date={align_prev_date}")
