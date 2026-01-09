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

import datetime

import pytest
import time_machine

from airflow._shared.timezones import timezone
from airflow.timetables.base import DagRunInfo, TimeRestriction
from airflow.timetables.simple import OnceTimetable

FROZEN_NOW = timezone.coerce_datetime(datetime.datetime(2025, 3, 4, 5, 6, 7, 8))

PREVIOUS_INFO = DagRunInfo.exact(FROZEN_NOW - datetime.timedelta(days=1))


@pytest.mark.parametrize(
    ("prev_info", "end_date", "expected_next_info"),
    [
        (None, None, DagRunInfo.exact(FROZEN_NOW)),
        (None, FROZEN_NOW + datetime.timedelta(days=1), DagRunInfo.exact(FROZEN_NOW)),
        (None, FROZEN_NOW - datetime.timedelta(days=1), None),
        (PREVIOUS_INFO, None, None),
        (PREVIOUS_INFO, FROZEN_NOW + datetime.timedelta(days=1), None),
        (PREVIOUS_INFO, FROZEN_NOW - datetime.timedelta(days=1), None),
    ],
)
@pytest.mark.parametrize("catchup", [True, False])  # Irrelevant for @once.
@time_machine.travel(FROZEN_NOW)
def test_no_start_date_means_now(catchup, prev_info, end_date, expected_next_info):
    timetable = OnceTimetable()
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=prev_info,
        restriction=TimeRestriction(earliest=None, latest=end_date, catchup=catchup),
    )
    assert next_info == expected_next_info
