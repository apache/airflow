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

import pendulum
import pytest

from airflow.sdk.definitions.timetables.events import EventsTimetable

EVENT_DATES = [
    pendulum.datetime(2025, 3, 1),
    pendulum.datetime(2025, 1, 1),
    pendulum.datetime(2025, 2, 1),
]


def test_event_dates_sorted_by_default():
    timetable = EventsTimetable(event_dates=EVENT_DATES)
    assert timetable.event_dates == sorted(EVENT_DATES)


@pytest.mark.parametrize("event_dates", [EVENT_DATES, iter(EVENT_DATES)], ids=["list", "iterator"])
def test_event_dates_kept_as_given_when_presorted(event_dates):
    timetable = EventsTimetable(event_dates=event_dates, presorted=True)
    assert timetable.event_dates == EVENT_DATES
