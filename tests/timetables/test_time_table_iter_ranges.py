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

"""Tests for Timetable.iter_between()."""

from datetime import datetime, timedelta

import pytest

from airflow.settings import TIMEZONE
from airflow.timetables.interval import DeltaDataIntervalTimetable


@pytest.fixture()
def timetable_1s():
    return DeltaDataIntervalTimetable(timedelta(seconds=1))


def test_end_date_before_start_date(timetable_1s):
    start = datetime(2016, 2, 1, tzinfo=TIMEZONE)
    end = datetime(2016, 1, 1, tzinfo=TIMEZONE)
    message = r"start \([- :+\d]{25}\) > end \([- :+\d]{25}\)"
    with pytest.raises(ValueError, match=message):
        list(timetable_1s.iter_between(start, end, align=True))
