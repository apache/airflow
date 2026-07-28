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

import pytest

from airflow.sdk.definitions.timetables._cron import CronMixin
from airflow.sdk.definitions.timetables.interval import CronDataIntervalTimetable
from airflow.sdk.exceptions import AirflowTimetableInvalid

SAMPLE_TZ = "UTC"

# Static table so a typo in CRON_PRESETS is caught by the test.
PRESET_CASES = [
    ("@hourly", "0 * * * *"),
    ("@daily", "0 0 * * *"),
    ("@weekly", "0 0 * * 0"),
    ("@monthly", "0 0 1 * *"),
    ("@quarterly", "0 0 1 */3 *"),
    ("@yearly", "0 0 1 1 *"),
]


@pytest.mark.parametrize(("preset", "expected"), PRESET_CASES)
def test_cron_preset_resolved(preset, expected):
    cm = CronMixin(expression=preset, timezone=SAMPLE_TZ)
    assert cm.expression == expected


def test_cron_preset_validate_does_not_raise():
    cm = CronMixin(expression="@quarterly", timezone=SAMPLE_TZ)
    cm.validate()


def test_invalid_cron_expression_raises():
    cm = CronMixin(expression="invalid", timezone=SAMPLE_TZ)
    with pytest.raises(AirflowTimetableInvalid):
        cm.validate()


def test_valid_cron_expression_does_not_raise():
    cm = CronMixin(expression="0 0 * * *", timezone=SAMPLE_TZ)
    cm.validate()


def test_cron_data_interval_timetable_quarterly_preset():
    """Regression test for #66101: CronDataIntervalTimetable must accept @quarterly."""
    timetable = CronDataIntervalTimetable(expression="@quarterly", timezone=SAMPLE_TZ)
    assert timetable.expression == "0 0 1 */3 *"
    timetable.validate()
