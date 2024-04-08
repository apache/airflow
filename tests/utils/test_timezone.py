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

import pendulum
import pytest
from pendulum.tz.timezone import Timezone

from airflow.utils import timezone
from airflow.utils.timezone import coerce_datetime, parse_timezone

CET = Timezone("Europe/Paris")
EAT = Timezone("Africa/Nairobi")  # Africa/Nairobi
ICT = Timezone("Asia/Bangkok")  # Asia/Bangkok
UTC = timezone.utc


class TestTimezone:
    def test_is_aware(self):
        assert timezone.is_localized(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT))
        assert not timezone.is_localized(datetime.datetime(2011, 9, 1, 13, 20, 30))

    def test_is_naive(self):
        assert not timezone.is_naive(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT))
        assert timezone.is_naive(datetime.datetime(2011, 9, 1, 13, 20, 30))

    def test_utcnow(self):
        now = timezone.utcnow()
        assert timezone.is_localized(now)
        assert now.replace(tzinfo=None) == now.astimezone(UTC).replace(tzinfo=None)

    def test_convert_to_utc(self):
        naive = datetime.datetime(2011, 9, 1, 13, 20, 30)
        utc = datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=UTC)
        assert utc == timezone.convert_to_utc(naive)

        eat = datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT)
        utc = datetime.datetime(2011, 9, 1, 10, 20, 30, tzinfo=UTC)
        assert utc == timezone.convert_to_utc(eat)

    def test_make_naive(self):
        assert timezone.make_naive(
            datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT), EAT
        ) == datetime.datetime(2011, 9, 1, 13, 20, 30)
        assert timezone.make_naive(
            datetime.datetime(2011, 9, 1, 17, 20, 30, tzinfo=ICT), EAT
        ) == datetime.datetime(2011, 9, 1, 13, 20, 30)

        with pytest.raises(ValueError):
            timezone.make_naive(datetime.datetime(2011, 9, 1, 13, 20, 30), EAT)

    def test_make_aware(self):
        assert timezone.make_aware(datetime.datetime(2011, 9, 1, 13, 20, 30), EAT) == datetime.datetime(
            2011, 9, 1, 13, 20, 30, tzinfo=EAT
        )
        with pytest.raises(ValueError):
            timezone.make_aware(datetime.datetime(2011, 9, 1, 13, 20, 30, tzinfo=EAT), EAT)

    def test_td_format(self):
        td = datetime.timedelta(seconds=3752)
        assert timezone.td_format(td) == "1h:2M:32s"
        td = 3200.0
        assert timezone.td_format(td) == "53M:20s"
        td = 3200
        assert timezone.td_format(td) == "53M:20s"
        td = 0.123
        assert timezone.td_format(td) == "<1s"
        td = None
        assert timezone.td_format(td) is None
        td = datetime.timedelta(seconds=300752)
        assert timezone.td_format(td) == "3d:11h:32M:32s"
        td = 434343600.0
        assert timezone.td_format(td) == "13y:11m:17d:3h"


@pytest.mark.parametrize(
    "input_datetime, output_datetime",
    [
        pytest.param(None, None, id="None datetime"),
        pytest.param(
            pendulum.DateTime(2021, 11, 1),
            pendulum.DateTime(2021, 11, 1, tzinfo=UTC),
            id="Non aware pendulum Datetime",
        ),
        pytest.param(
            pendulum.DateTime(2021, 11, 1, tzinfo=CET),
            pendulum.DateTime(2021, 11, 1, tzinfo=CET),
            id="Aware pendulum Datetime",
        ),
        pytest.param(
            datetime.datetime(2021, 11, 1),
            pendulum.DateTime(2021, 11, 1, tzinfo=UTC),
            id="Non aware datetime",
        ),
        pytest.param(
            datetime.datetime(2021, 11, 1, tzinfo=CET),
            pendulum.DateTime(2021, 11, 1, tzinfo=CET),
            id="Aware datetime",
        ),
    ],
)
def test_coerce_datetime(input_datetime, output_datetime):
    assert output_datetime == coerce_datetime(input_datetime)


@pytest.mark.parametrize(
    "tz_name",
    [
        pytest.param("Europe/Paris", id="CET"),
        pytest.param("Africa/Nairobi", id="EAT"),
        pytest.param("Asia/Bangkok", id="ICT"),
    ],
)
def test_parse_timezone_iana(tz_name: str):
    tz = parse_timezone(tz_name)
    assert tz.name == tz_name
    assert parse_timezone(tz_name) is tz


@pytest.mark.parametrize("tz_name", ["utc", "UTC", "uTc"])
def test_parse_timezone_utc(tz_name):
    tz = parse_timezone(tz_name)
    assert tz.name == "UTC"
    assert parse_timezone(tz_name) is tz
    assert tz is timezone.utc, "Expected that UTC timezone is same object as `airflow.utils.timezone.utc`"


@pytest.mark.parametrize(
    "tz_offset, expected_offset, expected_name",
    [
        pytest.param(0, 0, "+00:00", id="zero-offset"),
        pytest.param(-3600, -3600, "-01:00", id="1-hour-behind"),
        pytest.param(19800, 19800, "+05:30", id="5.5-hours-ahead"),
    ],
)
def test_parse_timezone_offset(tz_offset: int, expected_offset, expected_name):
    tz = parse_timezone(tz_offset)
    assert hasattr(tz, "offset")
    assert tz.offset == expected_offset
    assert tz.name == expected_name
    assert parse_timezone(tz_offset) is tz
