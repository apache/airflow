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

from datetime import datetime

import pytest

from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.temporal import (
    StartOfDayMapper,
    StartOfHourMapper,
    StartOfMonthMapper,
    StartOfQuarterMapper,
    StartOfWeekMapper,
    StartOfYearMapper,
)
from airflow.partition_mappers.window import (
    DayWindow,
    HourWindow,
    MonthWindow,
    QuarterWindow,
    WeekWindow,
    YearWindow,
)


class TestHourWindow:
    def test_yields_60_minute_period_starts(self):
        period_start = datetime(2024, 6, 10, 14)
        members = list(HourWindow().to_upstream(period_start))
        assert members == [datetime(2024, 6, 10, 14, m) for m in range(60)]


class TestDayWindow:
    def test_yields_24_hourly_period_starts(self):
        period_start = datetime(2024, 6, 10)
        members = list(DayWindow().to_upstream(period_start))
        assert members == [datetime(2024, 6, 10, h) for h in range(24)]

    def test_day_window_yields_24_naive_steps_regardless_of_dst(self):
        """DayWindow always yields exactly 24 naive steps; it does not know about DST."""
        # 2024-03-10: US Eastern spring-forward (clocks skip 02:00 → 03:00).
        spring_forward = datetime(2024, 3, 10)
        assert list(DayWindow().to_upstream(spring_forward)) == [datetime(2024, 3, 10, h) for h in range(24)]

        # 2024-11-03: US Eastern fall-back (clocks repeat 01:00 → 01:00).
        fall_back = datetime(2024, 11, 3)
        assert list(DayWindow().to_upstream(fall_back)) == [datetime(2024, 11, 3, h) for h in range(24)]

    @pytest.mark.xfail(
        reason=(
            "DayWindow with a local-timezone upstream mapper cannot satisfy the rollup on a "
            "spring-forward day: the 02:00 ET gap causes one naive step to encode to "
            "03:00 ET, which upstream producers never emit. This is an accepted limitation "
            "of naive 24-hour stepping — the documented mitigation is to use UTC input_format "
            "so local-clock ambiguity never arises. See DayWindow's docstring."
        ),
        strict=True,
    )
    def test_day_window_rollup_under_yields_on_spring_forward_with_local_tz(self):
        """
        Rollup with a local-timezone upstream mapper is unsatisfiable on spring-forward days.

        DayWindow generates 24 naive steps. StartOfDayMapper(timezone="America/New_York")
        encodes each step into a local-time key. On 2024-03-10 (spring-forward), 02:00
        local time does not exist — the encoder produces "2024-03-10T03" for *two*
        consecutive steps. One expected upstream key ("2024-03-10T02") is never emitted
        by any real producer, so to_upstream returns a frozenset of 23 *distinct* keys.
        The rollup window requires all 24 and therefore can never be satisfied.
        """
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(timezone="America/New_York", input_format="%Y-%m-%dT%H"),
            window=DayWindow(),
        )
        upstream_keys = mapper.to_upstream("2024-03-10")
        # A correctly functioning rollup would expect exactly 24 distinct upstream keys.
        # On spring-forward this assertion fails because only 23 distinct keys are produced.
        assert len(upstream_keys) == 24


class TestWeekWindow:
    def test_yields_seven_daily_period_starts(self):
        # 2024-06-10 is a Monday.
        period_start = datetime(2024, 6, 10)
        members = list(WeekWindow().to_upstream(period_start))
        assert members == [
            datetime(2024, 6, 10),
            datetime(2024, 6, 11),
            datetime(2024, 6, 12),
            datetime(2024, 6, 13),
            datetime(2024, 6, 14),
            datetime(2024, 6, 15),
            datetime(2024, 6, 16),
        ]


class TestMonthWindow:
    def test_yields_all_days_in_february_leap_year(self):
        members = list(MonthWindow().to_upstream(datetime(2024, 2, 1)))
        assert members == [datetime(2024, 2, d) for d in range(1, 30)]

    def test_crosses_year_boundary(self):
        members = list(MonthWindow().to_upstream(datetime(2024, 12, 1)))
        assert members == [datetime(2024, 12, d) for d in range(1, 32)]

    def test_rejects_non_day_one_input(self):
        # A custom decode_downstream that yields Jan 31 would crash month
        # arithmetic later; surface the contract violation explicitly instead.
        with pytest.raises(ValueError, match="expects a period start on day 1"):
            list(MonthWindow().to_upstream(datetime(2024, 1, 31)))


class TestQuarterWindow:
    def test_yields_three_monthly_period_starts(self):
        # Q2 starts at 2024-04-01.
        members = list(QuarterWindow().to_upstream(datetime(2024, 4, 1)))
        assert members == [datetime(2024, 4, 1), datetime(2024, 5, 1), datetime(2024, 6, 1)]

    def test_rejects_non_day_one_input(self):
        with pytest.raises(ValueError, match="expects a period start on day 1"):
            list(QuarterWindow().to_upstream(datetime(2024, 1, 31)))


class TestYearWindow:
    def test_yields_twelve_monthly_period_starts(self):
        members = list(YearWindow().to_upstream(datetime(2024, 1, 1)))
        assert members == [datetime(2024, m, 1) for m in range(1, 13)]

    def test_rejects_non_day_one_input(self):
        with pytest.raises(ValueError, match="expects a period start on day 1"):
            list(YearWindow().to_upstream(datetime(2024, 1, 31)))


class TestRollupMapperComposition:
    def test_to_downstream_delegates_to_upstream_mapper(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(),
            window=WeekWindow(),
        )
        # Wednesday 2024-06-12 belongs to the week starting Monday 2024-06-10.
        assert mapper.to_downstream("2024-06-12T14:30:00") == "2024-06-10 (W24)"

    def test_is_rollup_flag(self):
        mapper = RollupMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        assert mapper.is_rollup is True

    def test_hour_rollup_to_upstream_keys(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfHourMapper(input_format="%Y-%m-%dT%H:%M", output_format="%Y-%m-%dT%H"),
            window=HourWindow(),
        )
        upstream = mapper.to_upstream("2024-06-10T14")
        assert upstream == frozenset(f"2024-06-10T14:{m:02d}" for m in range(60))

    def test_day_rollup_to_upstream_keys(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(input_format="%Y-%m-%dT%H", output_format="%Y-%m-%d"),
            window=DayWindow(),
        )
        upstream = mapper.to_upstream("2024-06-10")
        assert upstream == frozenset(f"2024-06-10T{h:02d}" for h in range(24))

    def test_day_rollup_honours_timezone_in_encode(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(
                input_format="%Y-%m-%dT%H%z",
                output_format="%Y-%m-%d",
                timezone="Asia/Taipei",
            ),
            window=DayWindow(),
        )
        upstream = mapper.to_upstream("2024-06-10")
        assert upstream == frozenset(f"2024-06-10T{h:02d}+0800" for h in range(24))

    def test_week_rollup_with_default_format(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
            window=WeekWindow(),
        )
        upstream = mapper.to_upstream("2024-06-10")
        assert upstream == frozenset(
            {
                "2024-06-10",
                "2024-06-11",
                "2024-06-12",
                "2024-06-13",
                "2024-06-14",
                "2024-06-15",
                "2024-06-16",
            }
        )

    def test_week_rollup_accepts_custom_output_format(self):
        # decode_downstream lives on the mapper, so any format embedding the date works.
        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(
                input_format="%Y-%m-%d",
                output_format="Week-of-%Y-%m-%d",
            ),
            window=WeekWindow(),
        )
        upstream = mapper.to_upstream("Week-of-2024-06-10")
        assert upstream == frozenset(f"2024-06-{10 + i:02d}" for i in range(7))

    def test_week_rollup_raises_when_downstream_key_has_no_date(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d"),
            window=WeekWindow(),
        )
        with pytest.raises(ValueError, match="StartOfWeekMapper.decode_downstream could not parse"):
            mapper.to_upstream("week-24")

    def test_quarter_rollup_to_upstream_keys(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfQuarterMapper(input_format="%Y-%m"),
            window=QuarterWindow(),
        )
        assert mapper.to_upstream("2024-Q2") == frozenset({"2024-04", "2024-05", "2024-06"})

    def test_quarter_rollup_raises_when_marker_missing(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfQuarterMapper(input_format="%Y-%m"),
            window=QuarterWindow(),
        )
        with pytest.raises(ValueError, match="StartOfQuarterMapper.decode_downstream could not parse"):
            mapper.to_upstream("2024-06")

    def test_quarter_rollup_accepts_reordered_output_format(self):
        # ``{quarter}`` and ``%Y`` can appear in any order; the format-derived
        # regex pulls out both via named groups.
        mapper = RollupMapper(
            upstream_mapper=StartOfQuarterMapper(input_format="%Y-%m", output_format="Q{quarter}/%Y"),
            window=QuarterWindow(),
        )
        assert mapper.to_upstream("Q2/2024") == frozenset({"2024-04", "2024-05", "2024-06"})

    def test_year_rollup_to_upstream_keys(self):
        mapper = RollupMapper(
            upstream_mapper=StartOfYearMapper(input_format="%Y-%m"),
            window=YearWindow(),
        )
        assert mapper.to_upstream("2024") == frozenset(f"2024-{m:02d}" for m in range(1, 13))

    def test_serialize_round_trip(self):
        from airflow.serialization.decoders import decode_partition_mapper
        from airflow.serialization.encoders import encode_partition_mapper

        mapper = RollupMapper(
            upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d", output_format="%Y-%m-%d"),
            window=WeekWindow(),
        )
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert isinstance(restored.upstream_mapper, StartOfWeekMapper)
        assert isinstance(restored.window, WeekWindow)
        assert restored.to_upstream("2024-06-10") == mapper.to_upstream("2024-06-10")

    @pytest.mark.parametrize(
        ("upstream_factory", "window", "downstream_key"),
        [
            pytest.param(
                lambda: StartOfHourMapper(input_format="%Y-%m-%dT%H:%M", output_format="%Y-%m-%dT%H"),
                HourWindow(),
                "2024-06-10T14",
                id="hour",
            ),
            pytest.param(
                lambda: StartOfDayMapper(input_format="%Y-%m-%dT%H", output_format="%Y-%m-%d"),
                DayWindow(),
                "2024-06-10",
                id="day",
            ),
            pytest.param(
                lambda: StartOfQuarterMapper(input_format="%Y-%m"),
                QuarterWindow(),
                "2024-Q2",
                id="quarter",
            ),
            pytest.param(
                lambda: StartOfYearMapper(input_format="%Y-%m"),
                YearWindow(),
                "2024",
                id="year",
            ),
            pytest.param(
                lambda: StartOfMonthMapper(input_format="%Y-%m-%d"),
                MonthWindow(),
                "2024-06",
                id="month",
            ),
        ],
    )
    def test_window_serialize_round_trip(self, upstream_factory, window, downstream_key):
        from airflow.serialization.decoders import decode_partition_mapper
        from airflow.serialization.encoders import encode_partition_mapper

        mapper = RollupMapper(upstream_mapper=upstream_factory(), window=window)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert isinstance(restored.window, type(window))
        assert restored.to_upstream(downstream_key) == mapper.to_upstream(downstream_key)


class TestWindowSerializationGate:
    """``encode_window`` / ``decode_window`` must reject non-built-in Windows.

    Custom Window subclasses are not supported: a tampered serialized Dag
    could otherwise name any importable class and have the scheduler
    ``import_string`` it during deserialization.
    """

    def test_encode_rejects_custom_window_subclass(self):
        from airflow.partition_mappers.window import Window
        from airflow.serialization.encoders import encode_window
        from airflow.serialization.helpers import WindowNotSupported

        class CustomWindow(Window):
            def to_upstream(self, decoded_downstream):
                return ()

        with pytest.raises(WindowNotSupported, match="CustomWindow"):
            encode_window(CustomWindow())

    def test_decode_rejects_non_core_import_path(self):
        from airflow.serialization.decoders import decode_window
        from airflow.serialization.enums import Encoding
        from airflow.serialization.helpers import WindowNotSupported

        with pytest.raises(WindowNotSupported, match="os.system"):
            decode_window({Encoding.TYPE: "os.system", Encoding.VAR: {}})
