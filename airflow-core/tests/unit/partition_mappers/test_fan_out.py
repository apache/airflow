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

from unittest.mock import MagicMock

import pytest

from airflow.partition_mappers.base import PartitionMapper
from airflow.partition_mappers.temporal import (
    FanOutMapper,
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
    Window,
    YearWindow,
)
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper
from airflow.serialization.enums import Encoding


class TestFanOutMapper:
    def test_week_to_seven_daily_keys(self):
        """A weekly upstream key produces seven consecutive daily keys."""
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        # 2024-01-15 is a Monday, so it's already the week start under the default ISO week.
        result = list(mapper.to_downstream("2024-01-15T00:00:00"))
        assert result == [
            "2024-01-15",
            "2024-01-16",
            "2024-01-17",
            "2024-01-18",
            "2024-01-19",
            "2024-01-20",
            "2024-01-21",
        ]

    def test_normalises_mid_week_upstream_to_week_start(self):
        """An upstream timestamp on Wednesday still yields the seven days from Monday."""
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        result = list(mapper.to_downstream("2024-01-17T13:42:00"))
        assert result == [
            "2024-01-15",
            "2024-01-16",
            "2024-01-17",
            "2024-01-18",
            "2024-01-19",
            "2024-01-20",
            "2024-01-21",
        ]

    def test_day_to_24_hourly_keys(self):
        """A daily upstream key produces 24 consecutive hourly keys."""
        mapper = FanOutMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow())
        result = list(mapper.to_downstream("2024-01-15T03:30:00"))
        assert result == [f"2024-01-15T{hour:02d}" for hour in range(24)]

    def test_month_to_daily_keys(self):
        """A monthly upstream key produces one downstream key per day of the month."""
        mapper = FanOutMapper(upstream_mapper=StartOfMonthMapper(), window=MonthWindow())
        result = list(mapper.to_downstream("2024-02-10T00:00:00"))
        # February 2024 has 29 days (leap year).
        assert result == [f"2024-02-{day:02d}" for day in range(1, 30)]

    def test_quarter_to_three_monthly_keys(self):
        """A quarterly upstream key produces three monthly keys."""
        mapper = FanOutMapper(upstream_mapper=StartOfQuarterMapper(), window=QuarterWindow())
        result = list(mapper.to_downstream("2024-02-10T00:00:00"))
        assert result == ["2024-01", "2024-02", "2024-03"]

    def test_year_to_twelve_monthly_keys(self):
        """A yearly upstream key produces twelve monthly keys."""
        mapper = FanOutMapper(upstream_mapper=StartOfYearMapper(), window=YearWindow())
        result = list(mapper.to_downstream("2024-07-04T00:00:00"))
        assert result == [f"2024-{month:02d}" for month in range(1, 13)]

    @pytest.mark.parametrize(
        ("window", "expected_cls"),
        [
            (WeekWindow(), StartOfDayMapper),
            (MonthWindow(), StartOfDayMapper),
            (DayWindow(), StartOfHourMapper),
            (QuarterWindow(), StartOfMonthMapper),
            (YearWindow(), StartOfMonthMapper),
        ],
    )
    def test_default_downstream_mapper_resolution(self, window, expected_cls):
        """Each window class resolves to a sensible default downstream mapper."""
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=window)
        assert isinstance(mapper.downstream_mapper, expected_cls)

    def test_no_default_for_hour_window_requires_explicit_downstream_mapper(self):
        """HourWindow has no default downstream_mapper (no minute-level mapper exists)."""
        with pytest.raises(ValueError, match="HourWindow"):
            FanOutMapper(upstream_mapper=StartOfDayMapper(), window=HourWindow())

    def test_explicit_downstream_mapper_overrides_default(self):
        """Passing downstream_mapper explicitly overrides the lookup."""
        custom_downstream = StartOfDayMapper(output_format="%Y/%m/%d")
        mapper = FanOutMapper(
            upstream_mapper=StartOfWeekMapper(),
            window=WeekWindow(),
            downstream_mapper=custom_downstream,
        )
        result = list(mapper.to_downstream("2024-01-15T00:00:00"))
        assert result == [
            "2024/01/15",
            "2024/01/16",
            "2024/01/17",
            "2024/01/18",
            "2024/01/19",
            "2024/01/20",
            "2024/01/21",
        ]

    def test_non_temporal_downstream_mapper_falls_back_to_str(self):
        """A downstream_mapper without ``format`` falls back to the isoformat middle layer.

        Documents the contract in ``_format_with``: temporal mappers use
        ``format(dt)`` (layer 1); a mapper without ``format`` but with datetime
        values uses ``isoformat()`` (layer 2). Pinning this guards against a
        silent regression that skips the isoformat layer and falls to bare
        ``str(decoded)``.
        """
        # ``spec=PartitionMapper`` ensures the mock has no ``format`` attribute,
        # which is the trigger for the ``str(decoded)`` fallback.
        downstream = MagicMock(spec=PartitionMapper)
        mapper = FanOutMapper(
            upstream_mapper=StartOfWeekMapper(),
            window=WeekWindow(),
            downstream_mapper=downstream,
        )
        result = list(mapper.to_downstream("2024-01-15T00:00:00"))
        # WeekWindow yields 7 naive datetimes; without ``format`` each is rendered via ``isoformat()``.
        assert result == [f"2024-01-{day:02d}T00:00:00" for day in range(15, 22)]

    def test_chained_fan_out_upstream_rejected(self):
        """An upstream_mapper that itself returns multiple keys is not supported."""
        # FanOutMapper's upstream_mapper must produce a single coarse key, so
        # nesting one FanOutMapper inside another is rejected with a clear
        # error rather than silently producing wrong output.
        outer = FanOutMapper(
            upstream_mapper=FanOutMapper(
                upstream_mapper=StartOfMonthMapper(),
                window=MonthWindow(),
            ),
            window=DayWindow(),
            downstream_mapper=StartOfHourMapper(),
        )
        with pytest.raises(
            TypeError,
            match=(
                r"FanOutMapper\.upstream_mapper must produce a single key from to_downstream; "
                r"chained fan-out \(mapper that itself returns multiple keys\) is not supported\."
            ),
        ):
            list(outer.to_downstream("2024-02-10T00:00:00"))

    def test_serialize_roundtrip(self):
        """Serialize + deserialize reconstructs an equivalent mapper (explicit downstream mapper)."""
        mapper = FanOutMapper(
            upstream_mapper=StartOfWeekMapper(timezone="UTC"),
            window=WeekWindow(),
            downstream_mapper=StartOfDayMapper(output_format="%Y/%m/%d"),
        )
        data = mapper.serialize()
        restored = FanOutMapper.deserialize(data)
        assert isinstance(restored, FanOutMapper)
        assert isinstance(restored.upstream_mapper, StartOfWeekMapper)
        assert isinstance(restored.window, WeekWindow)
        assert isinstance(restored.downstream_mapper, StartOfDayMapper)
        assert restored.downstream_mapper.output_format == "%Y/%m/%d"
        # Behavior round-trips too.
        assert list(restored.to_downstream("2024-01-15T00:00:00")) == list(
            mapper.to_downstream("2024-01-15T00:00:00")
        )

    @pytest.mark.parametrize(
        ("upstream_factory", "window_cls", "expected_downstream_cls", "upstream_key"),
        [
            pytest.param(StartOfDayMapper, DayWindow, StartOfHourMapper, "2024-01-15T00:00:00", id="day"),
            pytest.param(StartOfWeekMapper, WeekWindow, StartOfDayMapper, "2024-01-15T00:00:00", id="week"),
            pytest.param(
                StartOfMonthMapper, MonthWindow, StartOfDayMapper, "2024-02-10T00:00:00", id="month"
            ),
            pytest.param(
                StartOfQuarterMapper,
                QuarterWindow,
                StartOfMonthMapper,
                "2024-02-10T00:00:00",
                id="quarter",
            ),
            pytest.param(StartOfYearMapper, YearWindow, StartOfMonthMapper, "2024-07-04T00:00:00", id="year"),
        ],
    )
    def test_serialize_roundtrip_default_downstream_mapper_all_windows(
        self, upstream_factory, window_cls, expected_downstream_cls, upstream_key
    ):
        """The auto-resolved default downstream mapper survives encode → decode for every supported window.

        The default-table lookup happens in ``__init__`` (so the restored
        mapper carries a concrete ``downstream_mapper`` instance, not a
        sentinel). Round-tripping the serialized blob must reconstruct the
        same class on the downstream side and produce byte-identical
        ``to_downstream`` output.
        """
        mapper = FanOutMapper(upstream_mapper=upstream_factory(), window=window_cls())
        restored = FanOutMapper.deserialize(mapper.serialize())

        assert isinstance(restored, FanOutMapper)
        assert isinstance(restored.upstream_mapper, upstream_factory)
        assert isinstance(restored.window, window_cls)
        assert isinstance(restored.downstream_mapper, expected_downstream_cls)
        assert list(restored.to_downstream(upstream_key)) == list(mapper.to_downstream(upstream_key))

    @pytest.mark.parametrize(
        ("direction", "expected"),
        [
            pytest.param(
                Window.Direction.FORWARD,
                [
                    "2024-03-04",
                    "2024-03-05",
                    "2024-03-06",
                    "2024-03-07",
                    "2024-03-08",
                    "2024-03-09",
                    "2024-03-10",
                ],
                id="forward",
            ),
            pytest.param(
                Window.Direction.BACKWARD,
                [
                    "2024-02-27",
                    "2024-02-28",
                    "2024-02-29",
                    "2024-03-01",
                    "2024-03-02",
                    "2024-03-03",
                    "2024-03-04",
                ],
                id="backward",
            ),
        ],
    )
    def test_fan_out_with_directional_window(self, direction, expected):
        """WeekWindow direction selects the period relative to the upstream Monday.

        2024-03-04 is a Monday; StartOfWeekMapper normalises it to itself.
        FORWARD yields the 7 days starting at that Monday (03-04 Mon … 03-10 Sun);
        BACKWARD yields the trailing 7 days ending at it (02-27 Tue … 03-04 Mon,
        including the leap-year Feb 29).
        """
        mapper = FanOutMapper(
            upstream_mapper=StartOfWeekMapper(),
            window=WeekWindow(direction=direction),
            downstream_mapper=StartOfDayMapper(),
        )
        result = list(mapper.to_downstream("2024-03-04T00:00:00"))
        assert result == expected

    @pytest.mark.parametrize(
        "direction",
        [Window.Direction.FORWARD, Window.Direction.BACKWARD],
    )
    def test_fan_out_with_directional_window_resolves_default_downstream_mapper(self, direction):
        """A directional WeekWindow is still a WeekWindow — default downstream lookup works unchanged."""
        mapper = FanOutMapper(
            upstream_mapper=StartOfWeekMapper(),
            window=WeekWindow(direction=direction),
        )
        assert isinstance(mapper.downstream_mapper, StartOfDayMapper)

    @pytest.mark.parametrize(
        "direction",
        [Window.Direction.FORWARD, Window.Direction.BACKWARD],
    )
    def test_fan_out_with_directional_window_serialize_roundtrip(self, direction):
        """A directional WeekWindow survives serialize → deserialize (direction and output preserved)."""
        mapper = FanOutMapper(
            upstream_mapper=StartOfWeekMapper(),
            window=WeekWindow(direction=direction),
        )
        restored = FanOutMapper.deserialize(mapper.serialize())
        assert isinstance(restored, FanOutMapper)
        assert isinstance(restored.window, WeekWindow)
        assert restored.window.direction is direction
        assert list(restored.to_downstream("2024-03-04T00:00:00")) == list(
            mapper.to_downstream("2024-03-04T00:00:00")
        )

    def test_max_downstream_keys_encode_decode_roundtrip(self):
        """max_downstream_keys=5 survives encode_partition_mapper → decode_partition_mapper."""
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow(), max_downstream_keys=5)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert restored.max_downstream_keys == 5

    def test_max_downstream_keys_absent_from_default_encoded_payload(self):
        """max_downstream_keys must NOT appear in the encoded payload when not set (zero-bloat contract)."""
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        encoded_var = encode_partition_mapper(mapper)[Encoding.VAR]
        assert "max_downstream_keys" not in encoded_var

    def test_max_downstream_keys_defaults_to_none_when_absent(self):
        """A payload lacking max_downstream_keys (e.g. serialized by an older Airflow) decodes to None.

        This is the real backward-compatibility path: the scheduler reads
        ``mapper.max_downstream_keys`` directly, so the attribute must always
        exist — ``deserialize`` defaults it to None when the key is absent.
        """
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert restored.max_downstream_keys is None
