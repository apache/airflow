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

from datetime import datetime, timedelta
from unittest import mock

import pytest

from airflow import plugins_manager
from airflow._shared.module_loading import qualname
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
    SegmentWindow,
    WeekWindow,
    Window,
    YearWindow,
)
from airflow.serialization.decoders import decode_partition_mapper, decode_window
from airflow.serialization.encoders import _serializer, encode_partition_mapper, encode_window
from airflow.serialization.enums import Encoding
from airflow.serialization.helpers import WindowNotSupported


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

    def test_day_window_tz_none_yields_24_naive_steps(self):
        """With no timezone (UTC / naive upstream mapper) DayWindow always yields 24 naive steps."""
        # 2024-03-10 / 2024-11-03 are US Eastern DST days, but with tz=None the window is
        # timezone-agnostic and yields a fixed 24 naive hourly steps.
        spring_forward = datetime(2024, 3, 10)
        assert list(DayWindow().to_upstream(spring_forward)) == [datetime(2024, 3, 10, h) for h in range(24)]

        fall_back = datetime(2024, 11, 3)
        assert list(DayWindow().to_upstream(fall_back)) == [datetime(2024, 11, 3, h) for h in range(24)]

    def test_day_window_spring_forward_local_tz_yields_23_keys(self):
        """
        On a spring-forward day a local-timezone rollup expects the 23 real local hours.

        2024-03-10 US Eastern skips 02:00 → 03:00, so the day has 23 hours. DayWindow is DST-aware
        and yields those 23 (no ``2024-03-10T02`` gap key), so the rollup can actually be satisfied
        instead of waiting forever for a key no producer emits.
        """
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(timezone="America/New_York", input_format="%Y-%m-%dT%H"),
            window=DayWindow(),
        )
        upstream_keys = mapper.to_upstream("2024-03-10")
        assert len(upstream_keys) == 23
        assert "2024-03-10T02" not in upstream_keys

    def test_day_window_fall_back_local_tz_does_not_hang(self):
        """
        On a fall-back day the local clock 01:00 repeats.

        Without ``%z`` in the upstream format the two 01:00 hours share a single key, so the window
        yields 24 distinct keys (not 25). Crucially none of them is unsatisfiable, so the rollup no
        longer hangs (full 25-key coverage would require ``%z`` in ``input_format``).
        """
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(timezone="America/New_York", input_format="%Y-%m-%dT%H"),
            window=DayWindow(),
        )
        assert len(mapper.to_upstream("2024-11-03")) == 24

    def test_day_window_non_dst_local_day_yields_24_keys(self):
        """A local-timezone day with no DST transition still yields exactly 24 keys."""
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(timezone="America/New_York", input_format="%Y-%m-%dT%H"),
            window=DayWindow(),
        )
        assert len(mapper.to_upstream("2024-06-10")) == 24

    def test_day_window_utc_yields_24_keys_on_dst_day(self):
        """A UTC upstream mapper is unaffected by local DST: 24 keys even on a spring-forward date."""
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(timezone="UTC", input_format="%Y-%m-%dT%H"),
            window=DayWindow(),
        )
        assert len(mapper.to_upstream("2024-03-10")) == 24

    def test_day_window_backward_spring_forward_local_tz_includes_anchor(self):
        """Backward local-timezone day windows mirror forward semantics and still skip the DST gap."""
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(timezone="America/New_York", input_format="%Y-%m-%dT%H"),
            window=DayWindow(direction=Window.Direction.BACKWARD),
        )
        upstream_keys = mapper.to_upstream("2024-03-11")
        assert len(upstream_keys) == 23
        assert "2024-03-10T00" not in upstream_keys
        assert "2024-03-10T02" not in upstream_keys
        assert "2024-03-11T00" in upstream_keys

    def test_day_window_backward_fall_back_local_tz_includes_anchor(self):
        """Backward local-timezone day windows exclude the prior day's anchor and include this one."""
        mapper = RollupMapper(
            upstream_mapper=StartOfDayMapper(timezone="America/New_York", input_format="%Y-%m-%dT%H"),
            window=DayWindow(direction=Window.Direction.BACKWARD),
        )
        upstream_keys = mapper.to_upstream("2024-11-04")
        assert len(upstream_keys) == 24
        assert "2024-11-03T00" not in upstream_keys
        assert "2024-11-04T00" in upstream_keys


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


class TestDirection:
    def test_default_direction_is_forward(self):
        assert WeekWindow().direction is Window.Direction.FORWARD

    @pytest.mark.parametrize(
        ("window", "anchor", "expected"),
        [
            pytest.param(
                HourWindow(direction=Window.Direction.BACKWARD),
                datetime(2024, 3, 4, 0),
                [datetime(2024, 3, 4, 0) - timedelta(minutes=59) + timedelta(minutes=i) for i in range(60)],
                id="hour",
            ),
            pytest.param(
                DayWindow(direction=Window.Direction.BACKWARD),
                datetime(2024, 3, 4),
                [datetime(2024, 3, 4) - timedelta(hours=23) + timedelta(hours=i) for i in range(24)],
                id="day",
            ),
            pytest.param(
                WeekWindow(direction=Window.Direction.BACKWARD),
                datetime(2024, 3, 4),  # Monday
                [datetime(2024, 2, 27) + timedelta(days=i) for i in range(7)],
                id="week",
            ),
            pytest.param(
                MonthWindow(direction=Window.Direction.BACKWARD),
                datetime(2024, 3, 1),
                # 2024-02 has 29 days (leap year); trailing period = Feb 2 … Mar 1 (29 members)
                [datetime(2024, 2, d) for d in range(2, 30)] + [datetime(2024, 3, 1)],
                id="month_backward_trailing",
            ),
            pytest.param(
                QuarterWindow(direction=Window.Direction.BACKWARD),
                datetime(2024, 1, 1),
                [datetime(2023, 11, 1), datetime(2023, 12, 1), datetime(2024, 1, 1)],
                id="quarter_backward_trailing",
            ),
            pytest.param(
                YearWindow(direction=Window.Direction.BACKWARD),
                datetime(2024, 1, 1),
                [datetime(2023, m, 1) for m in range(2, 13)] + [datetime(2024, 1, 1)],
                id="year_backward_trailing",
            ),
        ],
    )
    def test_backward_yields_trailing_period_ending_at_anchor(self, window, anchor, expected):
        result = list(window.to_upstream(anchor))
        assert result == expected
        assert result[-1] == anchor

    def test_month_backward_trailing_not_calendar_month(self):
        """Month backward yields the trailing period (prev_month_start, anchor], not a calendar month."""
        anchor = datetime(2024, 3, 1)
        result = list(MonthWindow(direction=Window.Direction.BACKWARD).to_upstream(anchor))
        # Must include anchor (Mar 1) and must NOT include prev_month_start (Feb 1)
        assert datetime(2024, 3, 1) in result
        assert datetime(2024, 2, 1) not in result

    @pytest.mark.parametrize(
        "window",
        [
            pytest.param(MonthWindow(direction=Window.Direction.BACKWARD), id="month"),
            pytest.param(QuarterWindow(direction=Window.Direction.BACKWARD), id="quarter"),
            pytest.param(YearWindow(direction=Window.Direction.BACKWARD), id="year"),
        ],
    )
    def test_backward_on_non_day_one_raises(self, window):
        """_require_day_one fires before the backward shift — non-day-1 + BACKWARD direction still raises."""
        with pytest.raises(ValueError, match="expects a period start on day 1"):
            list(window.to_upstream(datetime(2024, 3, 15)))

    @pytest.mark.parametrize(
        "window",
        [
            pytest.param(HourWindow(direction=Window.Direction.FORWARD), id="hour"),
            pytest.param(DayWindow(direction=Window.Direction.FORWARD), id="day"),
            pytest.param(WeekWindow(direction=Window.Direction.FORWARD), id="week"),
            pytest.param(MonthWindow(direction=Window.Direction.FORWARD), id="month"),
            pytest.param(QuarterWindow(direction=Window.Direction.FORWARD), id="quarter"),
            pytest.param(YearWindow(direction=Window.Direction.FORWARD), id="year"),
        ],
    )
    def test_serialize_roundtrip_with_forward(self, window):
        """Window.Direction.FORWARD survives serialize → deserialize; behaviour is identical."""
        restored = decode_window(encode_window(window))
        assert restored.direction is Window.Direction.FORWARD
        assert type(restored) is type(window)
        if isinstance(window, WeekWindow):
            anchor = datetime(2024, 3, 4)
            assert list(restored.to_upstream(anchor)) == list(window.to_upstream(anchor))

    @pytest.mark.parametrize(
        "window_cls",
        [HourWindow, DayWindow, WeekWindow, MonthWindow, QuarterWindow, YearWindow],
    )
    def test_serialize_roundtrip_backward(self, window_cls):
        window = window_cls(direction=Window.Direction.BACKWARD)
        restored = decode_window(encode_window(window))
        assert restored.direction is Window.Direction.BACKWARD
        assert type(restored) is window_cls
        if window_cls is WeekWindow:
            anchor = datetime(2024, 3, 4)
            assert list(restored.to_upstream(anchor)) == list(window.to_upstream(anchor))

    @pytest.mark.parametrize(
        ("window_cls", "anchor", "expected_count"),
        [
            (HourWindow, datetime(2024, 3, 15, 7, 30), 60),
            (DayWindow, datetime(2024, 3, 15), 24),
            (WeekWindow, datetime(2024, 3, 13), 7),  # Wed, non-Monday
        ],
        ids=["hour", "day", "week"],
    )
    def test_backward_non_day_one_does_not_raise(self, window_cls, anchor, expected_count):
        window = window_cls(direction=Window.Direction.BACKWARD)
        result = list(window.to_upstream(anchor))
        assert len(result) == expected_count


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
                # The window carries no tz state - the timezone lives on the mapper and is
                # serialized there, so the restored rollup must still reproduce the DST-correct
                # 23-key set on a spring-forward day.
                lambda: StartOfDayMapper(
                    timezone="America/New_York", input_format="%Y-%m-%dT%H", output_format="%Y-%m-%d"
                ),
                DayWindow(),
                "2024-03-10",
                id="day-local-tz-dst",
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
        mapper = RollupMapper(upstream_mapper=upstream_factory(), window=window)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert isinstance(restored.window, type(window))
        assert restored.to_upstream(downstream_key) == mapper.to_upstream(downstream_key)


class TestDirectionValidation:
    """Window.__init__ must coerce valid strings and reject invalid ones at construction time."""

    @pytest.mark.parametrize(
        ("direction_input", "expected_member"),
        [
            pytest.param(Window.Direction.FORWARD, Window.Direction.FORWARD, id="enum_forward"),
            pytest.param(Window.Direction.BACKWARD, Window.Direction.BACKWARD, id="enum_backward"),
            pytest.param("forward", Window.Direction.FORWARD, id="str_forward"),
            pytest.param("backward", Window.Direction.BACKWARD, id="str_backward"),
        ],
    )
    def test_valid_direction_coerced_to_enum(self, direction_input, expected_member):
        window = WeekWindow(direction=direction_input)
        assert window.direction is expected_member

    @pytest.mark.parametrize(
        "bad_value",
        [
            pytest.param("forwrd", id="typo_forwrd"),
            pytest.param("backwards", id="typo_backwards"),
            pytest.param("FORWARD", id="wrong_case"),
            pytest.param("", id="empty_string"),
        ],
    )
    def test_invalid_direction_raises_value_error(self, bad_value):
        with pytest.raises(ValueError, match=r"is not a valid Window\.Direction"):
            WeekWindow(direction=bad_value)


class TestSegmentWindow:
    def test_to_upstream_returns_full_set_ignoring_anchor(self):
        w = SegmentWindow(["us", "eu", "apac"])
        result_a = frozenset(w.to_upstream("any-anchor"))
        result_b = frozenset(w.to_upstream("different-anchor"))
        assert result_a == frozenset({"us", "eu", "apac"})
        assert result_a == result_b

    def test_expected_decoded_type_is_str(self):
        assert SegmentWindow.expected_decoded_type is str

    @pytest.mark.parametrize(
        ("segments", "match"),
        [
            pytest.param([], "at least one segment key", id="empty-list"),
            pytest.param(iter([]), "at least one segment key", id="empty-iterator"),
            pytest.param([1, "b"], "must be str", id="int-element"),
            pytest.param([None, "b"], "must be str", id="none-element"),
            pytest.param(["", "b"], "non-empty", id="empty-string-first"),
            pytest.param(["a", ""], "non-empty", id="empty-string-second"),
        ],
    )
    def test_rejects_invalid_segments(self, segments, match):
        with pytest.raises(ValueError, match=match):
            SegmentWindow(segments)

    def test_deduplication(self):
        w = SegmentWindow(["us", "us", "eu"])
        assert frozenset(w.to_upstream("any")) == frozenset({"us", "eu"})

    def test_serialize_uses_sorted_order(self):
        w = SegmentWindow(["z", "a", "m"])
        assert w.serialize() == {"segments": ["a", "m", "z"]}

    def test_deserialize_round_trip(self):
        w = SegmentWindow(["us", "eu", "apac"])
        restored = SegmentWindow.deserialize(w.serialize())
        assert isinstance(restored, SegmentWindow)
        assert frozenset(restored.to_upstream("any")) == frozenset({"us", "eu", "apac"})


class CustomRegWindow(Window):
    """A custom Window subclass defined at module scope so it has a stable qualname."""

    def to_upstream(self, decoded_downstream):
        return [decoded_downstream]


class TestWindowSerializationGate:
    """``encode_window`` / ``decode_window`` accept registered custom Windows and reject unregistered ones.

    A tampered serialized Dag naming an unregistered import path is still
    rejected; only classes that appear on a plugin's ``windows`` attribute
    are allowed through.
    """

    @pytest.fixture
    def window_plugin(self, monkeypatch):
        """Patch the plugins manager to expose CustomRegWindow under its qualname."""
        qn = qualname(CustomRegWindow)
        monkeypatch.setattr(
            plugins_manager,
            "get_windows_plugins",
            lambda: {qn: CustomRegWindow},
        )

    @pytest.mark.usefixtures("window_plugin")
    def test_registered_custom_window_round_trips(self):
        w = CustomRegWindow()
        restored = decode_window(encode_window(w))
        assert type(restored) is CustomRegWindow
        assert restored.to_upstream("x") == ["x"]

    def test_unregistered_custom_window_raises_on_encode(self):
        class UnregisteredWindow(Window):
            def to_upstream(self, decoded_downstream):
                return ()

        with pytest.raises(WindowNotSupported, match="UnregisteredWindow"):
            encode_window(UnregisteredWindow())

    def test_unregistered_import_path_raises_on_decode(self):
        with mock.patch("airflow.serialization.decoders.import_string") as mock_import:
            with pytest.raises(WindowNotSupported, match="os.system"):
                decode_window({Encoding.TYPE: "os.system", Encoding.VAR: {}})
        mock_import.assert_not_called()

    def test_builtin_windows_fast_path_unchanged(self):
        # The fast-path dict must contain exactly 7 entries, one per builtin SDK class.
        # Verify that every entry maps to the canonical core import path.
        expected = {
            "airflow.partition_mappers.window.HourWindow",
            "airflow.partition_mappers.window.DayWindow",
            "airflow.partition_mappers.window.WeekWindow",
            "airflow.partition_mappers.window.MonthWindow",
            "airflow.partition_mappers.window.QuarterWindow",
            "airflow.partition_mappers.window.SegmentWindow",
            "airflow.partition_mappers.window.YearWindow",
        }
        assert set(_serializer.BUILTIN_WINDOWS.values()) == expected
