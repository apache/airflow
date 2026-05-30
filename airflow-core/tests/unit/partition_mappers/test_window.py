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
from airflow.partition_mappers.constant import ConstantMapper
from airflow.partition_mappers.identity import IdentityMapper
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
    DynamicSegmentWindow,
    HourWindow,
    MonthWindow,
    QuarterWindow,
    SegmentWindow,
    WeekWindow,
    YearWindow,
)


# Module-level resolver used by TestDynamicSegmentWindow — must be at module scope so it
# is importable via its dotted path (the serialization round-trip requirement).
def _test_resolver_three_regions() -> list[str]:
    """Return a fixed three-region list for use in tests."""
    return ["us-east", "eu-west", "ap-south"]


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


class TestSegmentWindow:
    def test_construction_basic(self):
        w = SegmentWindow(["us-east", "eu-west", "ap-south"])
        assert w._segments == frozenset({"us-east", "eu-west", "ap-south"})

    def test_to_upstream_returns_full_segment_set(self):
        segments = ["us-east", "eu-west", "ap-south"]
        w = SegmentWindow(segments)
        assert w.to_upstream("anything") == frozenset(segments)

    def test_to_upstream_ignores_downstream_anchor(self):
        w = SegmentWindow(["a", "b"])
        # Different anchor values must produce the same result.
        assert w.to_upstream("key-1") == w.to_upstream("key-2") == w.to_upstream("")

    def test_deduplication(self):
        w = SegmentWindow(["x", "y", "x", "y", "z"])
        assert w._segments == frozenset({"x", "y", "z"})

    @pytest.mark.parametrize(
        ("segments", "match"),
        [
            pytest.param([], "at least one segment key", id="empty-list"),
            pytest.param(iter([]), "at least one segment key", id="empty-iterator"),
        ],
    )
    def test_rejects_empty_segments(self, segments, match):
        with pytest.raises(ValueError, match=match):
            SegmentWindow(segments)

    @pytest.mark.parametrize(
        ("segments", "match"),
        [
            pytest.param([1, "b"], "must be str", id="int-element"),
            pytest.param([None, "b"], "must be str", id="none-element"),
            pytest.param([["nested"], "b"], "must be str", id="list-element"),
        ],
    )
    def test_rejects_non_str_elements(self, segments, match):
        with pytest.raises(ValueError, match=match):
            SegmentWindow(segments)

    @pytest.mark.parametrize(
        ("segments", "match"),
        [
            pytest.param(["", "b"], "non-empty strings", id="empty-string-first"),
            pytest.param(["a", ""], "non-empty strings", id="empty-string-second"),
        ],
    )
    def test_rejects_empty_string_keys(self, segments, match):
        with pytest.raises(ValueError, match=match):
            SegmentWindow(segments)

    def test_serialize_round_trip(self):
        from airflow.serialization.decoders import decode_window
        from airflow.serialization.encoders import encode_window

        w = SegmentWindow(["eu-west", "us-east", "ap-south"])
        restored = decode_window(encode_window(w))
        assert isinstance(restored, SegmentWindow)
        assert restored._segments == w._segments
        assert restored.to_upstream("anchor") == w.to_upstream("anchor")

    def test_serialize_uses_sorted_order(self):
        w = SegmentWindow(["z", "a", "m"])
        data = w.serialize()
        assert data == {"segments": ["a", "m", "z"]}

    def test_sdk_window_serialize_round_trip(self):
        # User code authors with the SDK class; the scheduler serializes that
        # instance. This path dispatches on the SDK type (not the core class
        # exercised by ``test_serialize_round_trip``), so it must survive the
        # round trip without dropping the segment set.
        from airflow.sdk import SegmentWindow as SdkSegmentWindow
        from airflow.serialization.decoders import decode_window
        from airflow.serialization.encoders import encode_window

        sdk_window = SdkSegmentWindow(["eu-west", "us-east", "ap-south"])
        restored = decode_window(encode_window(sdk_window))
        assert isinstance(restored, SegmentWindow)
        assert restored._segments == frozenset({"eu-west", "us-east", "ap-south"})

    def test_rollup_mapper_with_constant_mapper(self):
        segments = {"us-east", "eu-west", "ap-south"}
        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=SegmentWindow(sorted(segments)),
        )
        # Every segment collapses onto the one downstream partition...
        assert {mapper.to_downstream(s) for s in segments} == {"all_regions"}
        # ...and that partition expects the full segment set.
        assert mapper.to_upstream("all_regions") == frozenset(segments)

    def test_rollup_mapper_rejects_non_collapsing_mapper(self):
        with pytest.raises(TypeError, match="collapse onto one downstream partition"):
            RollupMapper(
                upstream_mapper=IdentityMapper(),
                window=SegmentWindow(["us-east", "eu-west", "ap-south"]),
            )

    def test_rollup_mapper_serialize_round_trip(self):
        from airflow.serialization.decoders import decode_partition_mapper
        from airflow.serialization.encoders import encode_partition_mapper

        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=SegmentWindow(["us-east", "eu-west"]),
        )
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert isinstance(restored.window, SegmentWindow)
        assert isinstance(restored.upstream_mapper, ConstantMapper)
        assert restored.upstream_mapper.downstream_key == "all_regions"
        assert restored.to_downstream("anything") == "all_regions"
        assert restored.to_upstream("any-key") == mapper.to_upstream("any-key")


class TestDynamicSegmentWindow:
    """Tests for DynamicSegmentWindow construction, validation, and serialization."""

    def test_to_upstream_returns_resolver_set(self):
        w = DynamicSegmentWindow(_test_resolver_three_regions)
        result = w.to_upstream("any-downstream-key")
        assert result == frozenset({"us-east", "eu-west", "ap-south"})

    def test_to_upstream_ignores_downstream_anchor(self):
        w = DynamicSegmentWindow(_test_resolver_three_regions)
        assert w.to_upstream("key-1") == w.to_upstream("key-2") == w.to_upstream("")

    def test_stores_resolver_path(self):
        w = DynamicSegmentWindow(_test_resolver_three_regions)
        # The path ends with the function name; the module prefix varies by test runner.
        assert w._resolver_path.endswith("._test_resolver_three_regions")

    def test_stores_resolver_callable(self):
        w = DynamicSegmentWindow(_test_resolver_three_regions)
        assert w._resolver is _test_resolver_three_regions

    # --- rejection at construction ---

    @pytest.mark.parametrize(
        ("bad_resolver", "exc_type", "match"),
        [
            pytest.param(
                lambda: ["a"],
                ValueError,
                "Lambdas are rejected",
                id="lambda",
            ),
            pytest.param(
                42,
                TypeError,
                "plain function",
                id="not-a-function",
            ),
        ],
    )
    def test_rejects_invalid_resolver_at_construction(self, bad_resolver, exc_type, match):
        with pytest.raises(exc_type, match=match):
            DynamicSegmentWindow(bad_resolver)

    def test_rejects_closure_at_construction(self):
        def _make_closure():
            items = ["a"]

            def _inner():
                return items

            return _inner

        closure = _make_closure()
        with pytest.raises(ValueError, match="Closures and nested functions are rejected"):
            DynamicSegmentWindow(closure)

    def test_rejects_nested_function_at_construction(self):
        def _nested():
            return ["x"]

        with pytest.raises(ValueError, match="Closures and nested functions are rejected"):
            DynamicSegmentWindow(_nested)

    def test_rejects_bound_method_at_construction(self):
        class _Helper:
            def resolve(self):
                return ["a"]

        obj = _Helper()
        with pytest.raises(TypeError, match="plain function"):
            DynamicSegmentWindow(obj.resolve)

    def test_rejects_non_importable_function(self):
        """A function whose dotted path does not round-trip is rejected at construction."""

        # Create a function in a fake module that cannot be re-imported.
        code = compile("def my_resolver(): return ['a']", "<string>", "exec")
        globs = {"__name__": "_fake_test_module_xyz"}
        exec(code, globs)
        fn = globs["my_resolver"]
        fn.__module__ = "_fake_test_module_xyz"
        # _fake_test_module_xyz is not in sys.modules, so import_string will fail.
        with pytest.raises(ValueError, match="is not importable via its dotted path"):
            DynamicSegmentWindow(fn)

    # --- validation at to_upstream ---

    def test_to_upstream_raises_on_non_str_element(self):
        import sys
        import types

        # Build a module-level resolver that returns a non-str element, importable via sys.modules.
        mod_name = "tests.unit.partition_mappers._helper_non_str_resolver"
        mod = types.ModuleType(mod_name)
        code = compile("def resolve(): return [42, 'b']", "<string>", "exec")
        globs = {"__name__": mod_name}
        exec(code, globs)
        mod.resolve = globs["resolve"]
        mod.resolve.__module__ = mod_name
        sys.modules[mod_name] = mod

        try:
            w = DynamicSegmentWindow(mod.resolve)
            with pytest.raises(ValueError, match="non-str"):
                w.to_upstream("anchor")
        finally:
            sys.modules.pop(mod_name, None)

    def test_to_upstream_raises_on_empty_string_element(self):
        import sys
        import types

        mod_name = "tests.unit.partition_mappers._helper_empty_str_resolver"
        mod = types.ModuleType(mod_name)
        code = compile("def resolve(): return ['a', '', 'c']", "<string>", "exec")
        globs = {"__name__": mod_name}
        exec(code, globs)
        mod.resolve = globs["resolve"]
        mod.resolve.__module__ = mod_name
        sys.modules[mod_name] = mod

        try:
            w = DynamicSegmentWindow(mod.resolve)
            with pytest.raises(ValueError, match="empty string"):
                w.to_upstream("anchor")
        finally:
            sys.modules.pop(mod_name, None)

    # --- serialization ---

    def test_serialize_returns_resolver_path(self):
        w = DynamicSegmentWindow(_test_resolver_three_regions)
        data = w.serialize()
        # The exact module prefix varies by test runner; check the key and suffix.
        assert "resolver" in data
        assert data["resolver"].endswith("._test_resolver_three_regions")

    def test_core_serialize_round_trip(self):
        """Core-class encode → decode preserves the resolver path and produces the same set."""
        from airflow.serialization.decoders import decode_window
        from airflow.serialization.encoders import encode_window

        w = DynamicSegmentWindow(_test_resolver_three_regions)
        restored = decode_window(encode_window(w))
        assert isinstance(restored, DynamicSegmentWindow)
        assert restored._resolver_path == w._resolver_path
        assert restored.to_upstream("anchor") == w.to_upstream("anchor")

    def test_sdk_window_serialize_round_trip(self):
        """
        SDK-class encode → decode must not drop the resolver path.

        User code authors with the SDK class (``from airflow.sdk import DynamicSegmentWindow``);
        the scheduler serializes that SDK instance. This test exercises the SDK-type dispatch path
        in ``serialize_window`` — the path that was missing for ``SegmentWindow`` before the fix.
        The core-class round-trip (``test_core_serialize_round_trip``) does NOT cover this path.
        """
        from airflow.sdk import DynamicSegmentWindow as SdkDynamicSegmentWindow
        from airflow.serialization.decoders import decode_window
        from airflow.serialization.encoders import encode_window

        sdk_window = SdkDynamicSegmentWindow(_test_resolver_three_regions)
        restored = decode_window(encode_window(sdk_window))
        assert isinstance(restored, DynamicSegmentWindow)
        assert restored._resolver_path.endswith("._test_resolver_three_regions")
        assert restored.to_upstream("anchor") == frozenset({"us-east", "eu-west", "ap-south"})

    def test_rollup_mapper_with_constant_mapper(self):
        """DynamicSegmentWindow composes correctly with ConstantMapper inside RollupMapper."""
        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=DynamicSegmentWindow(_test_resolver_three_regions),
        )
        assert {mapper.to_downstream(s) for s in ("us-east", "eu-west", "ap-south")} == {"all_regions"}
        assert mapper.to_upstream("all_regions") == frozenset({"us-east", "eu-west", "ap-south"})

    def test_rollup_mapper_rejects_non_collapsing_mapper(self):
        with pytest.raises(TypeError, match="collapse onto one downstream partition"):
            RollupMapper(
                upstream_mapper=IdentityMapper(),
                window=DynamicSegmentWindow(_test_resolver_three_regions),
            )

    def test_rollup_mapper_serialize_round_trip(self):
        """RollupMapper containing DynamicSegmentWindow survives a full encode → decode cycle."""
        from airflow.serialization.decoders import decode_partition_mapper
        from airflow.serialization.encoders import encode_partition_mapper

        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=DynamicSegmentWindow(_test_resolver_three_regions),
        )
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert isinstance(restored, RollupMapper)
        assert isinstance(restored.window, DynamicSegmentWindow)
        assert isinstance(restored.upstream_mapper, ConstantMapper)
        assert restored.to_downstream("anything") == "all_regions"
        assert restored.to_upstream("any-key") == mapper.to_upstream("any-key")


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
