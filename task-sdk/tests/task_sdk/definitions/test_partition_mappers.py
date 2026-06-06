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
from typing import ClassVar

import pytest

from airflow.sdk.definitions.partition_mappers.base import PartitionMapper, RollupMapper
from airflow.sdk.definitions.partition_mappers.fixed_key import FixedKeyMapper
from airflow.sdk.definitions.partition_mappers.temporal import StartOfDayMapper
from airflow.sdk.definitions.partition_mappers.window import (
    DayWindow,
    HourWindow,
    MonthWindow,
    QuarterWindow,
    SegmentWindow,
    WeekWindow,
    Window,
    YearWindow,
)


class TestSdkRollupMapperInit:
    """
    The SDK-side ``RollupMapper.__init__`` mirrors the core check so user code
    (which imports from ``airflow.sdk``) fails at Dag parse time instead of
    deferring the error to scheduler deserialization, where the misconfiguration
    is swallowed by the bare ``except`` in ``_create_dagruns_for_partitioned_asset_dags``.
    """

    def test_rejects_identity_mapper_with_temporal_window(self):
        class _StringOnlyMapper(PartitionMapper):
            pass

        with pytest.raises(TypeError, match="DayWindow expects decoded values of type 'datetime'"):
            RollupMapper(upstream_mapper=_StringOnlyMapper(), window=DayWindow())

    def test_accepts_temporal_mapper_with_temporal_window(self):
        # Should not raise.
        RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow())

    def test_accepts_string_only_window_with_identity_mapper(self):
        class _StringOnlyMapper(PartitionMapper):
            pass

        class _AlphaWindow(Window):
            expected_decoded_type: ClassVar[type] = str

        # Should not raise.
        RollupMapper(upstream_mapper=_StringOnlyMapper(), window=_AlphaWindow())


class TestSdkDirectionValidation:
    """SDK Window.__init__ must coerce valid strings and reject invalid ones at construction time."""

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


class TestSdkWindowExpectedDecodedType:
    """Each SDK temporal window must declare ``datetime`` so the validation lines up with core."""

    @pytest.mark.parametrize(
        "window_cls",
        [HourWindow, DayWindow, WeekWindow, MonthWindow, QuarterWindow, YearWindow],
    )
    def test_temporal_windows_declare_datetime(self, window_cls):
        assert window_cls.expected_decoded_type is datetime


class TestSdkFixedKeyMapper:
    """SDK-side FixedKeyMapper construction and validation."""

    def test_to_downstream_returns_constant_for_any_key(self):
        m = FixedKeyMapper("all_regions")
        assert m.to_downstream("us") == "all_regions"
        assert m.to_downstream("eu") == "all_regions"
        assert m.to_downstream("apac") == "all_regions"

    def test_is_rollup_false(self):
        assert FixedKeyMapper("all").is_rollup is False

    @pytest.mark.parametrize(
        ("downstream_key", "match"),
        [
            pytest.param("", "non-empty str", id="empty-string"),
            pytest.param(None, "non-empty str", id="none"),
            pytest.param(1, "non-empty str", id="int"),
        ],
    )
    def test_rejects_invalid_downstream_key(self, downstream_key, match):
        with pytest.raises(ValueError, match=match):
            FixedKeyMapper(downstream_key)

    def test_requires_downstream_key(self):
        with pytest.raises(TypeError):
            FixedKeyMapper()


class TestSdkSegmentWindow:
    """SDK-side SegmentWindow construction and validation mirrors the core implementation."""

    def test_expected_decoded_type_is_str(self):
        assert SegmentWindow.expected_decoded_type is str

    def test_deduplication(self):
        w = SegmentWindow(["a", "b", "a"])
        assert w._segments == frozenset({"a", "b"})

    @pytest.mark.parametrize(
        ("segments", "match"),
        [
            pytest.param([], "at least one segment key", id="empty-list"),
            pytest.param([1, "b"], "must be str", id="int-element"),
            pytest.param(["", "b"], "non-empty strings", id="empty-string"),
        ],
    )
    def test_rejects_invalid_segments(self, segments, match):
        with pytest.raises(ValueError, match=match):
            SegmentWindow(segments)


class TestSdkCategoricalRollupGuard:
    """SDK-side RollupMapper guard mirrors core: str mapper + str window passes."""

    def test_fixed_key_with_segment_window_does_not_raise(self):
        # SDK guard: FixedKeyMapper.expected_decoded_type is str,
        # SegmentWindow.expected_decoded_type is str -> guard passes.
        RollupMapper(upstream_mapper=FixedKeyMapper("all"), window=SegmentWindow(["us", "eu"]))

    def test_str_mapper_with_datetime_window_raises(self):
        # SDK guard: FixedKeyMapper (str) + DayWindow (datetime) -> raise.
        with pytest.raises(TypeError, match="DayWindow expects decoded values of type 'datetime'"):
            RollupMapper(upstream_mapper=FixedKeyMapper("all"), window=DayWindow())
