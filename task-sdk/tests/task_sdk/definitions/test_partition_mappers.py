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
from airflow.sdk.definitions.partition_mappers.temporal import StartOfDayMapper
from airflow.sdk.definitions.partition_mappers.window import (
    DayWindow,
    HourWindow,
    MonthWindow,
    QuarterWindow,
    WeekWindow,
    Window,
    WindowDirection,
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


class TestSdkWindowDirectionValidation:
    """SDK Window.__init__ must coerce valid strings and reject invalid ones at construction time."""

    @pytest.mark.parametrize(
        ("direction_input", "expected_member"),
        [
            pytest.param(WindowDirection.FORWARD, WindowDirection.FORWARD, id="enum_forward"),
            pytest.param(WindowDirection.BACKWARD, WindowDirection.BACKWARD, id="enum_backward"),
            pytest.param("forward", WindowDirection.FORWARD, id="str_forward"),
            pytest.param("backward", WindowDirection.BACKWARD, id="str_backward"),
        ],
    )
    def test_valid_direction_coerced_to_enum(self, direction_input, expected_member):
        window = WeekWindow(direction=direction_input)
        assert window.direction is expected_member

    @pytest.mark.parametrize(
        "bad_value",
        [
            pytest.param("foward", id="typo_foward"),
            pytest.param("backwards", id="typo_backwards"),
            pytest.param("FORWARD", id="wrong_case"),
            pytest.param("", id="empty_string"),
        ],
    )
    def test_invalid_direction_raises_value_error(self, bad_value):
        with pytest.raises(ValueError, match="is not a valid WindowDirection"):
            WeekWindow(direction=bad_value)


class TestSdkWindowExpectedDecodedType:
    """Each SDK temporal window must declare ``datetime`` so the validation lines up with core."""

    @pytest.mark.parametrize(
        "window_cls",
        [HourWindow, DayWindow, WeekWindow, MonthWindow, QuarterWindow, YearWindow],
    )
    def test_temporal_windows_declare_datetime(self, window_cls):
        assert window_cls.expected_decoded_type is datetime
