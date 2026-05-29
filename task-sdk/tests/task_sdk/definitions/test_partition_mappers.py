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
from airflow.sdk.definitions.partition_mappers.constant import ConstantMapper
from airflow.sdk.definitions.partition_mappers.identity import IdentityMapper
from airflow.sdk.definitions.partition_mappers.temporal import StartOfDayMapper
from airflow.sdk.definitions.partition_mappers.window import (
    DayWindow,
    DynamicSegmentWindow,
    HourWindow,
    MonthWindow,
    QuarterWindow,
    SegmentWindow,
    WeekWindow,
    Window,
    YearWindow,
)


# Module-level resolver — must be at module scope so it is importable via its dotted path.
def _sdk_test_resolver_two_regions() -> list[str]:
    """Return a fixed two-region list for use in SDK tests."""
    return ["us-east", "eu-west"]


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


class TestSdkWindowExpectedDecodedType:
    """Each SDK temporal window must declare ``datetime`` so the validation lines up with core."""

    @pytest.mark.parametrize(
        "window_cls",
        [HourWindow, DayWindow, WeekWindow, MonthWindow, QuarterWindow, YearWindow],
    )
    def test_temporal_windows_declare_datetime(self, window_cls):
        assert window_cls.expected_decoded_type is datetime


class TestSdkSegmentWindow:
    """SDK-side SegmentWindow construction and validation mirrors the core implementation."""

    def test_construction_stores_frozenset(self):
        w = SegmentWindow(["us-east", "eu-west", "ap-south"])
        assert w._segments == frozenset({"us-east", "eu-west", "ap-south"})

    def test_expected_decoded_type_is_str(self):
        assert SegmentWindow.expected_decoded_type is str

    def test_deduplication(self):
        w = SegmentWindow(["a", "b", "a"])
        assert w._segments == frozenset({"a", "b"})

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
            pytest.param([42, "b"], "must be str", id="int-element"),
            pytest.param([None, "b"], "must be str", id="none-element"),
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

    def test_rollup_mapper_accepts_constant_mapper(self):
        """SDK RollupMapper must accept SegmentWindow paired with a collapsing mapper."""
        # Should not raise: ConstantMapper collapses every segment onto one partition.
        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=SegmentWindow(["us-east", "eu-west"]),
        )
        assert mapper.window._segments == frozenset({"us-east", "eu-west"})

    def test_rollup_mapper_rejects_non_collapsing_mapper(self):
        """SDK RollupMapper must reject SegmentWindow paired with a non-collapsing mapper."""
        with pytest.raises(TypeError, match="collapse onto one downstream partition"):
            RollupMapper(
                upstream_mapper=IdentityMapper(),
                window=SegmentWindow(["us-east", "eu-west"]),
            )


class TestSdkDynamicSegmentWindow:
    """SDK-side DynamicSegmentWindow construction and validation mirrors the core implementation."""

    def test_construction_stores_resolver_path(self):
        w = DynamicSegmentWindow(_sdk_test_resolver_two_regions)
        # The resolver path is the dotted path used to re-import the function.
        assert w._resolver_path.endswith("._sdk_test_resolver_two_regions")

    def test_construction_stores_resolver_callable(self):
        w = DynamicSegmentWindow(_sdk_test_resolver_two_regions)
        assert w._resolver is _sdk_test_resolver_two_regions

    def test_expected_decoded_type_is_str(self):
        assert DynamicSegmentWindow.expected_decoded_type is str

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

    def test_rollup_mapper_accepts_constant_mapper(self):
        """SDK RollupMapper must accept DynamicSegmentWindow paired with a collapsing mapper."""
        # Should not raise: ConstantMapper collapses every segment onto one partition.
        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=DynamicSegmentWindow(_sdk_test_resolver_two_regions),
        )
        assert mapper.window._resolver is _sdk_test_resolver_two_regions

    def test_rollup_mapper_rejects_non_collapsing_mapper(self):
        """SDK RollupMapper must reject DynamicSegmentWindow paired with a non-collapsing mapper."""
        with pytest.raises(TypeError, match="collapse onto one downstream partition"):
            RollupMapper(
                upstream_mapper=IdentityMapper(),
                window=DynamicSegmentWindow(_sdk_test_resolver_two_regions),
            )


class TestSdkConstantMapper:
    """SDK-side ConstantMapper construction and validation."""

    def test_collapses_every_key_onto_constant(self):
        pm = ConstantMapper("all_regions")
        assert pm.to_downstream("us") == "all_regions"
        assert pm.to_downstream("eu") == "all_regions"

    def test_collapses_to_constant_flag(self):
        assert ConstantMapper.collapses_to_constant is True

    @pytest.mark.parametrize(
        "downstream_key",
        [
            pytest.param("", id="empty-string"),
            pytest.param(None, id="none"),
            pytest.param(1, id="int"),
        ],
    )
    def test_rejects_invalid_downstream_key(self, downstream_key):
        with pytest.raises(ValueError, match="non-empty str"):
            ConstantMapper(downstream_key)
