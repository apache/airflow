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

from airflow.partition_mappers.base import PartitionMapper, RollupMapper
from airflow.partition_mappers.fixed_key import FixedKeyMapper
from airflow.partition_mappers.window import DayWindow, SegmentWindow
from airflow.sdk import (
    FixedKeyMapper as SdkFixedKeyMapper,
    RollupMapper as SdkRollupMapper,
    SegmentWindow as SdkSegmentWindow,
)
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper


class TestFixedKeyMapper:
    @pytest.mark.parametrize("key", ["us", "eu", "apac", "anything-else"])
    def test_to_downstream_returns_constant_for_any_key(self, key):
        assert FixedKeyMapper("all").to_downstream(key) == "all"

    def test_is_rollup_false(self):
        # A bare FixedKeyMapper is not a rollup; rollup-ness comes from RollupMapper.
        assert FixedKeyMapper("all").is_rollup is False

    def test_does_not_override_decode_encode(self):
        m = FixedKeyMapper("all")
        assert type(m).decode_downstream is PartitionMapper.decode_downstream
        assert type(m).encode_upstream is PartitionMapper.encode_upstream

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

    def test_serialize_round_trip(self):
        m = FixedKeyMapper("bucket")
        restored = FixedKeyMapper.deserialize(m.serialize())
        assert isinstance(restored, FixedKeyMapper)
        assert restored.downstream_key == "bucket"


class TestCategoricalRollupEquivalence:
    """RollupMapper(FixedKeyMapper, SegmentWindow) behaves like old SegmentMapper."""

    def setup_method(self):
        self.m = RollupMapper(
            upstream_mapper=FixedKeyMapper("all"),
            window=SegmentWindow(["us", "eu", "apac"]),
        )

    def test_is_rollup_flag(self):
        assert self.m.is_rollup is True

    def test_to_downstream_collapses_every_segment_onto_downstream_key(self):
        # Full-sequence equality: every declared segment key maps to the constant key.
        assert [self.m.to_downstream(s) for s in ("us", "eu", "apac")] == ["all", "all", "all"]

    @pytest.mark.parametrize("anchor", ["all", "anything"])
    def test_to_upstream_returns_full_set_ignoring_anchor(self, anchor):
        assert self.m.to_upstream(anchor) == frozenset({"us", "eu", "apac"})

    def test_core_encode_decode_round_trip(self):
        restored = decode_partition_mapper(encode_partition_mapper(self.m))
        assert isinstance(restored, RollupMapper)
        assert restored.is_rollup is True
        assert restored.to_downstream("us") == "all"
        assert restored.to_upstream("all") == frozenset({"us", "eu", "apac"})

    def test_sdk_encode_decode_round_trip(self):
        # User code authors with SDK classes; the scheduler serializes and deserializes
        # into core classes.
        sdk_mapper = SdkRollupMapper(
            upstream_mapper=SdkFixedKeyMapper("all_regions"),
            window=SdkSegmentWindow(["us", "eu", "apac"]),
        )
        restored = decode_partition_mapper(encode_partition_mapper(sdk_mapper))
        assert isinstance(restored, RollupMapper)
        assert restored.to_upstream("all_regions") == frozenset({"us", "eu", "apac"})


class TestCategoricalRollupTypeGuard:
    """Core-side RollupMapper guard: FixedKeyMapper(str) + SegmentWindow(str) must pass."""

    def test_fixed_key_with_segment_window_does_not_raise(self):
        # Core guard: FixedKeyMapper does not override decode_downstream,
        # SegmentWindow.expected_decoded_type is str -> guard passes.
        RollupMapper(upstream_mapper=FixedKeyMapper("all"), window=SegmentWindow(["us", "eu"]))

    def test_str_mapper_with_datetime_window_raises(self):
        # Core guard: FixedKeyMapper (no decode override) + DayWindow (datetime) -> raise.
        with pytest.raises(TypeError, match="DayWindow expects decoded values of type 'datetime'"):
            RollupMapper(upstream_mapper=FixedKeyMapper("all"), window=DayWindow())
