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

from airflow.partition_mappers.base import PartitionMapper
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.sequence import SequenceMapper
from airflow.partition_mappers.temporal import ToDailyMapper, ToHourlyMapper


class _FanOutMapper(PartitionMapper):
    def to_downstream(self, key: str) -> list[str]:
        return [f"{key}-a", f"{key}-b"]


class _InvalidReturnMapper(PartitionMapper):
    def to_downstream(self, key: str) -> int:  # type: ignore[override]
        return 42


class _InvalidIterableMapper(PartitionMapper):
    def to_downstream(self, key: str) -> list[object]:  # type: ignore[override]
        return [key, 42]


class TestSequenceMapper:
    def test_to_downstream(self):
        sm = SequenceMapper(ToHourlyMapper(), ToDailyMapper(input_format="%Y-%m-%dT%H"))
        assert sm.to_downstream("2024-01-15T10:30:00") == "2024-01-15"

    def test_to_downstream_fan_out(self):
        sm = SequenceMapper(_FanOutMapper(), IdentityMapper())
        assert sm.to_downstream("key") == ["key-a", "key-b"]

    def test_to_downstream_invalid_non_iterable_return(self):
        sm = SequenceMapper(IdentityMapper(), _InvalidReturnMapper())
        with pytest.raises(TypeError, match="must return a string or iterable of strings"):
            sm.to_downstream("key")

    def test_to_downstream_invalid_iterable_contents(self):
        sm = SequenceMapper(IdentityMapper(), _InvalidIterableMapper())
        with pytest.raises(TypeError, match="must return an iterable of strings"):
            sm.to_downstream("key")

    def test_serialize(self):
        from airflow.serialization.encoders import encode_partition_mapper

        sm = SequenceMapper(ToHourlyMapper(), ToDailyMapper(input_format="%Y-%m-%dT%H"))
        result = sm.serialize()
        assert result == {
            "mappers": [
                encode_partition_mapper(ToHourlyMapper()),
                encode_partition_mapper(ToDailyMapper(input_format="%Y-%m-%dT%H")),
            ],
        }

    def test_deserialize(self):
        sm = SequenceMapper(ToHourlyMapper(), ToDailyMapper(input_format="%Y-%m-%dT%H"))
        serialized = sm.serialize()
        restored = SequenceMapper.deserialize(serialized)
        assert isinstance(restored, SequenceMapper)
        assert len(restored.mappers) == 2
        assert restored.to_downstream("2024-01-15T10:30:00") == "2024-01-15"
