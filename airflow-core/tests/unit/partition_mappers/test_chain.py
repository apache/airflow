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

from datetime import datetime, timezone

import pytest

from airflow.partition_mappers.base import PartitionMapper
from airflow.partition_mappers.chain import ChainMapper
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.temporal import StartOfDayMapper, StartOfHourMapper
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper
from airflow.serialization.enums import Encoding


class _InvalidReturnMapper(PartitionMapper):
    def to_downstream(self, key: str) -> None:  # type: ignore[override]
        return None


class _InvalidIterableMapper(PartitionMapper):
    def to_downstream(self, key: str) -> list[None]:  # type: ignore[override]
        return [None]


class TestChainMapper:
    def test_to_downstream(self):
        sm = ChainMapper(StartOfHourMapper(), StartOfDayMapper(input_format="%Y-%m-%dT%H"))
        assert sm.to_downstream("2024-01-15T10:30:00") == "2024-01-15"

    @pytest.mark.parametrize(
        ("chain", "downstream_key", "expected"),
        [
            # Last mapper temporal → it owns the final downstream key, so it owns the anchor.
            (
                ChainMapper(IdentityMapper(), StartOfDayMapper()),
                "2024-03-15",
                datetime(2024, 3, 15, 0, 0, 0, tzinfo=timezone.utc),
            ),
            # Last mapper non-temporal → no anchor.
            (ChainMapper(StartOfDayMapper(), IdentityMapper()), "anything", None),
        ],
    )
    def test_to_partition_date_delegates_to_last_mapper(self, chain, downstream_key, expected):
        assert chain.to_partition_date(downstream_key) == expected

    def test_to_downstream_invalid_non_iterable_return(self):
        sm = ChainMapper(IdentityMapper(), _InvalidReturnMapper())
        with pytest.raises(TypeError, match="must return a string or iterable of strings"):
            sm.to_downstream("key")

    def test_to_downstream_invalid_iterable_contents(self):
        sm = ChainMapper(IdentityMapper(), _InvalidIterableMapper())
        with pytest.raises(TypeError, match="must return an iterable of strings"):
            sm.to_downstream("key")

    def test_serialize(self):
        sm = ChainMapper(StartOfHourMapper(), StartOfDayMapper(input_format="%Y-%m-%dT%H"))
        result = sm.serialize()
        assert result == {
            "mappers": [
                encode_partition_mapper(StartOfHourMapper()),
                encode_partition_mapper(StartOfDayMapper(input_format="%Y-%m-%dT%H")),
            ],
        }

    def test_deserialize(self):
        sm = ChainMapper(StartOfHourMapper(), StartOfDayMapper(input_format="%Y-%m-%dT%H"))
        serialized = sm.serialize()
        restored = ChainMapper.deserialize(serialized)
        assert isinstance(restored, ChainMapper)
        assert len(restored.mappers) == 2
        assert restored.to_downstream("2024-01-15T10:30:00") == "2024-01-15"

    def test_max_downstream_keys_encode_decode_roundtrip(self):
        """max_downstream_keys=5 survives encode_partition_mapper → decode_partition_mapper."""
        mapper = ChainMapper(
            StartOfHourMapper(), StartOfDayMapper(input_format="%Y-%m-%dT%H"), max_downstream_keys=5
        )
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert restored.max_downstream_keys == 5

    def test_max_downstream_keys_absent_from_default_encoded_payload(self):
        """max_downstream_keys must NOT appear in the encoded payload when not set (zero-bloat contract)."""
        mapper = ChainMapper(StartOfHourMapper(), StartOfDayMapper(input_format="%Y-%m-%dT%H"))
        encoded_var = encode_partition_mapper(mapper)[Encoding.VAR]
        assert "max_downstream_keys" not in encoded_var
