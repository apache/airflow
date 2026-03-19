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

from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.product import ProductMapper
from airflow.partition_mappers.temporal import ToDailyMapper, ToHourlyMapper


class TestProductMapper:
    def test_to_downstream(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper())
        assert pm.to_downstream("2024-01-15T10:30:00|2024-01-15T10:30:00") == "2024-01-15T10|2024-01-15"

    def test_to_downstream_wrong_segment_count(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper())
        with pytest.raises(ValueError, match="Expected 2 segments"):
            pm.to_downstream("2024-01-15T10:30:00|2024-01-15T10:30:00|extra")

    def test_to_downstream_single_segment_for_two_mappers(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper())
        with pytest.raises(ValueError, match="Expected 2 segments"):
            pm.to_downstream("2024-01-15T10:30:00")

    def test_custom_delimiter(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper(), delimiter="::")
        assert pm.to_downstream("2024-01-15T10:30:00::2024-01-15T10:30:00") == "2024-01-15T10::2024-01-15"

    def test_custom_delimiter_wrong_segment_count(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper(), delimiter="::")
        with pytest.raises(ValueError, match="Expected 2 segments"):
            pm.to_downstream("2024-01-15T10:30:00::2024-01-15T10:30:00::extra")

    def test_serialize(self):
        from airflow.serialization.encoders import encode_partition_mapper

        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper())
        result = pm.serialize()
        assert result == {
            "delimiter": "|",
            "mappers": [
                encode_partition_mapper(ToHourlyMapper()),
                encode_partition_mapper(ToDailyMapper()),
            ],
        }

    def test_serialize_custom_delimiter(self):
        from airflow.serialization.encoders import encode_partition_mapper

        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper(), delimiter="::")
        result = pm.serialize()
        assert result == {
            "delimiter": "::",
            "mappers": [
                encode_partition_mapper(ToHourlyMapper()),
                encode_partition_mapper(ToDailyMapper()),
            ],
        }

    def test_deserialize(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper())
        serialized = pm.serialize()
        restored = ProductMapper.deserialize(serialized)
        assert isinstance(restored, ProductMapper)
        assert len(restored.mappers) == 2
        assert restored.delimiter == "|"
        assert restored.to_downstream("2024-01-15T10:30:00|2024-01-15T10:30:00") == "2024-01-15T10|2024-01-15"

    def test_deserialize_custom_delimiter(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper(), delimiter="::")
        serialized = pm.serialize()
        restored = ProductMapper.deserialize(serialized)
        assert isinstance(restored, ProductMapper)
        assert restored.delimiter == "::"
        assert (
            restored.to_downstream("2024-01-15T10:30:00::2024-01-15T10:30:00") == "2024-01-15T10::2024-01-15"
        )

    def test_deserialize_backward_compat(self):
        """Deserializing data without delimiter field defaults to '|'."""
        from airflow.serialization.encoders import encode_partition_mapper

        data = {
            "mappers": [
                encode_partition_mapper(ToHourlyMapper()),
                encode_partition_mapper(ToDailyMapper()),
            ],
        }
        restored = ProductMapper.deserialize(data)
        assert restored.delimiter == "|"

    def test_three_mappers(self):
        pm = ProductMapper(ToHourlyMapper(), ToDailyMapper(), IdentityMapper())
        assert (
            pm.to_downstream("2024-01-15T10:30:00|2024-01-15T10:30:00|raw") == "2024-01-15T10|2024-01-15|raw"
        )
