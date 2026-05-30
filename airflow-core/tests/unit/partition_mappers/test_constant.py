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

from airflow.partition_mappers.constant import ConstantMapper


class TestConstantMapper:
    def test_collapses_every_key_onto_constant(self):
        pm = ConstantMapper("all_regions")
        assert pm.to_downstream("us") == "all_regions"
        assert pm.to_downstream("eu") == "all_regions"
        assert pm.to_downstream("") == "all_regions"

    def test_collapses_to_constant_flag(self):
        assert ConstantMapper.collapses_to_constant is True

    def test_is_not_rollup(self):
        assert ConstantMapper.is_rollup is False

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

    def test_serialize(self):
        assert ConstantMapper("all_regions").serialize() == {"downstream_key": "all_regions"}

    def test_deserialize_round_trip(self):
        pm = ConstantMapper("all_regions")
        restored = ConstantMapper.deserialize(pm.serialize())
        assert isinstance(restored, ConstantMapper)
        assert restored.downstream_key == "all_regions"

    def test_encode_decode_round_trip(self):
        from airflow.serialization.decoders import decode_partition_mapper
        from airflow.serialization.encoders import encode_partition_mapper

        pm = ConstantMapper("all_regions")
        restored = decode_partition_mapper(encode_partition_mapper(pm))
        assert isinstance(restored, ConstantMapper)
        assert restored.downstream_key == "all_regions"
