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


class TestProductMapper:
    def test_to_downstream(self):
        pm = ProductMapper([IdentityMapper(), IdentityMapper()])
        assert pm.to_downstream("a|b") == "a|b"

    def test_to_downstream_wrong_segment_count(self):
        pm = ProductMapper([IdentityMapper(), IdentityMapper()])
        with pytest.raises(ValueError, match="Expected 2 segments"):
            pm.to_downstream("a|b|c")

    def test_to_downstream_single_segment_for_two_mappers(self):
        pm = ProductMapper([IdentityMapper(), IdentityMapper()])
        with pytest.raises(ValueError, match="Expected 2 segments"):
            pm.to_downstream("a")

    def test_requires_at_least_two_mappers(self):
        with pytest.raises(ValueError, match="at least 2"):
            ProductMapper([IdentityMapper()])

    def test_requires_at_least_two_mappers_empty(self):
        with pytest.raises(ValueError, match="at least 2"):
            ProductMapper([])

    def test_serialize(self):
        from airflow.serialization.encoders import encode_partition_mapper

        pm = ProductMapper([IdentityMapper(), IdentityMapper()])
        result = pm.serialize()
        assert result == {
            "mappers": [
                encode_partition_mapper(IdentityMapper()),
                encode_partition_mapper(IdentityMapper()),
            ]
        }

    def test_deserialize(self):
        pm = ProductMapper([IdentityMapper(), IdentityMapper()])
        serialized = pm.serialize()
        restored = ProductMapper.deserialize(serialized)
        assert isinstance(restored, ProductMapper)
        assert len(restored.mappers) == 2
        assert restored.to_downstream("x|y") == "x|y"

    def test_three_mappers(self):
        pm = ProductMapper([IdentityMapper(), IdentityMapper(), IdentityMapper()])
        assert pm.to_downstream("a|b|c") == "a|b|c"
