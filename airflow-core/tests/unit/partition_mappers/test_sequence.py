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

from airflow.partition_mappers.sequence import SequenceMapper


class TestSequenceMapper:
    def test_to_downstream(self):
        pm = SequenceMapper(["us", "eu", "apac"])
        assert pm.to_downstream("us") == "us"
        assert pm.to_downstream("eu") == "eu"

    def test_to_downstream_invalid_key(self):
        pm = SequenceMapper(["us", "eu"])
        with pytest.raises(ValueError, match="not in sequence"):
            pm.to_downstream("apac")

    def test_serialize(self):
        pm = SequenceMapper(["a", "b", "c"])
        assert pm.serialize() == {"sequence": ["a", "b", "c"]}

    def test_deserialize(self):
        pm = SequenceMapper.deserialize({"sequence": ["x", "y"]})
        assert isinstance(pm, SequenceMapper)
        assert pm.sequence == ["x", "y"]

    def test_empty_sequence(self):
        pm = SequenceMapper([])
        assert pm.serialize() == {"sequence": []}
        with pytest.raises(ValueError, match="not in sequence"):
            pm.to_downstream("any")
