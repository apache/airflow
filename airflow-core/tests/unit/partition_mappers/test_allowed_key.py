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

from airflow.partition_mappers.allowed_key import AllowedKeyMapper
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper
from airflow.serialization.enums import Encoding


class TestAllowedKeyMapper:
    def test_to_downstream(self):
        pm = AllowedKeyMapper(["us", "eu", "apac"])
        assert pm.to_downstream("us") == "us"
        assert pm.to_downstream("eu") == "eu"

    def test_to_downstream_invalid_key(self):
        pm = AllowedKeyMapper(["us", "eu"])
        with pytest.raises(ValueError, match="not in allowed keys"):
            pm.to_downstream("apac")

    def test_serialize(self):
        pm = AllowedKeyMapper(["a", "b", "c"])
        assert pm.serialize() == {"allowed_keys": ["a", "b", "c"]}

    def test_deserialize(self):
        pm = AllowedKeyMapper.deserialize({"allowed_keys": ["x", "y"]})
        assert isinstance(pm, AllowedKeyMapper)
        assert pm.allowed_keys == ["x", "y"]

    def test_empty_allowed_keys(self):
        pm = AllowedKeyMapper([])
        assert pm.serialize() == {"allowed_keys": []}
        with pytest.raises(ValueError, match="not in allowed keys"):
            pm.to_downstream("any")

    def test_max_downstream_keys_encode_decode_roundtrip(self):
        """max_downstream_keys=5 survives encode_partition_mapper → decode_partition_mapper."""

        mapper = AllowedKeyMapper(["us", "eu", "apac", "latam", "africa"], max_downstream_keys=5)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert restored.max_downstream_keys == 5

    def test_max_downstream_keys_absent_from_default_encoded_payload(self):
        """max_downstream_keys must NOT appear in the encoded payload when not set (zero-bloat contract)."""

        mapper = AllowedKeyMapper(["us", "eu"])
        encoded_var = encode_partition_mapper(mapper)[Encoding.VAR]
        assert "max_downstream_keys" not in encoded_var
