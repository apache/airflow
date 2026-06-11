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

from airflow.partition_mappers.identity import IdentityMapper
from airflow.serialization.decoders import decode_partition_mapper
from airflow.serialization.encoders import encode_partition_mapper
from airflow.serialization.enums import Encoding


class TestIdentityMapper:
    def test_to_downstream(self):
        pm = IdentityMapper()
        assert pm.to_downstream("key") == "key"

    def test_serialize(self):
        pm = IdentityMapper()
        assert pm.serialize() == {}

    def test_deserialize(self):
        assert isinstance(IdentityMapper.deserialize({}), IdentityMapper)

    def test_max_downstream_keys_encode_decode_roundtrip(self):
        """max_downstream_keys=5 survives encode_partition_mapper → decode_partition_mapper."""
        mapper = IdentityMapper(max_downstream_keys=5)
        restored = decode_partition_mapper(encode_partition_mapper(mapper))
        assert restored.max_downstream_keys == 5

    def test_max_downstream_keys_absent_from_default_encoded_payload(self):
        """max_downstream_keys must NOT appear in the encoded payload when not set (zero-bloat contract)."""
        mapper = IdentityMapper()
        encoded_var = encode_partition_mapper(mapper)[Encoding.VAR]
        assert "max_downstream_keys" not in encoded_var
