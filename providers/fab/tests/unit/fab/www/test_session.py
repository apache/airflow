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

import msgspec
import pytest
from flask_babel import LazyString

from airflow.providers.fab.www.session import _LazySafeSerializer


class TestLazySafeSerializer:
    def setup_method(self):
        self.serializer = _LazySafeSerializer()

    def test_dumps_returns_str(self):
        """dumps must return a str so FAB 3.6+ cookie creation does not fail on bytes."""
        result = self.serializer.dumps({"key": "value"})
        assert isinstance(result, str), f"Expected str, got {type(result)}"

    def test_dumps_and_loads_roundtrip(self):
        """Data serialized by dumps must be recoverable by loads."""
        data = {"user_id": 42, "role": "Admin", "active": True}
        encoded = self.serializer.dumps(data)
        decoded = self.serializer.loads(encoded)
        assert decoded == data

    def test_dumps_encodes_lazy_string(self):
        """LazyString values must be converted to plain str during serialization."""
        lazy = LazyString(lambda: "translated_value")
        encoded = self.serializer.dumps({"label": lazy})
        decoded = self.serializer.loads(encoded)
        assert decoded["label"] == "translated_value"

    def test_loads_falls_back_to_msgpack_for_legacy_data(self):
        """loads must transparently handle sessions stored in the old msgpack format."""
        legacy_data = {"user_id": 1, "legacy": True}
        msgpack_bytes = msgspec.msgpack.Encoder().encode(legacy_data)
        decoded = self.serializer.loads(msgpack_bytes)
        assert decoded == legacy_data

    def test_loads_prefers_json_over_msgpack(self):
        """When data is valid JSON, loads must use the JSON decoder (not msgpack)."""
        data = {"source": "json"}
        json_bytes = msgspec.json.Encoder().encode(data)
        # Ensure json_bytes is valid for loads regardless of str/bytes input
        decoded = self.serializer.loads(json_bytes)
        assert decoded == data

    def test_dumps_handles_empty_session(self):
        """An empty session dict must serialize and deserialize cleanly."""
        encoded = self.serializer.dumps({})
        decoded = self.serializer.loads(encoded)
        assert decoded == {}

    def test_dumps_handles_nested_data(self):
        """Nested dicts and lists must survive the roundtrip."""
        data = {"prefs": {"theme": "dark", "items": [1, 2, 3]}}
        encoded = self.serializer.dumps(data)
        decoded = self.serializer.loads(encoded)
        assert decoded == data

    def test_encode_alias_matches_dumps(self):
        """encode is documented as an alias for dumps — both must produce identical output."""
        data = {"alias_check": True}
        assert self.serializer.encode(data) == self.serializer.dumps(data)

    def test_decode_alias_matches_loads(self):
        """decode is documented as an alias for loads — both must produce identical output."""
        encoded = self.serializer.dumps({"alias_check": True})
        assert self.serializer.decode(encoded) == self.serializer.loads(encoded)
