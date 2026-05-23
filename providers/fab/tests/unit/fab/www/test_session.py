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

from unittest.mock import MagicMock, patch

from flask_babel import LazyString

from airflow.providers.fab.www.session import (
    AirflowSecureCookieSessionInterface,
    _LazySafeSerializer,
)


class TestLazySafeSerializer:
    def setup_method(self):
        self.serializer = _LazySafeSerializer()

    def test_dumps_returns_bytes(self):
        result = self.serializer.dumps({"key": "value"})
        assert isinstance(result, bytes)

    def test_roundtrip(self):
        data = {"user_id": 42, "role": "Admin", "active": True}
        encoded = self.serializer.dumps(data)
        decoded = self.serializer.loads(encoded)
        assert decoded == data

    def test_lazy_string_converted(self):
        lazy = LazyString(lambda: "translated")
        encoded = self.serializer.dumps({"label": lazy})
        decoded = self.serializer.loads(encoded)
        assert decoded["label"] == "translated"

    def test_empty_session(self):
        encoded = self.serializer.dumps({})
        decoded = self.serializer.loads(encoded)
        assert decoded == {}

    def test_nested_data(self):
        data = {"prefs": {"theme": "dark", "items": [1, 2, 3]}}
        encoded = self.serializer.dumps(data)
        decoded = self.serializer.loads(encoded)
        assert decoded == data

    def test_encode_decode_aliases(self):
        data = {"alias": True}
        assert self.serializer.encode(data) == self.serializer.dumps(data)

        encoded = self.serializer.dumps(data)
        assert self.serializer.decode(encoded) == self.serializer.loads(encoded)


class TestAirflowSecureCookieSessionInterface:
    """Test that save_session coerces bytes cookie values to str for Werkzeug 3.0+."""

    def test_save_session_decodes_bytes_cookie_value(self):
        interface = AirflowSecureCookieSessionInterface()
        app = MagicMock()
        session = MagicMock()
        response = MagicMock()

        captured_values = []

        def track_set_cookie(key, value="", **kwargs):
            captured_values.append(value)

        response.set_cookie = track_set_cookie

        # Patch super().save_session to simulate Flask calling set_cookie with bytes
        with patch.object(
            type(interface).__mro__[1],
            "save_session",
            side_effect=lambda self, app, session, response: response.set_cookie(
                "session", b"signed-bytes-value"
            ),
        ):
            interface.save_session(app, session, response)

        assert len(captured_values) == 1
        assert isinstance(captured_values[0], str)
        assert captured_values[0] == "signed-bytes-value"

    def test_save_session_passes_str_value_unchanged(self):
        interface = AirflowSecureCookieSessionInterface()
        app = MagicMock()
        session = MagicMock()
        response = MagicMock()

        captured_values = []

        def track_set_cookie(key, value="", **kwargs):
            captured_values.append(value)

        response.set_cookie = track_set_cookie

        with patch.object(
            type(interface).__mro__[1],
            "save_session",
            side_effect=lambda self, app, session, response: response.set_cookie(
                "session", "already-a-string"
            ),
        ):
            interface.save_session(app, session, response)

        assert len(captured_values) == 1
        assert isinstance(captured_values[0], str)
        assert captured_values[0] == "already-a-string"
