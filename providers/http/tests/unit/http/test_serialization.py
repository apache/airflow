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

import base64
import pickle

import pytest
import requests

from airflow.providers.http.triggers.http import HttpResponseSerializer


class TestHttpResponseSerializer:
    def test_serialize_basic_response(self):
        """Test serialization of a basic HTTP response."""
        response = requests.Response()
        response.status_code = 200
        response._content = b"test content"
        response.url = "https://example.com"
        response.reason = "OK"
        response.encoding = "utf-8"

        serialized = HttpResponseSerializer.serialize(response)

        assert serialized["status_code"] == 200
        assert serialized["url"] == "https://example.com"
        assert serialized["reason"] == "OK"
        assert serialized["encoding"] == "utf-8"
        assert base64.standard_b64decode(serialized["content"]) == b"test content"

    def test_deserialize_basic_response(self):
        """Test deserialization of a basic HTTP response."""
        data = {
            "status_code": 200,
            "headers": {"Content-Type": "application/json"},
            "content": base64.standard_b64encode(b"test content").decode("ascii"),
            "url": "https://example.com",
            "reason": "OK",
            "encoding": "utf-8",
            "cookies": {},
            "history": [],
        }

        response = HttpResponseSerializer.deserialize(data)

        assert response.status_code == 200
        assert response.url == "https://example.com"
        assert response.reason == "OK"
        assert response.encoding == "utf-8"
        assert response.content == b"test content"
        assert response.headers["Content-Type"] == "application/json"

    def test_serialize_deserialize_roundtrip(self):
        """Test that serialization and deserialization are inverse operations."""
        original = requests.Response()
        original.status_code = 404
        original._content = b"Not Found"
        original.url = "https://example.com/missing"
        original.reason = "Not Found"
        original.encoding = "utf-8"

        serialized = HttpResponseSerializer.serialize(original)
        restored = HttpResponseSerializer.deserialize(serialized)

        assert restored.status_code == original.status_code
        assert restored.content == original.content
        assert restored.url == original.url
        assert restored.reason == original.reason
        assert restored.encoding == original.encoding

    def test_serialize_with_cookies(self):
        """Test serialization of response with cookies."""
        response = requests.Response()
        response._content = b"content"
        response.cookies.set("session_id", "abc123")
        response.cookies.set("user_pref", "dark_mode")

        serialized = HttpResponseSerializer.serialize(response)
        restored = HttpResponseSerializer.deserialize(serialized)

        assert restored.cookies["session_id"] == "abc123"
        assert restored.cookies["user_pref"] == "dark_mode"

    def test_serialize_with_history(self):
        """Test serialization of response with redirect history."""
        redirect_response = requests.Response()
        redirect_response.status_code = 301
        redirect_response._content = b"Moved"
        redirect_response.url = "https://example.com/old"

        final_response = requests.Response()
        final_response.status_code = 200
        final_response._content = b"Final content"
        final_response.url = "https://example.com/new"
        final_response.history = [redirect_response]

        serialized = HttpResponseSerializer.serialize(final_response)
        restored = HttpResponseSerializer.deserialize(serialized)

        assert len(restored.history) == 1
        assert restored.history[0].status_code == 301
        assert restored.history[0].url == "https://example.com/old"
        assert restored.status_code == 200
        assert restored.url == "https://example.com/new"

    def test_serialize_binary_content(self):
        """Test serialization of response with binary content."""
        binary_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
        response = requests.Response()
        response._content = binary_data

        serialized = HttpResponseSerializer.serialize(response)
        restored = HttpResponseSerializer.deserialize(serialized)

        assert restored.content == binary_data

    def test_deserialize_rejects_string(self):
        """Test that deserialize rejects string input (legacy pickle format)."""
        legacy_payload = base64.standard_b64encode(pickle.dumps(requests.Response())).decode("ascii")

        with pytest.raises(TypeError, match="Response data must be a dict, got str"):
            HttpResponseSerializer.deserialize(legacy_payload)

    def test_deserialize_rejects_invalid_type(self):
        """Test that deserialize rejects non-dict, non-string types."""
        with pytest.raises(TypeError, match="Expected dict, got list"):
            HttpResponseSerializer.deserialize([])

        with pytest.raises(TypeError, match="Expected dict, got int"):
            HttpResponseSerializer.deserialize(42)

    def test_response_text_property(self):
        """Test that .text property works on deserialized response."""
        response = requests.Response()
        response._content = b"Hello World"
        response.encoding = "utf-8"

        serialized = HttpResponseSerializer.serialize(response)
        restored = HttpResponseSerializer.deserialize(serialized)

        assert restored.text == "Hello World"

    def test_response_json_method(self):
        """Test that .json() method works on deserialized response."""
        response = requests.Response()
        response._content = b'{"key": "value"}'
        response.encoding = "utf-8"

        serialized = HttpResponseSerializer.serialize(response)
        restored = HttpResponseSerializer.deserialize(serialized)

        assert restored.json() == {"key": "value"}

    def test_response_ok_property(self):
        """Test that .ok property works on deserialized response."""
        response_ok = requests.Response()
        response_ok.status_code = 200

        response_error = requests.Response()
        response_error.status_code = 500

        serialized_ok = HttpResponseSerializer.serialize(response_ok)
        restored_ok = HttpResponseSerializer.deserialize(serialized_ok)
        assert restored_ok.ok is True

        serialized_error = HttpResponseSerializer.serialize(response_error)
        restored_error = HttpResponseSerializer.deserialize(serialized_error)
        assert restored_error.ok is False

    def test_response_raise_for_status(self):
        """Test that .raise_for_status() works on deserialized response."""
        response_ok = requests.Response()
        response_ok.status_code = 200

        response_error = requests.Response()
        response_error.status_code = 404

        serialized_ok = HttpResponseSerializer.serialize(response_ok)
        restored_ok = HttpResponseSerializer.deserialize(serialized_ok)
        restored_ok.raise_for_status()  # Should not raise

        serialized_error = HttpResponseSerializer.serialize(response_error)
        restored_error = HttpResponseSerializer.deserialize(serialized_error)

        with pytest.raises(requests.HTTPError):
            restored_error.raise_for_status()
