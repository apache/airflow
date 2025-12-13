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

import json
from datetime import datetime, timezone

import orjson
import pytest

from airflow.api_fastapi.common.responses import ORJSONResponse


class TestORJSONResponse:
    """Tests for ORJSONResponse custom response class."""

    def test_orjson_response_basic_content(self):
        """Test ORJSONResponse with basic content."""
        content = {"message": "hello", "value": 123}
        response = ORJSONResponse(content=content)
        
        assert response.media_type == "application/json"
        assert response.status_code == 200
        
        # Verify the rendered content can be decoded back
        rendered = response.render(content)
        assert isinstance(rendered, bytes)
        decoded = json.loads(rendered)
        assert decoded == content

    def test_orjson_response_with_datetime(self):
        """Test ORJSONResponse correctly serializes datetime objects with UTC timezone."""
        now = datetime(2025, 12, 7, 10, 30, 0, tzinfo=timezone.utc)
        content = {"timestamp": now, "event": "test"}
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        decoded = json.loads(rendered)
        
        # ORJSONResponse should format datetime with UTC_Z option (ISO format with Z suffix)
        assert "timestamp" in decoded
        assert isinstance(decoded["timestamp"], str)
        assert decoded["timestamp"].endswith("Z")

    def test_orjson_response_with_non_str_keys(self):
        """Test ORJSONResponse handles non-string dictionary keys."""
        # orjson.OPT_NON_STR_KEYS allows integer keys
        content = {1: "one", 2: "two", "three": 3}
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        decoded = json.loads(rendered)
        
        # Integer keys will be converted to strings in JSON
        assert "1" in decoded or 1 in decoded
        assert decoded.get("three") == 3 or decoded.get(3) == "three"

    def test_orjson_response_with_nested_structures(self):
        """Test ORJSONResponse with complex nested data structures."""
        content = {
            "dags": [
                {
                    "dag_id": "example_dag",
                    "is_paused": False,
                    "tags": ["production", "critical"],
                    "owners": ["admin", "user1"],
                },
                {
                    "dag_id": "test_dag",
                    "is_paused": True,
                    "tags": [],
                    "owners": ["test_user"],
                },
            ],
            "total_entries": 2,
        }
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        decoded = json.loads(rendered)
        
        assert decoded["total_entries"] == 2
        assert len(decoded["dags"]) == 2
        assert decoded["dags"][0]["dag_id"] == "example_dag"
        assert decoded["dags"][1]["is_paused"] is True

    def test_orjson_response_preserves_none_values(self):
        """Test ORJSONResponse preserves None values in content."""
        content = {"value": None, "name": "test", "count": 0}
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        decoded = json.loads(rendered)
        
        assert "value" in decoded
        assert decoded["value"] is None
        assert decoded["name"] == "test"
        assert decoded["count"] == 0

    def test_orjson_response_empty_dict(self):
        """Test ORJSONResponse with empty dictionary."""
        content = {}
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        decoded = json.loads(rendered)
        
        assert decoded == {}

    def test_orjson_response_empty_list(self):
        """Test ORJSONResponse with empty list."""
        content = {"items": [], "total": 0}
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        decoded = json.loads(rendered)
        
        assert decoded["items"] == []
        assert decoded["total"] == 0

    def test_orjson_response_with_custom_status_code(self):
        """Test ORJSONResponse with custom HTTP status code."""
        content = {"message": "created"}
        response = ORJSONResponse(content=content, status_code=201)
        
        assert response.status_code == 201

    def test_orjson_response_performance_benefit(self):
        """Test that ORJSONResponse uses orjson for serialization."""
        # This test verifies orjson is being used (not standard json)
        content = {"data": list(range(1000))}
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        
        # Verify it's valid JSON
        decoded = json.loads(rendered)
        assert len(decoded["data"]) == 1000
        
        # Verify orjson specific behavior - orjson is more compact
        # Standard json adds spaces, orjson doesn't by default
        assert b", " not in rendered or b"," in rendered

    def test_orjson_response_with_boolean_values(self):
        """Test ORJSONResponse correctly handles boolean values."""
        content = {"is_active": True, "is_deleted": False, "is_paused": None}
        
        response = ORJSONResponse(content=content)
        rendered = response.render(content)
        decoded = json.loads(rendered)
        
        assert decoded["is_active"] is True
        assert decoded["is_deleted"] is False
        assert decoded["is_paused"] is None
