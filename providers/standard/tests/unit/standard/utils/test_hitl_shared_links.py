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
"""Tests for HITL shared links utilities."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from airflow.configuration import conf
from airflow.providers.standard.utils.hitl_shared_links import HITLSharedLinkManager
from airflow.utils import timezone


class TestHITLSharedLinkManager:
    """Test HITL shared link manager functionality."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        with patch.object(conf, "getboolean", return_value=True):
            with patch.object(conf, "getint", return_value=24):
                with patch.object(conf, "get", return_value="http://localhost:8080"):
                    yield

    def test_is_enabled(self):
        """Test is_enabled method."""
        manager = HITLSharedLinkManager()

        with patch.object(conf, "getboolean", return_value=True):
            assert manager.is_enabled() is True

        with patch.object(conf, "getboolean", return_value=False):
            assert manager.is_enabled() is False

    def test_generate_action_link(self):
        """Test generating an action link."""
        manager = HITLSharedLinkManager()

        # Mock task instance
        mock_task_instance = Mock()
        mock_task_instance.uuid = "test-uuid-123"

        # Mock session
        mock_session = Mock()
        mock_session.scalar.return_value = mock_task_instance

        link_data = manager.generate_action_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            action="approve",
            chosen_options=["Approve"],
            params_input={"comment": "Approved"},
            session=mock_session,
        )

        assert link_data["link_type"] == "direct_action"
        assert link_data["action"] == "approve"
        assert link_data["dag_id"] == "test_dag"
        assert link_data["dag_run_id"] == "test_run"
        assert link_data["task_id"] == "test_task"
        assert link_data["try_number"] == 1
        assert "execute?token=" in link_data["url"]
        assert link_data["task_instance_uuid"] == "test-uuid-123"

        # Verify token
        token = link_data["url"].split("token=")[1]
        token_data = self._decode_token(token)
        assert token_data["task_instance_uuid"] == "test-uuid-123"
        assert token_data["type"] == "direct_action"
        assert token_data["action"] == "approve"
        assert token_data["chosen_options"] == ["Approve"]
        assert token_data["params_input"]["comment"] == "Approved"

    def test_generate_redirect_link(self):
        """Test generating a redirect link."""
        manager = HITLSharedLinkManager()

        # Mock task instance
        mock_task_instance = Mock()
        mock_task_instance.uuid = "test-uuid-456"

        # Mock session
        mock_session = Mock()
        mock_session.scalar.return_value = mock_task_instance

        link_data = manager.generate_redirect_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            session=mock_session,
        )

        assert link_data["link_type"] == "ui_redirect"
        assert link_data["action"] is None
        assert link_data["dag_id"] == "test_dag"
        assert link_data["dag_run_id"] == "test_run"
        assert link_data["task_id"] == "test_task"
        assert link_data["try_number"] == 1
        assert "redirect?token=" in link_data["url"]
        assert link_data["task_instance_uuid"] == "test-uuid-456"

        # Verify token
        token = link_data["url"].split("token=")[1]
        token_data = self._decode_token(token)
        assert token_data["task_instance_uuid"] == "test-uuid-456"
        assert token_data["type"] == "ui_redirect"
        assert token_data["action"] is None
        assert token_data["chosen_options"] is None
        assert token_data["params_input"] is None

    def test_generate_link_with_map_index(self):
        """Test generating a link with map index."""
        manager = HITLSharedLinkManager()

        # Mock task instance
        mock_task_instance = Mock()
        mock_task_instance.uuid = "test-uuid-789"

        # Mock session
        mock_session = Mock()
        mock_session.scalar.return_value = mock_task_instance

        link_data = manager.generate_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            map_index=0,
            link_type="direct_action",
            action="approve",
            chosen_options=["Approve"],
            session=mock_session,
        )

        assert link_data["map_index"] == 0
        assert link_data["task_instance_uuid"] == "test-uuid-789"

        # Verify token
        token = link_data["url"].split("token=")[1]
        token_data = self._decode_token(token)
        assert token_data["map_index"] == 0
        assert token_data["task_instance_uuid"] == "test-uuid-789"

    def test_generate_link_custom_expiration(self):
        """Test generating a link with custom expiration."""
        manager = HITLSharedLinkManager()

        # Mock task instance
        mock_task_instance = Mock()
        mock_task_instance.uuid = "test-uuid-123"

        # Mock session
        mock_session = Mock()
        mock_session.scalar.return_value = mock_task_instance

        link_data = manager.generate_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            link_type="direct_action",
            action="approve",
            expiration_hours=48,
            session=mock_session,
        )

        # Verify token expiration
        token = link_data["url"].split("token=")[1]
        token_data = self._decode_token(token)
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        expected_expires_at = timezone.utcnow() + timedelta(hours=48)

        # Allow for small time difference
        assert abs((expires_at - expected_expires_at).total_seconds()) < 5

    def test_generate_link_missing_action(self):
        """Test generating a direct_action link without action."""
        manager = HITLSharedLinkManager()

        with pytest.raises(ValueError, match="Action is required for direct_action-type links"):
            manager.generate_link(
                dag_id="test_dag",
                dag_run_id="test_run",
                task_id="test_task",
                try_number=1,
                link_type="direct_action",
                session=Mock(),
                # Missing action
            )

    def test_generate_link_disabled(self):
        """Test generating a link when feature is disabled."""
        manager = HITLSharedLinkManager()

        with patch.object(conf, "getboolean", return_value=False):
            with pytest.raises(ValueError, match="HITL shared links are not enabled"):
                manager.generate_link(
                    dag_id="test_dag",
                    dag_run_id="test_run",
                    task_id="test_task",
                    try_number=1,
                    link_type="direct_action",
                    action="approve",
                    session=Mock(),
                )

    def test_generate_link_missing_session(self):
        """Test generating a link without session."""
        manager = HITLSharedLinkManager()

        with pytest.raises(ValueError, match="Database session is required to generate shared link"):
            manager.generate_link(
                dag_id="test_dag",
                dag_run_id="test_run",
                task_id="test_task",
                try_number=1,
                link_type="direct_action",
                action="approve",
                session=None,
            )

    def test_verify_link_valid(self):
        """Test verifying a valid link."""
        manager = HITLSharedLinkManager()

        # Generate a link
        mock_task_instance = Mock()
        mock_task_instance.uuid = "test-uuid-123"
        mock_session = Mock()
        mock_session.scalar.return_value = mock_task_instance

        link_data = manager.generate_action_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            action="approve",
            session=mock_session,
        )

        # Extract token
        token = link_data["url"].split("token=")[1]

        # Verify token
        token_data = manager.verify_link(token)
        assert token_data["task_instance_uuid"] == "test-uuid-123"
        assert token_data["type"] == "direct_action"
        assert token_data["action"] == "approve"
        assert token_data["dag_id"] == "test_dag"

    def test_verify_link_expired(self):
        """Test verifying an expired link."""
        manager = HITLSharedLinkManager()

        # Create an expired token
        token_data = {
            "task_instance_uuid": "test-uuid-123",
            "type": "direct_action",
            "dag_id": "test_dag",
            "dag_run_id": "test_run",
            "task_id": "test_task",
            "try_number": 1,
            "map_index": None,
            "action": "approve",
            "chosen_options": ["Approve"],
            "params_input": {},
            "expires_at": (timezone.utcnow() - timedelta(hours=1)).isoformat(),
        }
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()

        with pytest.raises(ValueError, match="Token has expired"):
            manager.verify_link(token)

    def test_verify_link_invalid_json(self):
        """Test verifying a link with invalid JSON."""
        manager = HITLSharedLinkManager()

        with pytest.raises(ValueError, match="Invalid token"):
            manager.verify_link("invalid_token")

    def test_verify_link_missing_fields(self):
        """Test verifying a link with missing fields."""
        manager = HITLSharedLinkManager()

        # Create a token with missing fields
        token_data = {
            "task_instance_uuid": "test-uuid-123",
            "type": "direct_action",
            # Missing required fields
            "expires_at": (timezone.utcnow() + timedelta(hours=1)).isoformat(),
        }
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()

        with pytest.raises(ValueError, match="Missing required field"):
            manager.verify_link(token)

    def test_verify_link_disabled(self):
        """Test verifying a link when feature is disabled."""
        manager = HITLSharedLinkManager()

        with patch.object(conf, "getboolean", return_value=False):
            with pytest.raises(ValueError, match="HITL shared links are not enabled"):
                manager.verify_link("test_token")

    def test_default_expiration_hours(self):
        """Test default expiration hours configuration."""
        manager = HITLSharedLinkManager()

        # Mock task instance
        mock_task_instance = Mock()
        mock_task_instance.uuid = "test-uuid-123"
        mock_session = Mock()
        mock_session.scalar.return_value = mock_task_instance

        with patch.object(conf, "getint", return_value=48):
            link_data = manager.generate_action_link(
                dag_id="test_dag",
                dag_run_id="test_run",
                task_id="test_task",
                try_number=1,
                action="approve",
                session=mock_session,
            )

            token = link_data["url"].split("token=")[1]
            token_data = self._decode_token(token)
            expires_at = datetime.fromisoformat(token_data["expires_at"])
            expected_expires_at = timezone.utcnow() + timedelta(hours=48)

            # Allow for small time difference
            assert abs((expires_at - expected_expires_at).total_seconds()) < 5

    def _decode_token(self, token: str) -> dict:
        """Helper method to decode a token."""
        token_bytes = base64.urlsafe_b64decode(token)
        token_json = token_bytes.decode("utf-8")
        return json.loads(token_json)
