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
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from airflow.utils.hitl_shared_links import HITLSharedLinkManager, hitl_shared_link_manager


class TestHITLSharedLinkManager:
    """Test HITL shared link manager functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.task_instance_id = "test-task-instance-123"
        self.original_enabled = hitl_shared_link_manager.is_enabled()
        self.original_secret_key = hitl_shared_link_manager.secret_key

    def teardown_method(self):
        """Clean up test environment."""
        hitl_shared_link_manager.secret_key = self.original_secret_key

    @patch("airflow.configuration.conf.getboolean")
    def test_is_enabled_default_false(self, mock_getboolean):
        """Test that HITL shared links are disabled by default."""
        mock_getboolean.return_value = False
        assert not hitl_shared_link_manager.is_enabled()

    @patch("airflow.configuration.conf.getboolean")
    def test_is_enabled_when_configured(self, mock_getboolean):
        """Test that HITL shared links can be enabled via configuration."""
        mock_getboolean.return_value = True
        assert hitl_shared_link_manager.is_enabled()

    @patch("airflow.configuration.conf.get")
    def test_secret_key_configuration(self, mock_get):
        """Test secret key configuration handling."""
        mock_get.return_value = "test-secret-key"
        secret_key = hitl_shared_link_manager._get_secret_key()
        assert secret_key == b"test-secret-key"

    @patch("airflow.configuration.conf.get")
    def test_secret_key_default(self, mock_get):
        """Test default secret key when not configured."""
        mock_get.return_value = ""
        secret_key = hitl_shared_link_manager._get_secret_key()
        assert secret_key == b"airflow-hitl-default-key"

    @patch("airflow.configuration.conf.getint")
    def test_expiration_configuration(self, mock_getint):
        """Test expiration hours configuration."""
        mock_getint.return_value = 48
        # Create a new manager instance to pick up the new configuration
        manager = HITLSharedLinkManager()
        assert manager.expiration_hours == 48

    @patch("airflow.configuration.conf.getboolean")
    def test_generate_link_disabled(self, mock_getboolean):
        """Test that link generation fails when feature is disabled."""
        mock_getboolean.return_value = False
        with pytest.raises(ValueError, match="HITL shared links are not enabled"):
            hitl_shared_link_manager.generate_link(self.task_instance_id)

    @patch("airflow.configuration.conf.getboolean")
    def test_generate_redirect_link(self, mock_getboolean):
        """Test redirect link generation."""
        mock_getboolean.return_value = True

        link_url, expires_at = hitl_shared_link_manager.generate_redirect_link(
            task_instance_id=self.task_instance_id, expires_in_hours=1
        )

        assert "/hitl/shared/test-task-instance-123/redirect" in link_url
        assert "payload=" in link_url
        assert "signature=" in link_url
        assert isinstance(expires_at, datetime)
        # Use timezone-aware comparison
        assert expires_at > datetime.now(timezone.utc)

    @patch("airflow.configuration.conf.getboolean")
    def test_generate_action_link(self, mock_getboolean):
        """Test action link generation."""
        mock_getboolean.return_value = True

        link_url, expires_at = hitl_shared_link_manager.generate_action_link(
            task_instance_id=self.task_instance_id, action="approve", expires_in_hours=1
        )

        assert "/hitl/shared/test-task-instance-123" in link_url
        assert "payload=" in link_url
        assert "signature=" in link_url
        assert isinstance(expires_at, datetime)
        # Use timezone-aware comparison
        assert expires_at > datetime.now(timezone.utc)

    @patch("airflow.configuration.conf.getboolean")
    def test_verify_link_success(self, mock_getboolean):
        """Test successful link verification."""
        mock_getboolean.return_value = True

        link_url, _ = hitl_shared_link_manager.generate_action_link(
            task_instance_id=self.task_instance_id, action="approve"
        )

        from urllib.parse import parse_qs, urlparse

        parsed_url = urlparse(link_url)
        params = parse_qs(parsed_url.query)

        payload = params["payload"][0]
        signature = params["signature"][0]

        link_data = hitl_shared_link_manager.verify_link(payload, signature)
        assert link_data["ti_id"] == self.task_instance_id
        assert link_data["action"] == "approve"
        assert link_data["link_type"] == "action"

    def test_verify_link_invalid_signature(self):
        """Test link verification with invalid signature."""
        with pytest.raises(ValueError, match="Invalid link"):
            hitl_shared_link_manager.verify_link("invalid-payload", "invalid-signature")

    def test_verify_link_expired(self):
        """Test link verification with expired link."""
        expired_payload = {
            "ti_id": self.task_instance_id,
            "action": "approve",
            "link_type": "action",
            "expires_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        }

        expired_signature = hitl_shared_link_manager._sign_payload(expired_payload)
        expired_payload_str = json.dumps(expired_payload, separators=(",", ":"))
        expired_payload_b64 = base64.urlsafe_b64encode(expired_payload_str.encode("utf-8")).decode("utf-8")
        expired_signature_b64 = base64.urlsafe_b64encode(expired_signature).decode("utf-8")

        with pytest.raises(ValueError, match="Link has expired"):
            hitl_shared_link_manager.verify_link(expired_payload_b64, expired_signature_b64)

    def test_verify_link_tampered_payload(self):
        """Test link verification with tampered payload."""
        payload = {
            "ti_id": self.task_instance_id,
            "action": "approve",
            "link_type": "action",
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        }

        signature = hitl_shared_link_manager._sign_payload(payload)
        payload_str = json.dumps(payload, separators=(",", ":"))
        payload_b64 = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")
        signature_b64 = base64.urlsafe_b64encode(signature).decode("utf-8")

        tampered_payload_b64 = payload_b64[:-1] + "X"

        with pytest.raises(ValueError, match="Invalid link"):
            hitl_shared_link_manager.verify_link(tampered_payload_b64, signature_b64)

    def test_sign_payload(self):
        """Test payload signing functionality."""
        payload = {"test": "data"}
        signature = hitl_shared_link_manager._sign_payload(payload)

        assert isinstance(signature, bytes)
        assert len(signature) > 0

    @patch("airflow.configuration.conf.getboolean")
    def test_link_type_handling(self, mock_getboolean):
        """Test different link type handling."""
        mock_getboolean.return_value = True

        redirect_link, _ = hitl_shared_link_manager.generate_link(
            task_instance_id=self.task_instance_id, link_type="redirect"
        )
        assert "/redirect" in redirect_link

        action_link, _ = hitl_shared_link_manager.generate_link(
            task_instance_id=self.task_instance_id, link_type="action"
        )
        assert "/redirect" not in action_link

    @patch("airflow.configuration.conf.getboolean")
    def test_custom_expiration(self, mock_getboolean):
        """Test custom expiration time handling."""
        mock_getboolean.return_value = True

        link_url, expires_at = hitl_shared_link_manager.generate_link(
            task_instance_id=self.task_instance_id, expires_in_hours=2
        )

        # Use timezone-aware comparison
        expected_expires = datetime.now(timezone.utc) + timedelta(hours=2)
        assert abs((expires_at - expected_expires).total_seconds()) < 10
