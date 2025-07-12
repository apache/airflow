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
"""Tests for HITL shared links utility."""

from __future__ import annotations

import base64
import json
from datetime import timedelta
from typing import Any
from unittest.mock import patch
from urllib.parse import unquote

import pytest

from airflow.utils import timezone
from airflow.utils.hitl_shared_links import HITLSharedLinkManager


class TestHITLSharedLinkManager:
    """Test HITL shared link manager functionality."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.manager = HITLSharedLinkManager()
        self.test_secret_key = "test-secret-key-12345"
        self.test_dag_id = "test_dag"
        self.test_dag_run_id = "test_run_2024-01-01T00:00:00+00:00"
        self.test_task_id = "test_task"

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_is_enabled_when_disabled(self, mock_conf: Any) -> None:
        """Test is_enabled returns False when feature is disabled."""
        mock_conf.getboolean.return_value = False
        assert not self.manager.is_enabled()

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_is_enabled_when_enabled(self, mock_conf: Any) -> None:
        """Test is_enabled returns True when feature is enabled."""
        mock_conf.getboolean.return_value = True
        assert self.manager.is_enabled()

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_signature(self, mock_conf: Any) -> None:
        """Test signature generation."""
        # Mock the configuration calls in __init__
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        # Create a new manager instance with mocked config
        manager = HITLSharedLinkManager()
        payload = '{"test": "data"}'
        signature = manager._generate_signature(payload)

        assert isinstance(signature, str)
        base64.urlsafe_b64decode(signature)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_signature_no_secret_key(self, mock_conf: Any) -> None:
        """Test signature generation fails when no secret key is configured."""
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): "",
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        payload = '{"test": "data"}'

        with pytest.raises(ValueError, match="HITL shared link secret key is not configured"):
            manager._generate_signature(payload)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_signature_valid(self, mock_conf: Any) -> None:
        """Test signature verification with valid signature."""
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        payload = '{"test": "data"}'
        signature = manager._generate_signature(payload)

        assert manager._verify_signature(payload, signature)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_signature_invalid(self, mock_conf: Any) -> None:
        """Test signature verification with invalid signature."""
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        payload = '{"test": "data"}'
        invalid_signature = "invalid_signature"

        assert not manager._verify_signature(payload, invalid_signature)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_action_type(self, mock_conf: Any) -> None:
        """Test generating an action-type link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        link_data = manager.generate_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            link_type="action",
            action="approve",
        )

        assert (
            link_data["task_instance_id"]
            == f"{self.test_dag_id}.{self.test_dag_run_id}.{self.test_task_id}.-1"
        )
        assert link_data["link_type"] == "action"
        assert link_data["action"] == "approve"
        assert "link_url" in link_data
        assert "expires_at" in link_data

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_redirect_type(self, mock_conf: Any) -> None:
        """Test generating a redirect-type link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        link_data = manager.generate_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            link_type="redirect",
        )

        assert link_data["link_type"] == "redirect"
        assert link_data["action"] is None

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_with_map_index(self, mock_conf: Any) -> None:
        """Test generating a link for a mapped task."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        link_data = manager.generate_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            map_index=5,
            link_type="action",
            action="reject",
        )

        assert (
            link_data["task_instance_id"]
            == f"{self.test_dag_id}.{self.test_dag_run_id}.{self.test_task_id}.5"
        )
        assert link_data["action"] == "reject"

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_feature_disabled(self, mock_conf: Any) -> None:
        """Test generating a link when feature is disabled."""
        mock_conf.getboolean.return_value = False

        with pytest.raises(ValueError, match="HITL shared links are not enabled"):
            self.manager.generate_link(
                dag_id=self.test_dag_id,
                dag_run_id=self.test_dag_run_id,
                task_id=self.test_task_id,
            )

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_action_required(self, mock_conf: Any) -> None:
        """Test generating an action link without action fails."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        with pytest.raises(ValueError, match="Action is required for action-type links"):
            manager.generate_link(
                dag_id=self.test_dag_id,
                dag_run_id=self.test_dag_run_id,
                task_id=self.test_task_id,
                link_type="action",
                action=None,
            )

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_link_valid(self, mock_conf: Any) -> None:
        """Test verifying a valid link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        link_data = manager.generate_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            link_type="action",
            action="approve",
        )

        url = link_data["link_url"]
        # Extract query parameters more robustly
        query_part = url.split("?")[1]
        params = {}
        for param in query_part.split("&"):
            if "=" in param:
                key, value = param.split("=", 1)
                params[key] = unquote(value)

        payload = params.get("payload", "")
        signature = params.get("signature", "")

        assert payload, "Failed to extract payload from URL"
        assert signature, "Failed to extract signature from URL"
        decoded_data = manager.verify_link(payload, signature)

        assert decoded_data["dag_id"] == self.test_dag_id
        assert decoded_data["dag_run_id"] == self.test_dag_run_id
        assert decoded_data["task_id"] == self.test_task_id
        assert decoded_data["link_type"] == "action"
        assert decoded_data["action"] == "approve"

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_link_invalid_signature(self, mock_conf: Any) -> None:
        """Test verifying a link with invalid signature."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()

        # Create a valid payload but with invalid signature
        payload_data = {
            "dag_id": self.test_dag_id,
            "dag_run_id": self.test_dag_run_id,
            "task_id": self.test_task_id,
            "link_type": "action",
            "action": "approve",
            "expires_at": timezone.utcnow().isoformat(),
        }
        payload_str = json.dumps(payload_data, sort_keys=True)
        valid_payload = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")
        invalid_signature = "invalid_signature"

        with pytest.raises(ValueError, match="Invalid signature"):
            manager.verify_link(valid_payload, invalid_signature)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_link_expired(self, mock_conf: Any) -> None:
        """Test verifying an expired link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        expired_time = timezone.utcnow() - timedelta(hours=1)
        payload_data = {
            "dag_id": self.test_dag_id,
            "dag_run_id": self.test_dag_run_id,
            "task_id": self.test_task_id,
            "link_type": "action",
            "action": "approve",
            "expires_at": expired_time.isoformat(),
        }

        payload_str = json.dumps(payload_data, sort_keys=True)
        signature = manager._generate_signature(payload_str)
        encoded_payload = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")

        with pytest.raises(ValueError, match="Link has expired"):
            manager.verify_link(encoded_payload, signature)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_link_feature_disabled(self, mock_conf: Any) -> None:
        """Test verifying a link when feature is disabled."""
        mock_conf.getboolean.return_value = False

        with pytest.raises(ValueError, match="HITL shared links are not enabled"):
            self.manager.verify_link("payload", "signature")

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_redirect_link(self, mock_conf: Any) -> None:
        """Test generating a redirect link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        link_data = manager.generate_redirect_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
        )

        assert link_data["link_type"] == "redirect"
        assert link_data["action"] is None

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_action_link(self, mock_conf: Any) -> None:
        """Test generating an action link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        link_data = manager.generate_action_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            action="approve",
        )

        assert link_data["link_type"] == "action"
        assert link_data["action"] == "approve"

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_custom_expiration_time(self, mock_conf: Any) -> None:
        """Test generating a link with custom expiration time."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        link_data = manager.generate_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            link_type="redirect",
            expires_in_hours=48,
        )

        expected_expires_at = timezone.utcnow() + timedelta(hours=48)
        actual_expires_at = link_data["expires_at"]

        time_diff = abs((expected_expires_at - actual_expires_at).total_seconds())
        assert time_diff < 60

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_custom_base_url(self, mock_conf: Any) -> None:
        """Test generating a link with custom base URL."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        custom_base_url = "https://my-airflow.company.com"
        link_data = manager.generate_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            link_type="redirect",
            base_url=custom_base_url,
        )

        assert link_data["link_url"].startswith(custom_base_url)
