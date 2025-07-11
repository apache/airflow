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
import datetime
import json
from datetime import timedelta
from typing import Any
from unittest.mock import patch
from urllib.parse import unquote

import pytest

from airflow.exceptions import AirflowConfigException
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
            ("api", "secret_key"): self.test_secret_key,
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

        def mock_get(section, key, fallback=None):
            if section == "api" and key == "secret_key":
                raise AirflowConfigException("section/key [api/secret_key] not found in config")
            if section == "api" and key == "hitl_shared_link_expiration_hours":
                return 24
            return fallback

        mock_conf.get.side_effect = mock_get
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        with pytest.raises(AirflowConfigException):
            HITLSharedLinkManager()

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_signature_valid(self, mock_conf: Any) -> None:
        """Test signature verification with valid signature."""
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
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
            ("api", "secret_key"): self.test_secret_key,
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
            ("api", "secret_key"): self.test_secret_key,
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
            try_number=1,
        )

        expected_task_instance_id = f"{self.test_dag_id}.{self.test_dag_run_id}.{self.test_task_id}.-1"
        actual_task_instance_id = f"{link_data['dag_id']}.{link_data['dag_run_id']}.{link_data['task_id']}.{link_data['map_index'] or -1}"
        assert actual_task_instance_id == expected_task_instance_id
        assert link_data["link_type"] == "action"
        assert link_data["action"] == "approve"
        assert "url" in link_data
        assert "expires_at" in link_data

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_redirect_type(self, mock_conf: Any) -> None:
        """Test generating a redirect-type link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
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
            try_number=1,
        )

        assert link_data["link_type"] == "redirect"
        assert link_data["action"] is None

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_with_map_index(self, mock_conf: Any) -> None:
        """Test generating a link for a mapped task."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
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
            try_number=1,
        )

        expected_task_instance_id = f"{self.test_dag_id}.{self.test_dag_run_id}.{self.test_task_id}.5"
        actual_task_instance_id = f"{link_data['dag_id']}.{link_data['dag_run_id']}.{link_data['task_id']}.{link_data['map_index'] or -1}"
        assert actual_task_instance_id == expected_task_instance_id
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
                try_number=1,
            )

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_action_required(self, mock_conf: Any) -> None:
        """Test generating an action link without action fails."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
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
                try_number=1,
            )

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_link_valid(self, mock_conf: Any) -> None:
        """Test verifying a valid link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
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
            try_number=1,
        )

        url = link_data["url"]
        query_params = url.split("?")[1].split("&")
        payload = None
        signature = None
        for param in query_params:
            if param.startswith("payload="):
                payload = param.split("=")[1]
            elif param.startswith("signature="):
                signature = param.split("=")[1]

        assert payload is not None
        assert signature is not None

        payload = unquote(payload)
        signature = unquote(signature)

        verified_data = manager.verify_link(payload, signature)
        assert verified_data.dag_id == self.test_dag_id
        assert verified_data.dag_run_id == self.test_dag_run_id
        assert verified_data.task_id == self.test_task_id
        assert verified_data.link_type == "action"
        assert verified_data.action == "approve"

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_link_invalid_signature(self, mock_conf: Any) -> None:
        """Test verifying a link with invalid signature."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        payload = "dGVzdA=="  # base64 encoded "test"
        invalid_signature = "invalid_signature"

        with pytest.raises(ValueError, match="Invalid signature"):
            manager.verify_link(payload, invalid_signature)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_verify_link_expired(self, mock_conf: Any) -> None:
        """Test verifying an expired link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()

        payload_data = {
            "dag_id": self.test_dag_id,
            "dag_run_id": self.test_dag_run_id,
            "task_id": self.test_task_id,
            "try_number": 1,
            "map_index": None,
            "link_type": "action",
            "action": "approve",
            "expires_at": (timezone.utcnow() - timedelta(hours=1)).isoformat(),
        }
        payload_str = json.dumps(payload_data)
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
            ("api", "secret_key"): self.test_secret_key,
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
            try_number=1,
        )

        assert link_data["link_type"] == "redirect"
        assert link_data["action"] is None

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_action_link(self, mock_conf: Any) -> None:
        """Test generating an action link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
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
            try_number=1,
        )

        assert link_data["link_type"] == "action"
        assert link_data["action"] == "approve"

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_custom_expiration_time(self, mock_conf: Any) -> None:
        """Test generating a link with custom expiration time."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
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
            expires_in_hours=48,
            try_number=1,
        )

        expected_expires_at = timezone.utcnow() + timedelta(hours=48)
        actual_expires_at = datetime.datetime.fromisoformat(link_data["expires_at"])
        time_diff = abs((expected_expires_at - actual_expires_at).total_seconds())
        assert time_diff < 60

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_custom_base_url(self, mock_conf: Any) -> None:
        """Test generating a link with custom base URL."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()
        custom_base_url = "https://custom-airflow.example.com"
        link_data = manager.generate_link(
            dag_id=self.test_dag_id,
            dag_run_id=self.test_dag_run_id,
            task_id=self.test_task_id,
            link_type="action",
            action="approve",
            base_url=custom_base_url,
            try_number=1,
        )

        assert link_data["url"].startswith(custom_base_url)

    @patch("airflow.utils.hitl_shared_links.conf")
    def test_generate_link_no_base_url_configured(self, mock_conf: Any) -> None:
        """Test generating a link fails when base_url is not configured."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("api", "secret_key"): self.test_secret_key,
            ("api", "hitl_shared_link_expiration_hours"): 24,
            ("api", "base_url"): "",  # Empty base_url
        }.get((section, key), fallback)
        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("api", "hitl_shared_link_expiration_hours"): 24,
        }.get((section, key), fallback)

        manager = HITLSharedLinkManager()

        with pytest.raises(ValueError, match="API base_url is not configured"):
            manager.generate_link(
                dag_id=self.test_dag_id,
                dag_run_id=self.test_dag_run_id,
                task_id=self.test_task_id,
                link_type="action",
                action="approve",
                try_number=1,
            )
