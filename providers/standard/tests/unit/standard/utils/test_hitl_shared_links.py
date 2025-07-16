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
from datetime import datetime
from unittest.mock import patch

import pytest

from airflow.api_fastapi.core_api.datamodels.hitl import HITLSharedLinkPayload
from airflow.providers.standard.utils.hitl_shared_links import HITLSharedLinkManager


class TestHITLSharedLinkManager:
    """Test HITLSharedLinkManager functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.manager = HITLSharedLinkManager()

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_is_enabled_true(self, mock_conf):
        """Test that is_enabled returns True when feature is enabled."""
        mock_conf.getboolean.return_value = True
        assert self.manager.is_enabled() is True
        mock_conf.getboolean.assert_called_once_with("api", "hitl_enable_shared_links", fallback=False)

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_is_enabled_false(self, mock_conf):
        """Test that is_enabled returns False when feature is disabled."""
        mock_conf.getboolean.return_value = False
        assert self.manager.is_enabled() is False

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_generate_signature_with_secret_key(self, mock_conf):
        """Test signature generation with valid secret key."""
        mock_conf.get.return_value = "test-secret-key"
        payload = '{"test": "data"}'
        signature = self.manager._generate_signature(payload)
        assert isinstance(signature, str)
        assert len(signature) > 0

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_generate_signature_without_secret_key(self, mock_conf):
        """Test signature generation fails without secret key."""
        mock_conf.get.return_value = None
        manager = HITLSharedLinkManager()
        payload = '{"test": "data"}'
        with pytest.raises(ValueError, match="API secret key is not configured"):
            manager._generate_signature(payload)

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_verify_signature_valid(self, mock_conf):
        """Test signature verification with valid signature."""
        mock_conf.get.return_value = "test-secret-key"
        payload = '{"test": "data"}'
        signature = self.manager._generate_signature(payload)
        assert self.manager._verify_signature(payload, signature) is True

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_verify_signature_invalid(self, mock_conf):
        """Test signature verification with invalid signature."""
        mock_conf.get.return_value = "test-secret-key"
        payload = '{"test": "data"}'
        invalid_signature = "invalid-signature"
        assert self.manager._verify_signature(payload, invalid_signature) is False

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    @patch("airflow.providers.standard.utils.hitl_shared_links.timezone")
    def test_generate_link_action_type(self, mock_timezone, mock_conf):
        """Test generating action-type link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key: {
            ("api", "secret_key"): "test-secret-key",
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key))
        mock_conf.getint.return_value = 24
        mock_timezone.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = self.manager.generate_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            link_type="action",
            action="approve",
        )

        assert result["dag_id"] == "test_dag"
        assert result["dag_run_id"] == "test_run"
        assert result["task_id"] == "test_task"
        assert result["try_number"] == 1
        assert result["link_type"] == "action"
        assert result["action"] == "approve"
        assert "url" in result
        assert "expires_at" in result

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_generate_link_disabled(self, mock_conf):
        """Test generating link when feature is disabled."""
        mock_conf.getboolean.return_value = False
        with pytest.raises(ValueError, match="HITL shared links are not enabled"):
            self.manager.generate_link(
                dag_id="test_dag",
                dag_run_id="test_run",
                task_id="test_task",
                try_number=1,
                link_type="action",
                action="approve",
            )

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_generate_link_action_type_without_action(self, mock_conf):
        """Test generating action-type link without action fails."""
        mock_conf.getboolean.return_value = True
        with pytest.raises(ValueError, match="Action is required for action-type links"):
            self.manager.generate_link(
                dag_id="test_dag",
                dag_run_id="test_run",
                task_id="test_task",
                try_number=1,
                link_type="action",
                action=None,
            )

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_generate_link_without_base_url(self, mock_conf):
        """Test generating link without base_url configuration fails."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key: {
            ("api", "secret_key"): "test-secret-key",
            ("api", "base_url"): None,
        }.get((section, key))
        mock_conf.getint.return_value = 24

        with pytest.raises(ValueError, match="API base_url is not configured"):
            self.manager.generate_link(
                dag_id="test_dag",
                dag_run_id="test_run",
                task_id="test_task",
                try_number=1,
                link_type="redirect",
            )

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    @patch("airflow.providers.standard.utils.hitl_shared_links.timezone")
    def test_generate_link_with_mapped_task(self, mock_timezone, mock_conf):
        """Test generating link for mapped task."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key: {
            ("api", "secret_key"): "test-secret-key",
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key))
        mock_conf.getint.return_value = 24
        mock_timezone.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = self.manager.generate_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            map_index=5,
            link_type="redirect",
        )

        assert result["map_index"] == 5
        assert "/5" in result["url"]

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    @patch("airflow.providers.standard.utils.hitl_shared_links.timezone")
    def test_verify_link_valid(self, mock_timezone, mock_conf):
        """Test verifying valid link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.return_value = "test-secret-key"
        mock_timezone.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)

        expires_at = datetime(2023, 1, 2, 12, 0, 0)
        payload_data = HITLSharedLinkPayload(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            map_index=None,
            link_type="action",
            action="approve",
            expires_at=expires_at.isoformat(),
        )

        payload_str = payload_data.model_dump_json()
        signature = self.manager._generate_signature(payload_str)
        encoded_payload = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")

        result = self.manager.verify_link(encoded_payload, signature)
        assert result.dag_id == "test_dag"
        assert result.dag_run_id == "test_run"
        assert result.task_id == "test_task"
        assert result.try_number == 1
        assert result.link_type == "action"
        assert result.action == "approve"

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_verify_link_disabled(self, mock_conf):
        """Test verifying link when feature is disabled."""
        mock_conf.getboolean.return_value = False
        with pytest.raises(ValueError, match="HITL shared links are not enabled"):
            self.manager.verify_link("payload", "signature")

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    def test_verify_link_invalid_signature(self, mock_conf):
        """Test verifying link with invalid signature."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.return_value = "test-secret-key"

        payload_data = HITLSharedLinkPayload(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            map_index=None,
            link_type="action",
            action="approve",
            expires_at=datetime(2023, 1, 2, 12, 0, 0).isoformat(),
        )
        payload_str = payload_data.model_dump_json()
        encoded_payload = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")

        with pytest.raises(ValueError, match="Invalid signature"):
            self.manager.verify_link(encoded_payload, "invalid-signature")

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    @patch("airflow.providers.standard.utils.hitl_shared_links.timezone")
    def test_verify_link_expired(self, mock_timezone, mock_conf):
        """Test verifying expired link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.return_value = "test-secret-key"
        mock_timezone.utcnow.return_value = datetime(2023, 1, 3, 12, 0, 0)

        expires_at = datetime(2023, 1, 2, 12, 0, 0)
        payload_data = HITLSharedLinkPayload(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            map_index=None,
            link_type="action",
            action="approve",
            expires_at=expires_at.isoformat(),
        )

        payload_str = payload_data.model_dump_json()
        signature = self.manager._generate_signature(payload_str)
        encoded_payload = base64.urlsafe_b64encode(payload_str.encode("utf-8")).decode("utf-8")

        with pytest.raises(ValueError, match="Link has expired"):
            self.manager.verify_link(encoded_payload, signature)

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    @patch("airflow.providers.standard.utils.hitl_shared_links.timezone")
    def test_generate_redirect_link(self, mock_timezone, mock_conf):
        """Test generating redirect link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key: {
            ("api", "secret_key"): "test-secret-key",
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key))
        mock_conf.getint.return_value = 24
        mock_timezone.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = self.manager.generate_redirect_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
        )

        assert result["link_type"] == "redirect"
        assert result["action"] is None

    @patch("airflow.providers.standard.utils.hitl_shared_links.conf")
    @patch("airflow.providers.standard.utils.hitl_shared_links.timezone")
    def test_generate_action_link(self, mock_timezone, mock_conf):
        """Test generating action link."""
        mock_conf.getboolean.return_value = True
        mock_conf.get.side_effect = lambda section, key: {
            ("api", "secret_key"): "test-secret-key",
            ("api", "base_url"): "http://localhost:8080",
        }.get((section, key))
        mock_conf.getint.return_value = 24
        mock_timezone.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)

        result = self.manager.generate_action_link(
            dag_id="test_dag",
            dag_run_id="test_run",
            task_id="test_task",
            try_number=1,
            action="approve",
        )

        assert result["link_type"] == "action"
        assert result["action"] == "approve"
