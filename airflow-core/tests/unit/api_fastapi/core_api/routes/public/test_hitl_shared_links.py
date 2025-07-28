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

import datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import ANY

import pytest
from fastapi import status

from airflow.api_fastapi.core_api.datamodels.hitl import HITLDetailResponse
from airflow.utils import timezone

from tests.test_utils.config import conf_vars


class TestHITLSharedLinksAPI:
    """Test HITL shared links API endpoints."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        with conf_vars({("api", "hitl_enable_shared_links"): "True"}):
            yield

    def test_generate_shared_link_disabled(self, client):
        """Test that shared link generation is disabled when not configured."""
        with conf_vars({("api", "hitl_enable_shared_links"): "False"}):
            response = client.post(
                "/api/v2/hitl-shared-links/generate/test_dag/test_run/test_task?try_number=1",
                json={"link_type": "direct_action", "action": "approve"},
            )
            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "not enabled" in response.json()["detail"]

    def test_generate_direct_action_link(self, client):
        """Test generating a direct action shared link."""
        with patch(
            "airflow.api_fastapi.core_api.routes.public.hitl_shared_links.service_generate_shared_link"
        ) as mock_generate:
            mock_generate.return_value = {
                "url": "http://localhost:8080/api/v2/hitl-shared-links/execute?token=test_token",
                "expires_at": "2025-01-01T12:00:00Z",
                "link_type": "direct_action",
                "action": "approve",
                "dag_id": "test_dag",
                "dag_run_id": "test_run",
                "task_id": "test_task",
                "try_number": 1,
                "map_index": None,
                "task_instance_uuid": "test-uuid-123",
            }

            response = client.post(
                "/api/v2/hitl-shared-links/generate/test_dag/test_run/test_task?try_number=1",
                json={
                    "link_type": "direct_action",
                    "action": "approve",
                    "chosen_options": ["Approve"],
                    "params_input": {"comment": "Approved"},
                },
            )

            assert response.status_code == status.HTTP_201_CREATED
            data = response.json()
            assert data["link_type"] == "direct_action"
            assert data["action"] == "approve"
            assert "execute?token=" in data["url"]
            assert data["task_instance_uuid"] == "test-uuid-123"

    def test_generate_ui_redirect_link(self, client):
        """Test generating a UI redirect shared link."""
        with patch(
            "airflow.api_fastapi.core_api.routes.public.hitl_shared_links.service_generate_shared_link"
        ) as mock_generate:
            mock_generate.return_value = {
                "url": "http://localhost:8080/api/v2/hitl-shared-links/redirect?token=test_token",
                "expires_at": "2025-01-01T12:00:00Z",
                "link_type": "ui_redirect",
                "action": None,
                "dag_id": "test_dag",
                "dag_run_id": "test_run",
                "task_id": "test_task",
                "try_number": 1,
                "map_index": None,
                "task_instance_uuid": "test-uuid-456",
            }

            response = client.post(
                "/api/v2/hitl-shared-links/generate/test_dag/test_run/test_task?try_number=1",
                json={"link_type": "ui_redirect"},
            )

            assert response.status_code == status.HTTP_201_CREATED
            data = response.json()
            assert data["link_type"] == "ui_redirect"
            assert data["action"] is None
            assert "redirect?token=" in data["url"]
            assert data["task_instance_uuid"] == "test-uuid-456"

    def test_generate_mapped_ti_shared_link(self, client):
        """Test generating a shared link for mapped task instances."""
        with patch(
            "airflow.api_fastapi.core_api.routes.public.hitl_shared_links.service_generate_shared_link"
        ) as mock_generate:
            mock_generate.return_value = {
                "url": "http://localhost:8080/api/v2/hitl-shared-links/execute?token=test_token",
                "expires_at": "2025-01-01T12:00:00Z",
                "link_type": "direct_action",
                "action": "approve",
                "dag_id": "test_dag",
                "dag_run_id": "test_run",
                "task_id": "test_task",
                "try_number": 1,
                "map_index": 0,
                "task_instance_uuid": "test-uuid-789",
            }

            response = client.post(
                "/api/v2/hitl-shared-links/generate/test_dag/test_run/test_task/0?try_number=1",
                json={
                    "link_type": "direct_action",
                    "action": "approve",
                    "chosen_options": ["Approve"],
                },
            )

            assert response.status_code == status.HTTP_201_CREATED
            data = response.json()
            assert data["map_index"] == 0
            assert data["task_instance_uuid"] == "test-uuid-789"

    def test_execute_shared_link_action(self, client):
        """Test executing an action via shared link."""
        # Create a valid token
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
            "params_input": {"comment": "Approved"},
            "expires_at": (timezone.utcnow() + timedelta(hours=1)).isoformat(),
        }
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()

        with patch(
            "airflow.api_fastapi.core_api.routes.public.hitl_shared_links.service_execute_shared_link_action"
        ) as mock_execute:
            mock_execute.return_value = HITLDetailResponse(
                user_id="shared_link_user",
                response_at=timezone.utcnow(),
                chosen_options=["Approve"],
                params_input={"comment": "Approved"},
            )

            response = client.get(f"/api/v2/hitl-shared-links/execute?token={token}")

            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["chosen_options"] == ["Approve"]

    def test_execute_shared_link_action_invalid_token(self, client):
        """Test executing an action with invalid token."""
        response = client.get("/api/v2/hitl-shared-links/execute?token=invalid_token")
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_execute_shared_link_action_expired_token(self, client):
        """Test executing an action with expired token."""
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

        response = client.get(f"/api/v2/hitl-shared-links/execute?token={token}")
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "expired" in response.json()["detail"]

    def test_execute_shared_link_action_wrong_type(self, client):
        """Test executing an action with wrong link type."""
        # Create a token with wrong type
        token_data = {
            "task_instance_uuid": "test-uuid-123",
            "type": "ui_redirect",
            "dag_id": "test_dag",
            "dag_run_id": "test_run",
            "task_id": "test_task",
            "try_number": 1,
            "map_index": None,
            "action": "approve",
            "chosen_options": ["Approve"],
            "params_input": {},
            "expires_at": (timezone.utcnow() + timedelta(hours=1)).isoformat(),
        }
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()

        response = client.get(f"/api/v2/hitl-shared-links/execute?token={token}")
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "not a direct_action link" in response.json()["detail"]

    def test_redirect_shared_link(self, client):
        """Test redirecting to Airflow UI via shared link."""
        # Create a valid token
        token_data = {
            "task_instance_uuid": "test-uuid-456",
            "type": "ui_redirect",
            "dag_id": "test_dag",
            "dag_run_id": "test_run",
            "task_id": "test_task",
            "try_number": 1,
            "map_index": None,
            "action": None,
            "chosen_options": None,
            "params_input": None,
            "expires_at": (timezone.utcnow() + timedelta(hours=1)).isoformat(),
        }
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()

        with patch(
            "airflow.api_fastapi.core_api.routes.public.hitl_shared_links.service_redirect_shared_link"
        ) as mock_redirect:
            mock_redirect.return_value = (
                "http://localhost:8080/dags/test_dag/grid?task_id=test_task&dag_run_id=test_run"
            )

            response = client.get(f"/api/v2/hitl-shared-links/redirect?token={token}")

            assert response.status_code == status.HTTP_307_TEMPORARY_REDIRECT
            assert (
                response.headers["location"]
                == "http://localhost:8080/dags/test_dag/grid?task_id=test_task&dag_run_id=test_run"
            )

    def test_redirect_shared_link_with_map_index(self, client):
        """Test redirecting to Airflow UI for mapped task instances."""
        # Create a valid token with map_index
        token_data = {
            "task_instance_uuid": "test-uuid-789",
            "type": "ui_redirect",
            "dag_id": "test_dag",
            "dag_run_id": "test_run",
            "task_id": "test_task",
            "try_number": 1,
            "map_index": 0,
            "action": None,
            "chosen_options": None,
            "params_input": None,
            "expires_at": (timezone.utcnow() + timedelta(hours=1)).isoformat(),
        }
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()

        with patch(
            "airflow.api_fastapi.core_api.routes.public.hitl_shared_links.service_redirect_shared_link"
        ) as mock_redirect:
            mock_redirect.return_value = (
                "http://localhost:8080/dags/test_dag/grid?task_id=test_task&dag_run_id=test_run&map_index=0"
            )

            response = client.get(f"/api/v2/hitl-shared-links/redirect?token={token}")

            assert response.status_code == status.HTTP_307_TEMPORARY_REDIRECT
            assert "map_index=0" in response.headers["location"]

    def test_redirect_shared_link_invalid_token(self, client):
        """Test redirecting with invalid token."""
        response = client.get("/api/v2/hitl-shared-links/redirect?token=invalid_token")
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_redirect_shared_link_wrong_type(self, client):
        """Test redirecting with wrong link type."""
        # Create a token with wrong type
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
            "expires_at": ANY,
        }
        token = base64.urlsafe_b64encode(json.dumps(token_data).encode()).decode()

        response = client.get(f"/api/v2/hitl-shared-links/redirect?token={token}")
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "not a ui_redirect link" in response.json()["detail"]

    def test_redirect_shared_link_disabled(self, client):
        """Test that redirect is disabled when feature is not enabled."""
        with conf_vars({("api", "hitl_enable_shared_links"): "False"}):
            response = client.get("/api/v2/hitl-shared-links/redirect?token=test_token")
            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "not enabled" in response.json()["detail"]

    def test_execute_shared_link_action_disabled(self, client):
        """Test that execute is disabled when feature is not enabled."""
        with conf_vars({("api", "hitl_enable_shared_links"): "False"}):
            response = client.get("/api/v2/hitl-shared-links/execute?token=test_token")
            assert response.status_code == status.HTTP_403_FORBIDDEN
            assert "not enabled" in response.json()["detail"]
