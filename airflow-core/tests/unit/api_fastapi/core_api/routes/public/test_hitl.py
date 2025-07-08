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

import pytest

from tests_common.test_utils.db import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop public API compatible with Airflow >= 3.0.1", allow_module_level=True)

from datetime import datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import time_machine
from uuid6 import uuid7

from airflow.models.hitl import HITLResponseModel

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


pytestmark = pytest.mark.db_test
TI_ID = uuid7()


@pytest.fixture
def sample_ti(create_task_instance) -> TaskInstance:
    return create_task_instance()


@pytest.fixture
def sample_hitl_response(session, sample_ti) -> HITLResponseModel:
    hitl_response_model = HITLResponseModel(
        ti_id=sample_ti.id,
        options=["Approve", "Reject"],
        subject="This is subject",
        body="this is body",
        default=["Approve"],
        multiple=False,
        params={"input_1": 1},
    )
    session.add(hitl_response_model)
    session.commit()

    return hitl_response_model


@pytest.fixture
def expected_sample_hitl_response_dict(sample_ti) -> dict[str, Any]:
    return {
        "body": "this is body",
        "default": ["Approve"],
        "multiple": False,
        "options": ["Approve", "Reject"],
        "params": {"input_1": 1},
        "params_input": {},
        "response_at": None,
        "response_content": None,
        "response_received": False,
        "subject": "This is subject",
        "ti_id": sample_ti.id,
        "user_id": None,
    }


class TestUpdateHITLResponseEndpoint:
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_response")
    def test_should_respond_200_with_existing_response(self, test_client, sample_ti):
        response = test_client.patch(
            f"/hitl-responses/{sample_ti.id}",
            json={"response_content": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "response_content": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    def test_should_respond_404(self, test_client, sample_ti):
        response = test_client.get(f"/hitl-responses/{sample_ti.id}")
        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Human-in-the-loop response not found",
                "reason": "not_found",
            },
        }

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_response")
    def test_should_respond_409(self, test_client, sample_ti, expected_sample_hitl_response_dict):
        response = test_client.patch(
            f"/hitl-responses/{sample_ti.id}",
            json={"response_content": ["Approve"], "params_input": {"input_1": 2}},
        )

        expected_response = {
            "params_input": {"input_1": 2},
            "response_content": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }
        assert response.status_code == 200
        assert response.json() == expected_response

        response = test_client.patch(
            f"/hitl-responses/{sample_ti.id}",
            json={"response_content": ["Approve"], "params_input": {"input_1": 2}},
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": (
                "Human-in-the-loop Response has already been updated for Task Instance "
                f"with id {sample_ti.id} "
                "and is not allowed to write again."
            )
        }

    def test_should_respond_401(self, unauthenticated_test_client, sample_ti):
        response = unauthenticated_test_client.get(f"/hitl-responses/{sample_ti.id}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, sample_ti):
        response = unauthorized_test_client.get(f"/hitl-responses/{sample_ti.id}")
        assert response.status_code == 403


class TestGetHITLResponseEndpoint:
    @pytest.mark.usefixtures("sample_hitl_response")
    def test_should_respond_200_with_existing_response(
        self, test_client, sample_ti, expected_sample_hitl_response_dict
    ):
        response = test_client.get(f"/hitl-responses/{sample_ti.id}")
        assert response.status_code == 200
        assert response.json() == expected_sample_hitl_response_dict

    def test_should_respond_404(self, test_client, sample_ti):
        response = test_client.get(f"/hitl-responses/{sample_ti.id}")
        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Human-in-the-loop response not found",
                "reason": "not_found",
            },
        }

    def test_should_respond_401(self, unauthenticated_test_client, sample_ti):
        response = unauthenticated_test_client.get(f"/hitl-responses/{sample_ti.id}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, sample_ti):
        response = unauthorized_test_client.get(f"/hitl-responses/{sample_ti.id}")
        assert response.status_code == 403


class TestGetHITLResponsesEndpoint:
    @pytest.mark.usefixtures("sample_hitl_response")
    def test_should_respond_200_with_existing_response(
        self, test_client, sample_ti, expected_sample_hitl_response_dict
    ):
        response = test_client.get("/hitl-responses/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_responses": [expected_sample_hitl_response_dict],
            "total_entries": 1,
        }

    def test_should_respond_200_without_response(self, test_client):
        response = test_client.get("/hitl-responses/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_responses": [],
            "total_entries": 0,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/hitl-responses/")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/hitl-responses/")
        assert response.status_code == 403


class TestHITLSharedLinksAPI:
    """Test HITL shared links API endpoints."""

    @pytest.mark.usefixtures("sample_hitl_response")
    @patch("airflow.configuration.conf.getboolean")
    def test_create_shared_link_redirect(self, mock_getboolean, test_client, sample_ti):
        """Test creating a redirect shared link."""
        mock_getboolean.return_value = True

        with patch(
            "airflow.utils.hitl_shared_links.hitl_shared_link_manager.generate_redirect_link"
        ) as mock_generate:
            mock_generate.return_value = ("/test/redirect/link", "2024-01-01T12:00:00Z")

            response = test_client.post(
                f"/api/v2/hitl-responses/{sample_ti.id}/shared-link",
                json={"link_type": "redirect", "expires_in_hours": 24},
            )

            assert response.status_code == 200
            data = response.json()
            assert data["task_instance_id"] == sample_ti.id
            assert data["link_url"] == "/test/redirect/link"
            assert data["link_type"] == "redirect"

    @pytest.mark.usefixtures("sample_hitl_response")
    @patch("airflow.configuration.conf.getboolean")
    def test_create_shared_link_action(self, mock_getboolean, test_client, sample_ti):
        """Test creating an action shared link."""
        mock_getboolean.return_value = True

        with patch(
            "airflow.utils.hitl_shared_links.hitl_shared_link_manager.generate_action_link"
        ) as mock_generate:
            mock_generate.return_value = ("/test/action/link", "2024-01-01T12:00:00Z")

            response = test_client.post(
                f"/api/v2/hitl-responses/{sample_ti.id}/shared-link",
                json={"link_type": "action", "action": "approve", "expires_in_hours": 24},
            )

            assert response.status_code == 200
            data = response.json()
            assert data["task_instance_id"] == sample_ti.id
            assert data["link_url"] == "/test/action/link"
            assert data["action"] == "approve"
            assert data["link_type"] == "action"

    @patch("airflow.configuration.conf.getboolean")
    def test_create_shared_link_disabled(self, mock_getboolean, test_client, sample_ti):
        """Test creating shared link when feature is disabled."""
        mock_getboolean.return_value = False

        response = test_client.post(
            f"/api/v2/hitl-responses/{sample_ti.id}/shared-link",
            json={"link_type": "action", "action": "approve"},
        )

        assert response.status_code == 403
        assert "HITL shared links are not enabled" in response.json()["detail"]

    def test_create_shared_link_not_found(self, test_client):
        """Test creating shared link for non-existent task instance."""
        response = test_client.post(
            "/api/v2/hitl-responses/non-existent-task/shared-link",
            json={"link_type": "action", "action": "approve"},
        )

        assert response.status_code == 404

    @pytest.mark.usefixtures("sample_hitl_response")
    @patch("airflow.configuration.conf.getboolean")
    def test_get_hitl_shared_response(self, mock_getboolean, test_client, sample_ti):
        """Test getting HITL response via shared link."""
        mock_getboolean.return_value = True

        with patch("airflow.utils.hitl_shared_links.hitl_shared_link_manager.verify_link") as mock_verify:
            mock_verify.return_value = {
                "ti_id": sample_ti.id,
                "action": None,
                "link_type": "redirect",
                "expires_at": "2024-01-01T12:00:00Z",
            }

            response = test_client.get(
                f"/api/v2/hitl/shared/{sample_ti.id}",
                params={"payload": "test-payload", "signature": "test-signature"},
            )

            assert response.status_code == 200
            data = response.json()
            assert data["ti_id"] == sample_ti.id
            assert data["subject"] == "This is subject"

    @patch("airflow.configuration.conf.getboolean")
    def test_get_hitl_shared_response_invalid_link(self, mock_getboolean, test_client, sample_ti):
        """Test getting HITL response with invalid shared link."""
        mock_getboolean.return_value = True

        with patch("airflow.utils.hitl_shared_links.hitl_shared_link_manager.verify_link") as mock_verify:
            mock_verify.side_effect = ValueError("Invalid link")

            response = test_client.get(
                f"/api/v2/hitl/shared/{sample_ti.id}",
                params={"payload": "invalid-payload", "signature": "invalid-signature"},
            )

            assert response.status_code == 400
            assert "Invalid shared link" in response.json()["detail"]

    @pytest.mark.usefixtures("sample_hitl_response")
    @patch("airflow.configuration.conf.getboolean")
    def test_perform_hitl_shared_action(self, mock_getboolean, test_client, sample_ti):
        """Test performing action via shared link."""
        mock_getboolean.return_value = True

        with patch("airflow.utils.hitl_shared_links.hitl_shared_link_manager.verify_link") as mock_verify:
            mock_verify.return_value = {
                "ti_id": sample_ti.id,
                "action": "approve",
                "link_type": "action",
                "expires_at": "2024-01-01T12:00:00Z",
            }

            response = test_client.post(
                f"/api/v2/hitl/shared/{sample_ti.id}/action",
                params={"payload": "test-payload", "signature": "test-signature"},
                json={"response_content": ["Approve"], "params_input": {"reason": "Approved by manager"}},
            )

            assert response.status_code == 200
            data = response.json()
            assert data["response_content"] == ["Approve"]
            assert data["params_input"] == {"reason": "Approved by manager"}

    @pytest.mark.usefixtures("sample_hitl_response")
    @patch("airflow.configuration.conf.getboolean")
    def test_perform_hitl_shared_action_already_responded(self, mock_getboolean, test_client, sample_ti):
        """Test performing action on already responded task."""
        mock_getboolean.return_value = True

        # Mark the response as already received
        sample_ti.response_received = True
        sample_ti.response_content = ["Reject"]
        sample_ti.response_at = "2024-01-01T10:00:00Z"

        with patch("airflow.utils.hitl_shared_links.hitl_shared_link_manager.verify_link") as mock_verify:
            mock_verify.return_value = {
                "ti_id": sample_ti.id,
                "action": "approve",
                "link_type": "action",
                "expires_at": "2024-01-01T12:00:00Z",
            }

            response = test_client.post(
                f"/api/v2/hitl/shared/{sample_ti.id}/action",
                params={"payload": "test-payload", "signature": "test-signature"},
                json={"response_content": ["Approve"], "params_input": {}},
            )

            assert response.status_code == 409
            assert "already been updated" in response.json()["detail"]

    @pytest.mark.usefixtures("sample_hitl_response")
    @patch("airflow.configuration.conf.getboolean")
    def test_redirect_to_hitl_ui(self, mock_getboolean, test_client, sample_ti):
        """Test redirect to HITL UI."""
        mock_getboolean.return_value = True

        with patch("airflow.utils.hitl_shared_links.hitl_shared_link_manager.verify_link") as mock_verify:
            mock_verify.return_value = {
                "ti_id": sample_ti.id,
                "action": None,
                "link_type": "redirect",
                "expires_at": "2024-01-01T12:00:00Z",
            }

            with patch("airflow.configuration.conf.get") as mock_conf_get:
                mock_conf_get.return_value = "http://localhost:8080"

                response = test_client.get(
                    f"/api/v2/hitl/shared/{sample_ti.id}/redirect",
                    params={"payload": "test-payload", "signature": "test-signature"},
                )

                assert response.status_code == 302
                assert "Location" in response.headers
                assert "hitl_mode=true" in response.headers["Location"]
