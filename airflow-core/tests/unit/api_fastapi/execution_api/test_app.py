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

import time
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import cached_app
from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance
from airflow.api_fastapi.execution_api.versions import bundle

pytestmark = pytest.mark.db_test


def test_custom_openapi_includes_extra_schemas(client):
    """Test to ensure that extra schemas are correctly included in the OpenAPI schema."""
    response = client.get("/execution/openapi.json?version=2025-04-28")
    assert response.status_code == 200

    openapi_schema = response.json()

    assert "TaskInstance" in openapi_schema["components"]["schemas"]
    schema = openapi_schema["components"]["schemas"]["TaskInstance"]

    assert schema["properties"].keys() == TaskInstance.model_json_schema()["properties"].keys()


def test_access_api_contract(client):
    response = client.get("/execution/docs")
    assert response.status_code == 200
    assert response.headers["airflow-api-version"] == bundle.versions[0].value


class TestCorrelationIdMiddleware:
    def test_correlation_id_echoed_in_response_headers(self, client):
        """Test that correlation-id from request is echoed back in response headers."""
        correlation_id = "test-correlation-id-12345"
        response = client.get("/execution/health", headers={"correlation-id": correlation_id})

        assert response.status_code == 200
        assert response.headers["correlation-id"] == correlation_id

    def test_correlation_id_in_error_response_content(self, client):
        """Test that correlation-id is included in error response content."""
        correlation_id = "error-test-correlation-id-67890"

        # Force an error by calling a non-existent endpoint
        response = client.get("/execution/non-existent-endpoint", headers={"correlation-id": correlation_id})

        assert response.status_code == 404
        # Correlation-id should still be in response headers from middleware
        assert response.headers.get("correlation-id") == correlation_id

    def test_correlation_id_propagates_through_request_lifecycle(self, client):
        """Test that correlation-id propagates through the entire request lifecycle."""
        correlation_id = "lifecycle-test-correlation-id"

        # Make a successful request
        response = client.get("/execution/health", headers={"correlation-id": correlation_id})
        assert response.status_code == 200
        assert response.headers["correlation-id"] == correlation_id

        # Make an error request (404)
        response = client.get("/execution/nonexistent", headers={"correlation-id": correlation_id})
        assert response.status_code == 404
        assert response.headers["correlation-id"] == correlation_id

    def test_multiple_requests_with_different_correlation_ids(self, client):
        """Test that different requests maintain their own correlation-ids."""
        correlation_id_1 = "request-1-correlation-id"
        correlation_id_2 = "request-2-correlation-id"

        # Make first request
        response1 = client.get("/execution/health", headers={"correlation-id": correlation_id_1})
        assert response1.status_code == 200
        assert response1.headers["correlation-id"] == correlation_id_1

        # Make second request with different correlation-id
        response2 = client.get("/execution/health", headers={"correlation-id": correlation_id_2})
        assert response2.status_code == 200
        assert response2.headers["correlation-id"] == correlation_id_2

        # Verify they didn't interfere with each other
        assert correlation_id_1 != correlation_id_2


class TestExpiredTokenRefresh:
    """Tests for expired JWT token refresh functionality."""

    @pytest.fixture
    def expired_token_client(self):
        import svcs

        app = cached_app(apps="execution")
        original_registry = lifespan.registry
        lifespan.registry = svcs.Registry()

        with TestClient(app) as client:
            yield client

        lifespan.registry = original_registry

    @pytest.fixture
    def setup_variable(self):
        from airflow.models.variable import Variable

        Variable.set(key="test_var", value="test_value")
        yield
        try:
            Variable.delete(key="test_var")
        except Exception:
            pass

    def test_expired_token_refreshed_for_running_task(self, expired_token_client, setup_variable):
        """Expired token is refreshed when task is in RUNNING state."""
        task_id = str(uuid4())

        mock_validator = AsyncMock(spec=JWTValidator)
        mock_validator.avalidated_claims.side_effect = jwt.ExpiredSignatureError("Token has expired")
        mock_validator.audience = "test-audience"
        mock_validator.issuer = None
        mock_validator.algorithm = ["HS256"]
        mock_validator.leeway = 0
        mock_validator.get_validation_key = AsyncMock(return_value="test-secret-key")

        mock_generator = AsyncMock(spec=JWTGenerator)
        mock_generator.generate.return_value = "new-refreshed-token"

        lifespan.registry.register_value(JWTValidator, mock_validator)
        lifespan.registry.register_value(JWTGenerator, mock_generator)

        expired_token = jwt.encode(
            {
                "sub": task_id,
                "exp": int(time.time()) - 3600,
                "iat": int(time.time()) - 7200,
                "nbf": int(time.time()) - 7200,
                "aud": "test-audience",
            },
            "test-secret-key",
            algorithm="HS256",
        )

        with patch(
            "airflow.api_fastapi.execution_api.deps._is_task_in_refreshable_state",
            new_callable=AsyncMock,
            return_value=True,
        ):
            response = expired_token_client.get(
                "/execution/variables/test_var",
                headers={"Authorization": f"Bearer {expired_token}"},
            )

            assert response.status_code == 200
            assert response.headers.get("Refreshed-API-Token") == "new-refreshed-token"
            mock_generator.generate.assert_called_once()

    def test_expired_token_rejected_for_completed_task(self, expired_token_client):
        """Expired token is rejected when task is not in RUNNING/QUEUED state."""
        task_id = str(uuid4())

        mock_validator = AsyncMock(spec=JWTValidator)
        mock_validator.avalidated_claims.side_effect = jwt.ExpiredSignatureError("Token has expired")
        mock_validator.audience = "test-audience"
        mock_validator.issuer = None
        mock_validator.algorithm = ["HS256"]
        mock_validator.leeway = 0
        mock_validator.get_validation_key = AsyncMock(return_value="test-secret-key")

        lifespan.registry.register_value(JWTValidator, mock_validator)

        expired_token = jwt.encode(
            {
                "sub": task_id,
                "exp": int(time.time()) - 3600,
                "iat": int(time.time()) - 7200,
                "nbf": int(time.time()) - 7200,
                "aud": "test-audience",
            },
            "test-secret-key",
            algorithm="HS256",
        )

        with patch(
            "airflow.api_fastapi.execution_api.deps._is_task_in_refreshable_state",
            new_callable=AsyncMock,
            return_value=False,
        ):
            response = expired_token_client.get(
                "/execution/variables/test_var",
                headers={"Authorization": f"Bearer {expired_token}"},
            )

            assert response.status_code == 403
            assert "not in a refreshable state" in response.json()["detail"]
            assert "Refreshed-API-Token" not in response.headers
