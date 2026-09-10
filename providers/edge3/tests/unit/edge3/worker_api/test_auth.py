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

from unittest import mock

import jwt
import pytest
from fastapi import HTTPException, Request
from itsdangerous import BadSignature

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.providers.edge3.worker_api.auth import (
    jwt_token_authorization,
    jwt_token_authorization_rest,
    jwt_validator,
)

from tests_common.test_utils.config import conf_vars

JWT_SECRET = "test-jwt-secret"


def _token(method: str | None = "test.method", secret: str = JWT_SECRET) -> str:
    # Mirrors how providers/edge3/src/airflow/providers/edge3/cli/api_client.py generates
    # tokens for real edge workers, so the token shape (iss/aud/exp/... claims) matches
    # what jwt_token_authorization() actually has to validate in production.
    generator = JWTGenerator(secret_key=secret, valid_for=300, audience="api")
    return generator.generate(extras={"method": method} if method is not None else {})


@pytest.fixture(autouse=True)
def _reset_jwt_validator_cache():
    # jwt_validator() is cached: make sure config overrides in one test can never leak
    # a stale validator into another.
    jwt_validator.cache_clear()
    yield
    jwt_validator.cache_clear()


class TestJwtTokenAuthorization:
    @conf_vars({("api_auth", "jwt_secret"): JWT_SECRET, ("api_auth", "jwt_leeway"): "5"})
    def test_matching_method_claim_is_authorized(self):
        jwt_token_authorization("test.method", _token("test.method"))

    @conf_vars({("api_auth", "jwt_secret"): JWT_SECRET, ("api_auth", "jwt_leeway"): "5"})
    def test_missing_method_claim_is_forbidden(self):
        with pytest.raises(HTTPException) as exc_info:
            jwt_token_authorization("test.method", _token(method=None))
        assert exc_info.value.status_code == 403

    @conf_vars({("api_auth", "jwt_secret"): JWT_SECRET, ("api_auth", "jwt_leeway"): "5"})
    def test_mismatched_method_claim_is_forbidden(self):
        with pytest.raises(HTTPException) as exc_info:
            jwt_token_authorization("test.method", _token("other.method"))
        assert exc_info.value.status_code == 403


class TestJwtTokenAuthorizationForbiddenResponse:
    """
    Every handled failure is collapsed onto the same anonymised 403 response by
    ``_forbidden_response`` so callers can never distinguish *why* a token was rejected -
    only the server-side log carries the real reason.
    """

    @pytest.mark.parametrize(
        "error",
        [
            BadSignature("Signature does not match"),
            jwt.InvalidAudienceError("Invalid audience"),
            jwt.InvalidSignatureError("Signature verification failed"),
            jwt.ImmatureSignatureError("The token is not yet valid"),
            jwt.ExpiredSignatureError("Signature has expired"),
            jwt.InvalidIssuedAtError("Issued at claim is in the future"),
            ValueError("Some other unexpected failure"),
        ],
    )
    @mock.patch("airflow.providers.edge3.worker_api.auth.jwt_validate", autospec=True)
    def test_each_handled_failure_is_forbidden_and_anonymized(self, mock_jwt_validate, error):
        mock_jwt_validate.side_effect = error

        with pytest.raises(HTTPException) as exc_info:
            jwt_token_authorization("test.method", "some-token")

        assert exc_info.value.status_code == 403
        assert "error_id=" in exc_info.value.detail
        assert str(error) not in exc_info.value.detail


class TestJwtValidatorCaching:
    def test_validator_is_cached_and_reuses_previously_configured_secret(self):
        with conf_vars({("api_auth", "jwt_secret"): "secret-one"}):
            first = jwt_validator()

        with conf_vars({("api_auth", "jwt_secret"): "secret-two"}):
            # No cache_clear() call: the cached validator (built with "secret-one")
            # is silently reused, per the caveat called out for this function.
            second = jwt_validator()

        assert first is second
        assert second.secret_key == "secret-one"

    def test_cache_clear_picks_up_the_new_secret(self):
        with conf_vars({("api_auth", "jwt_secret"): "secret-one"}):
            first = jwt_validator()

        jwt_validator.cache_clear()

        with conf_vars({("api_auth", "jwt_secret"): "secret-two"}):
            second = jwt_validator()

        assert second is not first
        assert second.secret_key == "secret-two"

    @conf_vars({("api_auth", "jwt_secret"): JWT_SECRET, ("api_auth", "jwt_leeway"): "90"})
    def test_leeway_is_read_from_config(self):
        assert jwt_validator().leeway == 90


class TestJwtTokenAuthorizationRest:
    @pytest.mark.parametrize(
        ("path", "expected_method"),
        [
            ("/edge_worker/v1/jobs/fetch/worker1", "jobs/fetch/worker1"),
            ("/edge_worker/v1/health", "health"),
            ("/some/other/path", "/some/other/path"),
        ],
    )
    @mock.patch("airflow.providers.edge3.worker_api.auth.jwt_token_authorization", autospec=True)
    def test_strips_edge_worker_v1_prefix_and_falls_back_to_full_path(
        self, mock_jwt_token_authorization, path, expected_method
    ):
        request = mock.MagicMock(spec=Request)
        request.url.path = path

        jwt_token_authorization_rest(request, authorization="some-token")

        mock_jwt_token_authorization.assert_called_once_with(expected_method, "some-token")
