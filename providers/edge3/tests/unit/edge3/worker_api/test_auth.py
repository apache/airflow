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

import pytest
from fastapi import HTTPException

from airflow.providers.edge3.worker_api import auth

from tests_common.test_utils.config import conf_vars

OIDC_JWKS_URL = "https://idp.example.com/keys"


@pytest.fixture(autouse=True)
def _clear_validator_cache():
    """Reset the ``jwt_validator`` memoization around every test.

    ``jwt_validator`` is ``@cache``-decorated, so a validator built from one
    test's ``conf_vars`` would otherwise leak into the next test and make
    config-dependent assertions unreliable.
    """
    auth.jwt_validator.cache_clear()

    yield

    auth.jwt_validator.cache_clear()


class TestOidcConfig:
    """Config readers translate raw ``[edge]`` options into validator inputs."""

    @pytest.mark.parametrize(
        ("configured", "expected"),
        [
            pytest.param(None, ["RS256"], id="default-rs256"),
            pytest.param("RS512", ["RS512"], id="single-algorithm"),
            pytest.param("RS256, RS512", ["RS256", "RS512"], id="comma-list-is-split-and-stripped"),
        ],
    )
    def test_oidc_algorithms_parsing(self, configured, expected):
        """Unset falls back to RS256; a configured value is split on commas and stripped."""
        overrides = {} if configured is None else {("edge", "jwt_algorithm"): configured}
        with conf_vars(overrides):
            assert auth._jwt_algorithms() == expected

    @pytest.mark.parametrize(
        ("configured", "expected"),
        [
            pytest.param(None, None, id="empty-is-none-so-audience-check-is-skipped"),
            pytest.param("api", "api", id="configured-value-passed-through"),
        ],
    )
    def test_oidc_audience_parsing(self, configured, expected):
        """An empty audience becomes ``None`` (skip check); a set value is forwarded verbatim."""
        overrides = {} if configured is None else {("edge", "jwt_audience"): configured}
        with conf_vars(overrides):
            assert auth._jwt_audience() == expected


class TestJwtValidatorSelection:
    """``jwt_validator`` picks shared-secret vs OIDC based on ``trusted_jwks_url``."""

    @conf_vars({("api_auth", "jwt_secret"): "secret"})
    def test_uses_shared_secret_validator_when_oidc_jwks_url_unset(self):
        """Default path is unchanged: a shared-secret validator with no JWKS is built."""
        validator = auth.jwt_validator()

        assert validator.secret_key == "secret"

        assert validator.jwks is None

    @conf_vars(
        {
            ("edge", "trusted_jwks_url"): OIDC_JWKS_URL,
            ("edge", "jwt_issuer"): "https://idp.example.com",
            ("edge", "jwt_algorithm"): "RS512",
        }
    )
    def test_uses_oidc_validator_when_jwks_url_set(self):
        """Setting ``trusted_jwks_url`` builds a JWKS-backed validator wired from ``[edge]`` config."""
        validator = auth.jwt_validator()

        assert validator.jwks is not None

        assert validator.algorithm == ["RS512"]

        assert validator.issuer == "https://idp.example.com"

        assert validator.audience is None


class TestMethodClaimCheck:
    """``_check_method_claim`` enforces the signed ``method`` only for shared-secret tokens."""

    @conf_vars({("api_auth", "jwt_secret"): "secret"})
    def test_shared_secret_rejects_mismatched_method(self):
        """A shared-secret token minted for another endpoint is forbidden (403)."""
        with pytest.raises(HTTPException) as exc_info:
            auth._check_method_claim("worker/register", {"method": "worker/other"})

        assert exc_info.value.status_code == 403

    @conf_vars({("api_auth", "jwt_secret"): "secret"})
    def test_shared_secret_accepts_matching_method(self):
        """A shared-secret token whose ``method`` matches the request passes without raising."""
        auth._check_method_claim("worker/register", {"method": "worker/register"})

    @conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL})
    def test_oidc_skips_method_claim(self):
        """OIDC tokens carry no ``method`` claim, so the check is skipped rather than 403."""
        auth._check_method_claim("worker/register", {})


class TestJwtTokenAuthorization:
    """End-to-end entry point accepts a valid token after the refactor."""

    @conf_vars({("api_auth", "jwt_secret"): "secret"})
    @mock.patch.object(auth, "jwt_validate", return_value={"method": "worker/register"})
    def test_accepts_valid_token(self, mock_jwt_validate):
        """A validated token with a matching ``method`` claim authorizes without raising."""
        auth.jwt_token_authorization("worker/register", "token")

        mock_jwt_validate.assert_called_once_with("token")
