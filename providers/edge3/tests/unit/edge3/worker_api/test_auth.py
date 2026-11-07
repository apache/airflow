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

from airflow.api_fastapi.auth.tokens import JWKS, JWTGenerator, generate_private_key
from airflow.providers.common.compat.sdk import AirflowConfigException
from airflow.providers.edge3.worker_api import auth
from airflow.providers.edge3.worker_api.auth import (
    jwt_token_authorization,
    jwt_token_authorization_rest,
    jwt_validator,
)

from tests_common.test_utils.config import conf_vars

JWT_SECRET = "test-jwt-secret"
OIDC_JWKS_URL = "https://idp.example.com/keys"
OIDC_ISSUER = "https://idp.example.com"
OIDC_KID = "kid1"

pytestmark = [pytest.mark.asyncio]


def _token(method: str | None = "test.method", secret: str = JWT_SECRET) -> str:
    # Mirrors how providers/edge3/src/airflow/providers/edge3/cli/api_client.py generates
    # tokens for real edge workers, so the token shape (iss/aud/exp/... claims) matches
    # what jwt_token_authorization() actually has to validate in production.
    generator = JWTGenerator(secret_key=secret, valid_for=300, audience="api")
    return generator.generate(extras={"method": method} if method is not None else {})


@pytest.fixture(autouse=True)
def _reset_jwt_validator_cache():
    # jwt_validator(), _oidc_enabled() and _oidc_authenticator() are cached: make sure
    # config overrides in one test can never leak a stale validator or mode into another.
    jwt_validator.cache_clear()
    auth._oidc_enabled.cache_clear()
    auth._oidc_authenticator.cache_clear()
    yield
    jwt_validator.cache_clear()
    auth._oidc_enabled.cache_clear()
    auth._oidc_authenticator.cache_clear()


class TestJwtTokenAuthorization:
    async def test_matching_method_claim_is_authorized(self):
        with conf_vars({("api_auth", "jwt_secret"): JWT_SECRET, ("api_auth", "jwt_leeway"): "5"}):
            await jwt_token_authorization("test.method", _token("test.method"))

    async def test_missing_method_claim_is_forbidden(self):
        with conf_vars({("api_auth", "jwt_secret"): JWT_SECRET, ("api_auth", "jwt_leeway"): "5"}):
            with pytest.raises(HTTPException) as exc_info:
                await jwt_token_authorization("test.method", _token(method=None))
        assert exc_info.value.status_code == 403

    async def test_mismatched_method_claim_is_forbidden(self):
        with conf_vars({("api_auth", "jwt_secret"): JWT_SECRET, ("api_auth", "jwt_leeway"): "5"}):
            with pytest.raises(HTTPException) as exc_info:
                await jwt_token_authorization("test.method", _token("other.method"))
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
    async def test_each_handled_failure_is_forbidden_and_anonymized(self, mock_jwt_validate, error):
        mock_jwt_validate.side_effect = error

        with pytest.raises(HTTPException) as exc_info:
            await jwt_token_authorization("test.method", "some-token")

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
    async def test_strips_edge_worker_v1_prefix_and_falls_back_to_full_path(
        self, mock_jwt_token_authorization, path, expected_method
    ):
        request = mock.MagicMock(spec=Request)
        request.url.path = path

        await jwt_token_authorization_rest(request, authorization="some-token")

        mock_jwt_token_authorization.assert_called_once_with(expected_method, "some-token")


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
            pytest.param(None, None, id="empty-is-none-accepts-only-aud-less-tokens"),
            pytest.param("api", "api", id="configured-value-passed-through"),
        ],
    )
    def test_oidc_audience_parsing(self, configured, expected):
        """An empty audience becomes ``None`` (accept only aud-less tokens); a set value is forwarded."""
        overrides = {} if configured is None else {("edge", "jwt_audience"): configured}
        with conf_vars(overrides):
            assert auth._jwt_audience() == expected

    @pytest.mark.parametrize(
        ("configured", "expected"),
        [
            pytest.param(None, None, id="empty-is-none-skips-issuer-check"),
            pytest.param(
                "https://idp.example.com", "https://idp.example.com", id="configured-passed-through"
            ),
        ],
    )
    def test_oidc_issuer_parsing(self, configured, expected):
        """An empty issuer becomes ``None`` (skip issuer check); a set value is forwarded verbatim."""
        overrides = {} if configured is None else {("edge", "jwt_issuer"): configured}
        with conf_vars(overrides):
            assert auth._jwt_issuer() == expected

    @pytest.mark.parametrize(
        ("configured", "expected"),
        [
            pytest.param(None, 30, id="default-30"),
            pytest.param("90", 90, id="configured-value-parsed-as-int"),
        ],
    )
    def test_oidc_leeway_parsing(self, configured, expected):
        """Unset falls back to 30 seconds; a configured value is read as an integer."""
        overrides = {} if configured is None else {("edge", "jwt_leeway"): configured}
        with conf_vars(overrides):
            assert auth._jwt_leeway() == expected


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
            ("edge", "jwt_leeway"): "90",
        }
    )
    def test_uses_oidc_validator_when_jwks_url_set(self):
        """Setting ``trusted_jwks_url`` builds a JWKS-backed validator wired from ``[edge]`` config."""
        validator = auth.jwt_validator()

        assert validator.jwks is not None

        assert validator.algorithm == ["RS512"]

        assert validator.issuer == "https://idp.example.com"

        assert validator.audience is None

        assert validator.leeway == 90

    @conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL})
    def test_empty_issuer_without_verifier_fails_closed(self):
        """Skipping issuer verification without a ``jwt_verifier`` is a hard config error."""
        with pytest.raises(AirflowConfigException, match="jwt_verifier"):
            auth.jwt_validator()

    @conf_vars(
        {
            ("edge", "trusted_jwks_url"): OIDC_JWKS_URL,
            ("edge", "jwt_verifier"): "airflow.providers.edge3.worker_api.auth._default_jwt_verifier",
        }
    )
    def test_empty_issuer_with_verifier_is_allowed(self):
        """A configured ``jwt_verifier`` permits skipping issuer verification."""
        validator = auth.jwt_validator()

        assert validator.jwks is not None

        assert validator.issuer is None

    @conf_vars(
        {
            ("edge", "trusted_jwks_url"): OIDC_JWKS_URL,
            ("edge", "jwt_issuer"): OIDC_ISSUER,
            ("edge", "jwt_verifier"): "no.such.module.verify",
        }
    )
    def test_unimportable_verifier_fails_closed_at_construction(self):
        """A typo'd ``jwt_verifier`` path fails when the authenticator is built, not per request."""
        with pytest.raises(AirflowConfigException):
            auth.jwt_validator()


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


@pytest.fixture(scope="session")
def oidc_private_key():
    return generate_private_key()


@pytest.fixture
def in_memory_jwks(oidc_private_key):
    # Replace the network-fetching JWKS(url=...) the OIDC validator builds with an
    # in-memory keyset, so signed tokens can be driven through the real validator.
    jwks = JWKS.from_private_key((oidc_private_key, OIDC_KID))
    with mock.patch.object(auth, "JWKS", return_value=jwks):
        yield


def _oidc_token(oidc_private_key, *, audience: str = "", issuer: str = OIDC_ISSUER, sub: str = "") -> str:
    generator = JWTGenerator(
        private_key=oidc_private_key,
        kid=OIDC_KID,
        valid_for=300,
        audience=audience,
        issuer=issuer,
        algorithm="RS256",
    )
    return generator.generate(extras={"sub": sub} if sub else {})


def verify_only_known_sub(claims: dict, context: auth.WorkerTokenContext) -> auth.WorkerTokenAuthorization:
    """Test ``jwt_verifier``: authorize only a known service-account ``sub``."""
    return {"authorized": claims.get("sub") == "dc-service-account"}


VERIFY_ONLY_KNOWN_SUB = f"{__name__}.verify_only_known_sub"


class TestOidcAudienceVerification:
    """
    Pin the real audience semantics by driving signed tokens through the OIDC validator.

    ``jwt_audience`` empty means ``audience=None``, which PyJWT accepts only for
    tokens that carry no ``aud`` claim and rejects for tokens that do. A configured
    value requires a matching ``aud``.
    """

    async def test_empty_audience_accepts_aud_less_token(self, oidc_private_key, in_memory_jwks):
        with conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL, ("edge", "jwt_issuer"): OIDC_ISSUER}):
            await jwt_token_authorization("worker/register", _oidc_token(oidc_private_key, audience=""))

    async def test_empty_audience_rejects_token_with_aud(self, oidc_private_key, in_memory_jwks):
        with conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL, ("edge", "jwt_issuer"): OIDC_ISSUER}):
            with pytest.raises(HTTPException) as exc_info:
                await jwt_token_authorization(
                    "worker/register", _oidc_token(oidc_private_key, audience="api")
                )
        assert exc_info.value.status_code == 403

    async def test_configured_audience_requires_match(self, oidc_private_key, in_memory_jwks):
        with conf_vars(
            {
                ("edge", "trusted_jwks_url"): OIDC_JWKS_URL,
                ("edge", "jwt_issuer"): OIDC_ISSUER,
                ("edge", "jwt_audience"): "api",
            }
        ):
            await jwt_token_authorization("worker/register", _oidc_token(oidc_private_key, audience="api"))
            with pytest.raises(HTTPException) as exc_info:
                await jwt_token_authorization(
                    "worker/register", _oidc_token(oidc_private_key, audience="other")
                )
        assert exc_info.value.status_code == 403


class TestOidcTokenValidation:
    """Drive signed tokens through the real ``_oidc_validator()`` and JWKS path."""

    async def test_trusted_issuer_but_unrelated_identity_is_rejected(self, oidc_private_key, in_memory_jwks):
        with conf_vars(
            {
                ("edge", "trusted_jwks_url"): OIDC_JWKS_URL,
                ("edge", "jwt_issuer"): OIDC_ISSUER,
                ("edge", "jwt_verifier"): VERIFY_ONLY_KNOWN_SUB,
            }
        ):
            token = _oidc_token(oidc_private_key, issuer=OIDC_ISSUER, sub="someone-else")
            with pytest.raises(HTTPException) as exc_info:
                await jwt_token_authorization("worker/register", token)
        assert exc_info.value.status_code == 403

    async def test_authorized_identity_from_trusted_issuer_is_accepted(
        self, oidc_private_key, in_memory_jwks
    ):
        with conf_vars(
            {
                ("edge", "trusted_jwks_url"): OIDC_JWKS_URL,
                ("edge", "jwt_issuer"): OIDC_ISSUER,
                ("edge", "jwt_verifier"): VERIFY_ONLY_KNOWN_SUB,
            }
        ):
            token = _oidc_token(oidc_private_key, issuer=OIDC_ISSUER, sub="dc-service-account")
            await jwt_token_authorization("worker/register", token)

    async def test_token_from_untrusted_issuer_is_rejected(self, oidc_private_key, in_memory_jwks):
        with conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL, ("edge", "jwt_issuer"): OIDC_ISSUER}):
            token = _oidc_token(oidc_private_key, issuer="https://evil.example.com")
            with pytest.raises(HTTPException) as exc_info:
                await jwt_token_authorization("worker/register", token)
        assert exc_info.value.status_code == 403


class TestWorkerAuthorization:
    """
    ``_check_worker_authorization`` gates OIDC tokens on the ``jwt_verifier``.

    A valid signature is not authorization: the verifier answers whether the
    identity may act as an edge worker.
    """

    def test_no_check_in_shared_secret_mode(self):
        """The verifier is not consulted when OIDC is disabled."""
        with mock.patch.object(auth, "_oidc_authenticator") as authenticator:
            auth._check_worker_authorization("worker/register", {"sub": "anyone"})
        authenticator.assert_not_called()

    @conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL, ("edge", "jwt_verifier"): VERIFY_ONLY_KNOWN_SUB})
    def test_configured_verifier_rejects_unauthorized_identity(self):
        """A token from the trusted issuer but an unrelated identity is rejected."""
        with pytest.raises(HTTPException) as exc_info:
            auth._check_worker_authorization("worker/register", {"sub": "someone-else"})
        assert exc_info.value.status_code == 403

    @conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL, ("edge", "jwt_verifier"): VERIFY_ONLY_KNOWN_SUB})
    def test_configured_verifier_authorizes_known_identity(self):
        """A token whose identity the verifier accepts passes without raising."""
        auth._check_worker_authorization("worker/register", {"sub": "dc-service-account"})

    @mock.patch.object(
        auth, "jwt_validate", new_callable=mock.AsyncMock, return_value={"sub": "dc-service-account"}
    )
    @mock.patch.object(auth, "_oidc_authenticator")
    async def test_verifier_raising_is_forbidden(self, mock_authenticator, _mock_validate):
        """A verifier that raises is caught by the entry point and collapsed to a 403."""
        mock_authenticator.return_value.verifier = mock.Mock(side_effect=ValueError("nope"))
        with conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL, ("edge", "jwt_issuer"): OIDC_ISSUER}):
            with pytest.raises(HTTPException) as exc_info:
                await auth.jwt_token_authorization("worker/register", "some-token")
        assert exc_info.value.status_code == 403

    def test_default_verifier_returns_authorized(self):
        assert auth._default_jwt_verifier(
            {"sub": "anyone"}, auth.WorkerTokenContext(method="worker/register")
        ) == {"authorized": True}

    @conf_vars({("edge", "trusted_jwks_url"): OIDC_JWKS_URL, ("edge", "jwt_verifier"): VERIFY_ONLY_KNOWN_SUB})
    @mock.patch.object(auth, "_oidc_authenticator")
    def test_verifier_receives_request_context(self, mock_authenticator):
        """The verifier is handed the request method via ``WorkerTokenContext``."""
        verifier = mock.Mock(return_value={"authorized": True})
        mock_authenticator.return_value.verifier = verifier
        auth._check_worker_authorization("jobs/fetch/worker1", {"sub": "dc-service-account"})
        verifier.assert_called_once_with(
            {"sub": "dc-service-account"}, auth.WorkerTokenContext(method="jobs/fetch/worker1")
        )
