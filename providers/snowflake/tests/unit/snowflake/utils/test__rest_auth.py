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

import sys
import time
from datetime import UTC, datetime, timedelta
from unittest import mock

import jwt
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils._rest_auth import (
    SnowflakeRestToken,
    SnowflakeRestTokenProvider,
    _fetch_aws_identity_token,
    _fetch_azure_identity_token,
    _fetch_gcp_identity_token,
    get_cortex_base_url,
)

MODULE_PATH = "airflow.providers.snowflake.utils._rest_auth"

CONN_PARAMS_OAUTH = {"account": "airflow", "authenticator": "oauth", "token": "token-1"}
CONN_PARAMS_PAT = {
    "account": "airflow",
    "authenticator": "programmatic_access_token",
    "password": "my_pat_token_value",
}
CONN_PARAMS_KEYPAIR = {"account": "airflow", "authenticator": "snowflake", "user": "user"}
CONN_PARAMS_WIF = {
    "account": "airflow",
    "authenticator": "WORKLOAD_IDENTITY",
    "workload_identity_provider": "GCP",
}
FETCH_TOKEN = MODULE_PATH + "._fetch_{}_identity_token"


def _make_jwt(**claims) -> str:
    return jwt.encode(claims, "secret", algorithm="HS256")


class TestSnowflakeRestTokenProvider:
    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_oauth_reflects_latest_conn_params(self, mock_conn_params):
        """Every call re-reads conn params -- the provider must not cache the OAuth branch itself."""
        mock_conn_params.side_effect = [
            {**CONN_PARAMS_OAUTH, "token": "token-1"},
            {**CONN_PARAMS_OAUTH, "token": "token-2"},
        ]
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook)

        first = provider.get_token()
        second = provider.get_token()

        assert (first.token, first.token_type) == ("token-1", "OAUTH")
        assert (second.token, second.token_type) == ("token-2", "OAUTH")

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_pat(self, mock_conn_params):
        mock_conn_params.return_value = CONN_PARAMS_PAT
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook)

        token = provider.get_token()

        assert token.token == "my_pat_token_value"
        assert token.token_type == "PROGRAMMATIC_ACCESS_TOKEN"

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_pat_raises_when_password_missing(self, mock_conn_params):
        mock_conn_params.return_value = {**CONN_PARAMS_PAT, "password": ""}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook)

        with pytest.raises(ValueError, match="Programmatic Access Token"):
            provider.get_token()

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_key_pair_reuses_jwt_within_renewal_window(self, mock_conn_params, time_machine):
        mock_conn_params.return_value = CONN_PARAMS_KEYPAIR
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(
            hook,
            token_life_time=timedelta(minutes=59),
            token_renewal_delta=timedelta(minutes=54),
            private_key_loader=lambda: key,
        )

        time_machine.move_to("2024-01-01T00:00:00+00:00", tick=False)
        first = provider.get_token()

        time_machine.move_to("2024-01-01T00:10:00+00:00", tick=False)
        second = provider.get_token()

        assert first.token == second.token
        assert first.token_type == "KEYPAIR_JWT"
        decoded = jwt.decode(first.token, options={"verify_signature": False})
        assert decoded["sub"] == "AIRFLOW.USER"
        assert decoded["iss"].startswith("AIRFLOW.USER.")

        time_machine.move_to("2024-01-01T01:00:00+00:00", tick=False)
        third = provider.get_token()

        assert third.token != first.token

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_key_pair_renews_every_call_when_renewal_delta_exceeds_life_time(
        self, mock_conn_params, time_machine
    ):
        """When `token_renewal_delta >= token_life_time`, the *next* scheduled renewal would land
        after the token has already expired: `renew_time = now + renewal_delta` outlives a token
        whose `exp = now + life_time`. Renewing on every call (the fix) must never serve a token
        whose `exp` has already passed."""
        mock_conn_params.return_value = CONN_PARAMS_KEYPAIR
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(
            hook,
            token_life_time=timedelta(minutes=10),
            token_renewal_delta=timedelta(minutes=54),  # default; >= life_time triggers the fix
            private_key_loader=lambda: key,
        )

        time_machine.move_to("2024-01-01T00:00:00+00:00", tick=False)
        first = provider.get_token()

        time_machine.move_to("2024-01-01T00:11:00+00:00", tick=False)
        second = provider.get_token()

        assert second.token != first.token
        decoded = jwt.decode(second.token, options={"verify_signature": False})
        now = datetime.now(UTC)
        assert decoded["exp"] > now.timestamp()

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_key_pair_raises_without_private_key(self, mock_conn_params):
        mock_conn_params.return_value = CONN_PARAMS_KEYPAIR
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with pytest.raises(ValueError, match="key-pair JWT"):
            provider.get_token()

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_with_private_key_uses_key_pair(self, mock_conn_params):
        mock_conn_params.return_value = {**CONN_PARAMS_KEYPAIR, "workload_identity_provider": "AWS"}
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: key)

        assert provider.get_token().token_type == "KEYPAIR_JWT"

    @pytest.mark.parametrize("provider_name", ["GCP", "AZURE", "AWS", "gcp"])
    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity(self, mock_conn_params, provider_name):
        credential = _make_jwt(exp=int(time.time()) + 3600)
        mock_conn_params.return_value = {**CONN_PARAMS_WIF, "workload_identity_provider": provider_name}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with mock.patch(FETCH_TOKEN.format(provider_name.lower()), return_value=credential) as fetch:
            token = provider.get_token()

        assert token.token == f"WIF.{provider_name.upper()}.{credential}"
        assert token.token_type == "WORKLOAD_IDENTITY_FEDERATION"
        fetch.assert_called_once_with()

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_oidc_token(self, mock_conn_params):
        mock_conn_params.return_value = {
            **CONN_PARAMS_WIF,
            "workload_identity_provider": "OIDC",
            "token": "oidc-id-token",
        }
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        assert provider.get_token().token == "WIF.OIDC.oidc-id-token"

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_oidc_token_file(self, mock_conn_params, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("file-id-token\n")
        mock_conn_params.return_value = {
            **CONN_PARAMS_WIF,
            "workload_identity_provider": "OIDC",
            "token_file_path": str(token_file),
        }
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        assert provider.get_token().token == "WIF.OIDC.file-id-token"

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_oidc_without_token_raises(self, mock_conn_params):
        mock_conn_params.return_value = {**CONN_PARAMS_WIF, "workload_identity_provider": "OIDC"}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with pytest.raises(ValueError, match="'token' or 'token_file_path'"):
            provider.get_token()

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_unknown_provider_raises(self, mock_conn_params):
        mock_conn_params.return_value = {**CONN_PARAMS_WIF, "workload_identity_provider": "nope"}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with pytest.raises(ValueError, match="AWS, AZURE, GCP or OIDC"):
            provider.get_token()

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_is_cached_until_expiry(self, mock_conn_params):
        mock_conn_params.return_value = CONN_PARAMS_WIF
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with mock.patch(
            FETCH_TOKEN.format("gcp"), return_value=_make_jwt(exp=int(time.time()) + 3600)
        ) as fetch:
            first = provider.get_token()
            second = provider.get_token()

        assert first == second
        fetch.assert_called_once()

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param({"exp": 10}, id="expires-within-buffer"),
            pytest.param({}, id="no-exp-claim"),
        ],
    )
    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_is_refetched_when_not_reusable(self, mock_conn_params, claims):
        if "exp" in claims:
            claims = {"exp": int(time.time()) + claims["exp"]}
        mock_conn_params.return_value = CONN_PARAMS_WIF
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with mock.patch(FETCH_TOKEN.format("gcp"), return_value=_make_jwt(**claims)) as fetch:
            provider.get_token()
            provider.get_token()

        assert fetch.call_count == 2

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_workload_identity_credential_is_not_a_jwt(self, mock_conn_params):
        mock_conn_params.return_value = CONN_PARAMS_WIF
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with mock.patch(FETCH_TOKEN.format("gcp"), return_value="opaque-access-token"):
            assert provider.get_token().token == "WIF.GCP.opaque-access-token"

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_without_any_credential_raises(self, mock_conn_params):
        mock_conn_params.return_value = CONN_PARAMS_KEYPAIR
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)

        with pytest.raises(ValueError, match="workload_identity_provider"):
            provider.get_token()

    @mock.patch(f"{MODULE_PATH}.JWTGenerator", autospec=True)
    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_key_pair_raises_without_user(self, mock_conn_params, mock_jwt_generator):
        mock_conn_params.return_value = {"account": "airflow", "authenticator": "snowflake"}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: mock.sentinel.key)

        with pytest.raises(ValueError, match=r"missing: login \(user\)"):
            provider.get_token()

        mock_jwt_generator.assert_not_called()

    @mock.patch(f"{MODULE_PATH}.JWTGenerator", autospec=True)
    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_key_pair_raises_without_account_and_user(self, mock_conn_params, mock_jwt_generator):
        mock_conn_params.return_value = {"authenticator": "snowflake"}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: mock.sentinel.key)

        with pytest.raises(ValueError, match=r"missing: account and login \(user\)"):
            provider.get_token()

        mock_jwt_generator.assert_not_called()

    @mock.patch(f"{MODULE_PATH}.JWTGenerator", autospec=True)
    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_key_pair_raises_without_account(self, mock_conn_params, mock_jwt_generator):
        mock_conn_params.return_value = {"authenticator": "snowflake", "user": "user"}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: mock.sentinel.key)

        with pytest.raises(ValueError, match="missing: account"):
            provider.get_token()

        mock_jwt_generator.assert_not_called()

    @mock.patch(f"{MODULE_PATH}.JWTGenerator", autospec=True)
    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_get_token_key_pair_raises_with_empty_account_and_host(
        self, mock_conn_params, mock_jwt_generator
    ):
        mock_conn_params.return_value = {**CONN_PARAMS_KEYPAIR, "account": "", "host": "custom.example.com"}
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: mock.sentinel.key)

        with pytest.raises(ValueError, match="missing: account"):
            provider.get_token()

        mock_jwt_generator.assert_not_called()

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_build_auth_headers_contains_only_auth_headers(self, mock_conn_params):
        mock_conn_params.return_value = CONN_PARAMS_PAT
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        provider = SnowflakeRestTokenProvider(hook)

        headers = provider.build_auth_headers()

        assert set(headers) == {"Authorization", "X-Snowflake-Authorization-Token-Type"}
        assert headers["Authorization"] == "Bearer my_pat_token_value"
        assert headers["X-Snowflake-Authorization-Token-Type"] == "PROGRAMMATIC_ACCESS_TOKEN"


class TestFetchIdentityTokens:
    @staticmethod
    def _fake_google() -> tuple[mock.MagicMock, dict[str, mock.MagicMock]]:
        google = mock.MagicMock()
        modules = {
            "google": google,
            "google.auth": google.auth,
            "google.auth.transport": google.auth.transport,
            "google.auth.transport.requests": google.auth.transport.requests,
            "google.oauth2": google.oauth2,
            "google.oauth2.id_token": google.oauth2.id_token,
        }
        return google, modules

    def test_aws_requests_web_identity_token_for_snowflake(self):
        boto3 = mock.MagicMock()
        boto3.client.return_value.get_web_identity_token.return_value = {"WebIdentityToken": "aws-jwt"}

        with mock.patch.dict(sys.modules, {"boto3": boto3}):
            assert _fetch_aws_identity_token() == "aws-jwt"

        boto3.client.assert_called_once_with("sts")
        boto3.client.return_value.get_web_identity_token.assert_called_once_with(
            Audience=["snowflakecomputing.com"], SigningAlgorithm="ES384"
        )

    def test_aws_old_boto3_raises(self):
        boto3 = mock.MagicMock()
        boto3.client.return_value = mock.Mock(spec=[])

        with mock.patch.dict(sys.modules, {"boto3": boto3}):
            with pytest.raises(ValueError, match="sts:GetWebIdentityToken"):
                _fetch_aws_identity_token()

    @pytest.mark.parametrize(
        ("fetch", "module", "package"),
        [
            pytest.param(_fetch_aws_identity_token, "boto3", "boto3", id="aws"),
            pytest.param(_fetch_azure_identity_token, "azure.identity", "azure-identity", id="azure"),
            pytest.param(_fetch_gcp_identity_token, "google.oauth2", "google-auth", id="gcp"),
        ],
    )
    def test_missing_cloud_library_raises(self, fetch, module, package):
        with mock.patch.dict(sys.modules, {module: None}):
            with pytest.raises(ValueError, match=package):
                fetch()

    def test_azure_uses_managed_identity_with_client_id(self, monkeypatch):
        for name in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("MANAGED_IDENTITY_CLIENT_ID", "mi-client-id")
        identity = mock.MagicMock()
        identity.ManagedIdentityCredential.return_value.get_token.return_value.token = "azure-token"
        modules = {"azure": identity, "azure.identity": identity}

        with mock.patch.dict(sys.modules, modules):
            assert _fetch_azure_identity_token() == "azure-token"

        identity.ManagedIdentityCredential.assert_called_once_with(client_id="mi-client-id")
        identity.WorkloadIdentityCredential.assert_not_called()
        identity.ManagedIdentityCredential.return_value.get_token.assert_called_once_with(
            "api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad/.default"
        )
        identity.ManagedIdentityCredential.return_value.close.assert_called_once_with()

    def test_azure_uses_workload_identity_on_aks(self, monkeypatch):
        monkeypatch.setenv("AZURE_CLIENT_ID", "client")
        monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
        monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token")
        identity = mock.MagicMock()
        identity.WorkloadIdentityCredential.return_value.get_token.return_value.token = "aks-token"
        modules = {"azure": identity, "azure.identity": identity}

        with mock.patch.dict(sys.modules, modules):
            assert _fetch_azure_identity_token() == "aks-token"

        identity.ManagedIdentityCredential.assert_not_called()

    def test_gcp_requests_id_token_for_snowflake(self):
        google, modules = self._fake_google()
        google.oauth2.id_token.fetch_id_token.return_value = "gcp-id-token"

        with mock.patch.dict(sys.modules, modules):
            assert _fetch_gcp_identity_token() == "gcp-id-token"

        google.oauth2.id_token.fetch_id_token.assert_called_once_with(
            google.auth.transport.requests.Request.return_value, "snowflakecomputing.com"
        )


class TestSnowflakeRestTokenRepr:
    def test_repr_does_not_leak_the_token(self):
        """The default dataclass repr would print the bearer token; that must not happen, since
        this repr can land in a log line, a traceback with locals, or a debugger."""
        assert "secret" not in repr(SnowflakeRestToken(token="secret", token_type="OAUTH"))


class TestGetCortexBaseUrl:
    def test_prefers_host_extra(self):
        assert (
            get_cortex_base_url({"host": "custom.example.com", "account": "airflow"})
            == "https://custom.example.com"
        )

    def test_falls_back_to_account(self):
        assert get_cortex_base_url({"account": "airflow"}) == "https://airflow.snowflakecomputing.com"

    def test_rejects_account_outside_charset(self):
        with pytest.raises(ValueError, match="Invalid Snowflake account"):
            get_cortex_base_url({"account": "acct.example.com/x"})

    def test_appends_region_when_set(self):
        assert (
            get_cortex_base_url({"account": "xy12345", "region": "us-east-2.aws"})
            == "https://xy12345.us-east-2.aws.snowflakecomputing.com"
        )

    def test_ignores_empty_region(self):
        assert (
            get_cortex_base_url({"account": "airflow", "region": ""})
            == "https://airflow.snowflakecomputing.com"
        )

    def test_host_wins_over_region(self):
        assert (
            get_cortex_base_url(
                {"host": "custom.example.com", "account": "xy12345", "region": "us-east-2.aws"}
            )
            == "https://custom.example.com"
        )

    def test_rejects_region_outside_charset(self):
        with pytest.raises(ValueError, match="Invalid Snowflake region"):
            get_cortex_base_url({"account": "xy12345", "region": "us-east-2/x"})
