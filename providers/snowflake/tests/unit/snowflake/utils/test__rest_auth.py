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

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
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

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    @mock.patch(FETCH_TOKEN.format("gcp"), autospec=True)
    def test_private_key_absence_cached_across_wif_refresh(self, fetch, conn_params, time_machine):
        time_machine.move_to("2024-01-01T00:00:00Z", tick=False)
        conn_params.return_value = CONN_PARAMS_WIF
        fetch.return_value = _make_jwt(exp=int(time.time()) + 3600)
        loader = mock.Mock(spec=SnowflakeHook().get_private_key, return_value=None)
        provider = SnowflakeRestTokenProvider(SnowflakeHook(), private_key_loader=loader)
        provider.get_token()
        provider.get_token()
        time_machine.shift(timedelta(hours=1))
        provider.get_token()
        loader.assert_called_once_with()
        assert fetch.call_count == 2

    @mock.patch.object(SnowflakeHook, "_get_conn_params", autospec=True)
    def test_private_key_loader_retries_after_error(self, conn_params):
        conn_params.return_value = {**CONN_PARAMS_WIF, "workload_identity_provider": "OIDC", "token": "token"}
        loader = mock.Mock(spec=SnowflakeHook().get_private_key, side_effect=[OSError("unavailable"), None])
        provider = SnowflakeRestTokenProvider(SnowflakeHook(), private_key_loader=loader)
        with pytest.raises(OSError, match="unavailable"):
            provider.get_token()
        assert provider.get_token().token == "WIF.OIDC.token"
        assert loader.call_count == 2

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


class TestWifDestinations:
    @pytest.mark.parametrize(
        "host",
        [
            "attacker.example",
            "evil-snowflakecomputing.com",
            "a.snowflakecomputing.com.evil",
            "user@a.snowflakecomputing.com",
            "a.snowflakecomputing.com:443",
            "a.snowflakecomputing.com/path",
            "a.snowflakecomputing.com?query",
            "a.snowflakecomputing.com#fragment",
            "a..snowflakecomputing.com",
            "a.snowflakecomputing.com..",
            "a.snowflakecomputing.com\n",
            "a.snowflakecomputing.com%00",
            "https://a.snowflakecomputing.com",
            "a\\.snowflakecomputing.com",
        ],
    )
    @pytest.mark.parametrize("cloud", ["AWS", "AZURE", "GCP", "OIDC"])
    def test_rejects_before_fetch_or_file_read(self, host, cloud):
        hook = mock.create_autospec(SnowflakeHook, instance=True)
        hook._get_conn_params.return_value = {
            **CONN_PARAMS_WIF,
            "host": host,
            "workload_identity_provider": cloud,
            "token_file_path": "/does/not/exist",
        }
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)
        with mock.patch(
            FETCH_TOKEN.format(cloud.lower()) if cloud != "OIDC" else f"{MODULE_PATH}.Path.read_text",
            autospec=True,
        ) as fetch:
            with pytest.raises(ValueError, match="recognized Snowflake host"):
                provider.get_token()
            fetch.assert_not_called()

    @pytest.mark.parametrize(
        "conn_config",
        [
            {"host": "a.snowflakecomputing.com"},
            {"host": "A.snowflakecomputing.cn."},
            {"host": "a.snowflakecomputing.mil"},
            {"host": "private.example"},
            {"host": "a.private.example"},
            {"host": "myorg-my_account.snowflakecomputing.com"},
            {"account": "myorg-my_account"},
            {"account": "_my_account_"},
            {"account": "_"},
        ],
    )
    @mock.patch(FETCH_TOKEN.format("gcp"), autospec=True, return_value="id-token")
    def test_allowed_destinations(self, fetch, conn_config, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES", " private.example,other.example ")
        hook = mock.create_autospec(SnowflakeHook, instance=True)
        hook._get_conn_params.return_value = {**CONN_PARAMS_WIF, **conn_config}
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)
        assert provider.get_token().token == "WIF.GCP.id-token"

    @mock.patch(FETCH_TOKEN.format("gcp"), autospec=True)
    def test_validates_cached_token_destination(self, fetch, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES", "private.example")
        fetch.return_value = _make_jwt(exp=int(time.time()) + 3600)
        hook = mock.create_autospec(SnowflakeHook, instance=True)
        hook._get_conn_params.return_value = {**CONN_PARAMS_WIF, "host": "private.example"}
        provider = SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None)
        provider.get_token()
        monkeypatch.delenv("SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES")
        with pytest.raises(ValueError, match="recognized Snowflake host"):
            provider.get_token()
        fetch.assert_called_once()

    def test_connection_cannot_extend_allowlist(self, monkeypatch):
        monkeypatch.delenv("SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES", raising=False)
        hook = mock.create_autospec(SnowflakeHook, instance=True)
        hook._get_conn_params.return_value = {
            **CONN_PARAMS_WIF,
            "host": "evil.example",
            "SNOWFLAKE_WIF_ALLOWED_HOST_SUFFIXES": "evil.example",
        }
        with pytest.raises(ValueError, match="recognized Snowflake host"):
            SnowflakeRestTokenProvider(hook, private_key_loader=lambda: None).get_token()


class TestFetchIdentityTokens:
    @pytest.mark.parametrize(
        ("aws_region", "default_region", "metadata_region", "expected"),
        [
            ("eu-west-1", "eu-west-2", "eu-west-3", "eu-west-1"),
            (None, "eu-west-2", "eu-west-3", "eu-west-2"),
            (None, None, "eu-west-3", "eu-west-3"),
            ("us-east-1", None, None, "us-east-1"),
        ],
    )
    @mock.patch("botocore.utils.InstanceMetadataRegionFetcher", autospec=True)
    def test_aws_requests_regional_web_identity_token(
        self, metadata, aws_region, default_region, metadata_region, expected, monkeypatch
    ):
        import boto3
        from botocore.stub import Stubber

        for name, value in (("AWS_REGION", aws_region), ("AWS_DEFAULT_REGION", default_region)):
            if value:
                monkeypatch.setenv(name, value)
            else:
                monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("AWS_STS_REGIONAL_ENDPOINTS", "legacy")
        metadata.return_value.retrieve_region.return_value = metadata_region
        real_session = boto3.Session
        clients = []

        def create_session(*args, **kwargs):
            session = real_session(
                *args, aws_access_key_id="testing", aws_secret_access_key="testing", **kwargs
            )
            real_client = session.client

            def create_client(*args, **kwargs):
                client = real_client(*args, **kwargs)
                clients.append(client)
                stubber = Stubber(client)
                stubber.add_response(
                    "get_web_identity_token",
                    {
                        "WebIdentityToken": "aws-jwt",
                        "Expiration": datetime(2024, 1, 1, tzinfo=UTC),
                    },
                    {"Audience": ["snowflakecomputing.com"], "SigningAlgorithm": "ES384"},
                )
                stubber.activate()
                return client

            session.client = create_client
            return session

        with mock.patch("boto3.Session", autospec=True, side_effect=create_session):
            assert _fetch_aws_identity_token() == "aws-jwt"
        assert clients[0].meta.region_name == expected
        assert clients[0].meta.endpoint_url == f"https://sts.{expected}.amazonaws.com"
        if aws_region or default_region:
            metadata.assert_not_called()
        else:
            metadata.return_value.retrieve_region.assert_called_once_with()

    @mock.patch("botocore.utils.InstanceMetadataRegionFetcher", autospec=True)
    @mock.patch("boto3.Session", autospec=True)
    def test_aws_missing_region_raises(self, session, metadata, monkeypatch):
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        metadata.return_value.retrieve_region.return_value = None
        with pytest.raises(ValueError, match="AWS_REGION or AWS_DEFAULT_REGION"):
            _fetch_aws_identity_token()
        session.assert_not_called()

    @mock.patch("boto3.Session", autospec=True)
    def test_aws_old_boto3_raises(self, session, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        session.return_value.client.return_value = mock.Mock(spec=[])
        with pytest.raises(AirflowOptionalProviderFeatureException, match="sts:GetWebIdentityToken"):
            _fetch_aws_identity_token()

    @pytest.mark.parametrize(
        ("fetch", "module", "installation"),
        [
            pytest.param(_fetch_aws_identity_token, "boto3", "Install boto3.", id="aws"),
            pytest.param(
                _fetch_azure_identity_token,
                "azure.identity",
                "Install apache-airflow-providers-snowflake[microsoft.azure].",
                id="azure",
            ),
            pytest.param(
                _fetch_gcp_identity_token, "google.auth.compute_engine", "Install google-auth.", id="gcp"
            ),
        ],
    )
    def test_missing_cloud_library_raises(self, fetch, module, installation):
        with mock.patch.dict(sys.modules, {module: None}):
            with pytest.raises(AirflowOptionalProviderFeatureException) as exc:
                fetch()
        assert installation in str(exc.value)

    @pytest.mark.parametrize("workload", [False, True])
    @pytest.mark.parametrize("fails", [False, True])
    @mock.patch("azure.identity.ManagedIdentityCredential", autospec=True)
    @mock.patch("azure.identity.WorkloadIdentityCredential", autospec=True)
    def test_azure_credential_selection_and_cleanup(self, aks, managed, workload, fails, monkeypatch):
        from azure.core.credentials import AccessToken

        for name in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"):
            if workload:
                monkeypatch.setenv(name, "configured")
            else:
                monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("MANAGED_IDENTITY_CLIENT_ID", "mi-client-id")
        selected, unused = (aks, managed) if workload else (managed, aks)
        selected.return_value.get_token.return_value = AccessToken("azure-token", 3600)
        if fails:
            selected.return_value.get_token.side_effect = RuntimeError("unavailable")
            with pytest.raises(RuntimeError, match="unavailable"):
                _fetch_azure_identity_token()
        else:
            assert _fetch_azure_identity_token() == "azure-token"
        if workload:
            selected.assert_called_once_with()
        else:
            selected.assert_called_once_with(client_id="mi-client-id")
        unused.assert_not_called()
        selected.return_value.get_token.assert_called_once_with(
            "api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad/.default"
        )
        selected.return_value.close.assert_called_once_with()

    @mock.patch("google.auth.compute_engine.IDTokenCredentials", autospec=True)
    @mock.patch("google.auth.transport.requests.Request", autospec=True)
    def test_gcp_uses_metadata_identity_even_with_application_credentials(
        self, request, credential, monkeypatch
    ):
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/does/not/exist.json")
        credential.return_value.token = "gcp-id-token"
        assert _fetch_gcp_identity_token() == "gcp-id-token"
        credential.assert_called_once_with(
            request.return_value, "snowflakecomputing.com", use_metadata_identity_endpoint=True
        )
        credential.return_value.refresh.assert_called_once_with(request.return_value)


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
