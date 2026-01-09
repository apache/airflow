#
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
import json
import sys
from copy import deepcopy
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock
from unittest.mock import Mock, PropertyMock

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.models import Connection
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

if TYPE_CHECKING:
    from pathlib import Path

_PASSWORD = "snowflake42"

BASE_CONNECTION_KWARGS: dict = {
    "login": "user",
    "conn_type": "snowflake",
    "password": "pw",
    "schema": "public",
    "extra": {
        "database": "db",
        "account": "airflow",
        "warehouse": "af_wh",
        "region": "af_region",
        "role": "af_role",
    },
}

CONN_PARAMS_OAUTH_BASE = {
    "account": "airflow",
    "application": "AIRFLOW",
    "authenticator": "oauth",
    "database": "db",
    "client_id": "test_client_id",
    "client_secret": "test_client_pw",
    "region": "af_region",
    "role": "af_role",
    "schema": "public",
    "session_parameters": None,
    "warehouse": "af_wh",
}

CONN_PARAMS_OAUTH = CONN_PARAMS_OAUTH_BASE | {"refresh_token": "secrettoken"}


@pytest.fixture
def unencrypted_temporary_private_key(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
    private_key = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    test_key_file = tmp_path / "test_key.pem"
    test_key_file.write_bytes(private_key)
    return test_key_file


@pytest.fixture
def base64_encoded_unencrypted_private_key(self, unencrypted_temporary_private_key: Path) -> str:
    return base64.b64encode(unencrypted_temporary_private_key.read_bytes()).decode("utf-8")


@pytest.fixture
def encrypted_temporary_private_key(tmp_path: Path) -> Path:
    key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
    private_key = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(_PASSWORD.encode()),
    )
    test_key_file: Path = tmp_path / "test_key.p8"
    test_key_file.write_bytes(private_key)
    return test_key_file


@pytest.fixture
def base64_encoded_encrypted_private_key(encrypted_temporary_private_key: Path) -> str:
    return base64.b64encode(encrypted_temporary_private_key.read_bytes()).decode("utf-8")


class TestPytestSnowflakeHook:
    @pytest.mark.parametrize(
        ("connection_kwargs", "expected_uri", "expected_conn_params"),
        [
            (
                BASE_CONNECTION_KWARGS,
                (
                    "snowflake://user:pw@airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "extra": {
                        "extra__snowflake__database": "db",
                        "extra__snowflake__account": "airflow",
                        "extra__snowflake__warehouse": "af_wh",
                        "extra__snowflake__region": "af_region",
                        "extra__snowflake__role": "af_role",
                    },
                },
                (
                    "snowflake://user:pw@airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "extra": {
                        "extra__snowflake__database": "db",
                        "extra__snowflake__account": "airflow",
                        "extra__snowflake__warehouse": "af_wh",
                        "extra__snowflake__region": "af_region",
                        "extra__snowflake__role": "af_role",
                        "extra__snowflake__insecure_mode": "True",
                        "extra__snowflake__json_result_force_utf8_decoding": "True",
                        "extra__snowflake__client_request_mfa_token": "True",
                        "extra__snowflake__client_store_temporary_credential": "True",
                    },
                },
                (
                    "snowflake://user:pw@airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                    "insecure_mode": True,
                    "json_result_force_utf8_decoding": True,
                    "client_request_mfa_token": True,
                    "client_store_temporary_credential": True,
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "extra": {
                        "extra__snowflake__database": "db",
                        "extra__snowflake__account": "airflow",
                        "extra__snowflake__warehouse": "af_wh",
                        "extra__snowflake__region": "af_region",
                        "extra__snowflake__role": "af_role",
                        "extra__snowflake__insecure_mode": "False",
                        "extra__snowflake__json_result_force_utf8_decoding": "False",
                        "extra__snowflake__client_request_mfa_token": "False",
                        "extra__snowflake__client_store_temporary_credential": "False",
                    },
                },
                (
                    "snowflake://user:pw@airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "extra": {
                        **BASE_CONNECTION_KWARGS["extra"],
                        "region": "",
                    },
                },
                (
                    "snowflake://user:pw@airflow/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "password": ";/?:@&=+$, ",
                },
                (
                    "snowflake://user:;%2F?%3A%40&=+$, @airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": ";/?:@&=+$, ",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "extra": {
                        **BASE_CONNECTION_KWARGS["extra"],
                        "extra__snowflake__insecure_mode": False,
                        "extra__snowflake__json_result_force_utf8_decoding": True,
                        "extra__snowflake__client_request_mfa_token": False,
                        "extra__snowflake__client_store_temporary_credential": False,
                    },
                },
                (
                    "snowflake://user:pw@airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                    "json_result_force_utf8_decoding": True,
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "extra": {
                        **BASE_CONNECTION_KWARGS["extra"],
                        "ocsp_fail_open": True,
                    },
                },
                (
                    "snowflake://user:pw@airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                    "ocsp_fail_open": True,
                },
            ),
            (
                {
                    **BASE_CONNECTION_KWARGS,
                    "extra": {
                        **BASE_CONNECTION_KWARGS["extra"],
                        "ocsp_fail_open": False,
                    },
                },
                (
                    "snowflake://user:pw@airflow.af_region/db/public?"
                    "application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
                ),
                {
                    "account": "airflow",
                    "application": "AIRFLOW",
                    "authenticator": "snowflake",
                    "database": "db",
                    "password": "pw",
                    "region": "af_region",
                    "role": "af_role",
                    "schema": "public",
                    "session_parameters": None,
                    "user": "user",
                    "warehouse": "af_wh",
                    "ocsp_fail_open": False,
                },
            ),
        ],
    )
    def test_hook_should_support_prepare_basic_conn_params_and_uri(
        self, connection_kwargs, expected_uri, expected_conn_params
    ):
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            assert SnowflakeHook(snowflake_conn_id="test_conn").get_uri() == expected_uri
            assert SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params() == expected_conn_params

    def test_get_conn_params_should_support_private_auth_in_connection(
        self, base64_encoded_encrypted_private_key: Path
    ):
        connection_kwargs: Any = {
            **BASE_CONNECTION_KWARGS,
            "password": _PASSWORD,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_content": base64_encoded_encrypted_private_key,
            },
        }
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            assert "private_key" in SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params()

    @pytest.mark.parametrize("include_params", [True, False])
    def test_hook_param_beats_extra(self, include_params):
        """When both hook params and extras are supplied, hook params should
        beat extras."""
        hook_params = dict(
            account="account",
            warehouse="warehouse",
            database="database",
            region="region",
            role="role",
            authenticator="authenticator",
            session_parameters="session_parameters",
        )
        extras = {k: f"{v}_extra" for k, v in hook_params.items()}
        with mock.patch.dict(
            "os.environ",
            AIRFLOW_CONN_TEST_CONN=Connection(conn_type="any", extra=json.dumps(extras)).get_uri(),
        ):
            assert hook_params != extras
            assert SnowflakeHook(
                snowflake_conn_id="test_conn", **(hook_params if include_params else {})
            )._get_conn_params() == {
                "user": None,
                "password": "",
                "application": "AIRFLOW",
                "schema": "",
                **(hook_params if include_params else extras),
            }

    @pytest.mark.parametrize("include_unprefixed", [True, False])
    def test_extra_short_beats_long(self, include_unprefixed):
        """When both prefixed and unprefixed values are found in extra (e.g.
        extra__snowflake__account and account), we should prefer the short
        name."""
        extras = dict(
            account="account",
            warehouse="warehouse",
            database="database",
            region="region",
            role="role",
        )
        extras_prefixed = {f"extra__snowflake__{k}": f"{v}_prefixed" for k, v in extras.items()}
        with mock.patch.dict(
            "os.environ",
            AIRFLOW_CONN_TEST_CONN=Connection(
                conn_type="any",
                extra=json.dumps({**(extras if include_unprefixed else {}), **extras_prefixed}),
            ).get_uri(),
        ):
            assert list(extras.values()) != list(extras_prefixed.values())
            assert SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params() == {
                "user": None,
                "password": "",
                "application": "AIRFLOW",
                "schema": "",
                "authenticator": "snowflake",
                "session_parameters": None,
                **(extras if include_unprefixed else dict(zip(extras.keys(), extras_prefixed.values()))),
            }

    def test_get_conn_params_should_support_private_auth_with_encrypted_key(
        self, encrypted_temporary_private_key
    ):
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "password": _PASSWORD,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_file": str(encrypted_temporary_private_key),
            },
        }
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            assert "private_key" in SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params()

    def test_get_conn_params_should_support_private_auth_with_unencrypted_key(
        self, unencrypted_temporary_private_key
    ):
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "password": None,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_file": str(unencrypted_temporary_private_key),
            },
        }
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            assert "private_key" in SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params()
        connection_kwargs["password"] = ""
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            assert "private_key" in SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params()
        connection_kwargs["password"] = _PASSWORD
        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()),
            pytest.raises(TypeError, match="Password was given but private key is not encrypted."),
        ):
            SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params()

    def test_get_conn_params_should_fail_on_invalid_key(self):
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "password": None,
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "private_key_file": "/dev/urandom",
            },
        }
        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()),
            pytest.raises(ValueError, match="The private_key_file path points to an empty or invalid file."),
        ):
            SnowflakeHook(snowflake_conn_id="test_conn").get_conn()

    @mock.patch("requests.post")
    @mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook._get_conn_params")
    def test_get_conn_params_should_support_oauth(self, mock_get_conn_params, requests_post):
        requests_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "access_token": "supersecretaccesstoken",
                "expires_in": 600,
                "refresh_token": "secrettoken",
                "token_type": "Bearer",
                "username": "test_user",
            },
        )
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "login": "test_client_id",
            "password": "test_client_secret",
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "refresh_token": "secrettoken",
                "authenticator": "oauth",
            },
        }
        mock_get_conn_params.return_value = connection_kwargs
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn_params = hook._get_conn_params()

        conn_params_keys = conn_params.keys()
        conn_params_extra = conn_params.get("extra", {})
        conn_params_extra_keys = conn_params_extra.keys()

        assert "authenticator" in conn_params_extra_keys
        assert conn_params_extra["authenticator"] == "oauth"

        assert "user" not in conn_params_keys
        assert "password" in conn_params_keys
        assert "refresh_token" in conn_params_extra_keys
        # Mandatory fields to generate account_identifier `https://<account>.<region>`
        assert "region" in conn_params_extra_keys
        assert "account" in conn_params_extra_keys

    @mock.patch("requests.post")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook._get_conn_params",
    )
    def test_get_conn_params_should_support_oauth_with_token_endpoint(
        self, mock_get_conn_params, requests_post
    ):
        requests_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "access_token": "supersecretaccesstoken",
                "expires_in": 600,
                "refresh_token": "secrettoken",
                "token_type": "Bearer",
                "username": "test_user",
            },
        )
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "login": "test_client_id",
            "password": "test_client_secret",
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "refresh_token": "secrettoken",
                "authenticator": "oauth",
                "token_endpoint": "https://www.example.com/oauth/token",
            },
        }
        mock_get_conn_params.return_value = connection_kwargs
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn_params = hook._get_conn_params()

        conn_params_keys = conn_params.keys()
        conn_params_extra = conn_params.get("extra", {})
        conn_params_extra_keys = conn_params_extra.keys()

        assert "authenticator" in conn_params_extra_keys
        assert conn_params_extra["authenticator"] == "oauth"
        assert conn_params_extra["token_endpoint"] == "https://www.example.com/oauth/token"

        assert "user" not in conn_params_keys
        assert "password" in conn_params_keys
        assert "refresh_token" in conn_params_extra_keys
        # Mandatory fields to generate account_identifier `https://<account>.<region>`
        assert "region" in conn_params_extra_keys
        assert "account" in conn_params_extra_keys

    @mock.patch("requests.post")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook._get_conn_params",
    )
    def test_get_conn_params_should_support_oauth_with_client_credentials(
        self, mock_get_conn_params, requests_post
    ):
        requests_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "access_token": "supersecretaccesstoken",
                "expires_in": 600,
                "refresh_token": "secrettoken",
                "token_type": "Bearer",
                "username": "test_user",
            },
        )
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "login": "test_client_id",
            "password": "test_client_secret",
            "extra": {
                "database": "db",
                "account": "airflow",
                "warehouse": "af_wh",
                "region": "af_region",
                "role": "af_role",
                "authenticator": "oauth",
                "token_endpoint": "https://www.example.com/oauth/token",
                "grant_type": "client_credentials",
            },
        }
        mock_get_conn_params.return_value = connection_kwargs
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn_params = hook._get_conn_params()

        conn_params_keys = conn_params.keys()
        conn_params_extra = conn_params.get("extra", {})
        conn_params_extra_keys = conn_params_extra.keys()

        assert "authenticator" in conn_params_extra_keys
        assert conn_params_extra["authenticator"] == "oauth"
        assert conn_params_extra["grant_type"] == "client_credentials"

        assert "user" not in conn_params_keys
        assert "password" in conn_params_keys
        assert "refresh_token" not in conn_params_extra_keys
        # Mandatory fields to generate account_identifier `https://<account>.<region>`
        assert "region" in conn_params_extra_keys
        assert "account" in conn_params_extra_keys

    def test_get_conn_params_should_support_oauth_with_azure_conn_id(self, mocker):
        azure_conn_id = "azure_test_conn"
        mock_azure_token = "azure_test_token"
        connection_kwargs = {
            "extra": {
                "database": "db",
                "account": "airflow",
                "region": "af_region",
                "warehouse": "af_wh",
                "authenticator": "oauth",
                "azure_conn_id": azure_conn_id,
            },
        }

        mock_connection_class = mocker.patch("airflow.providers.snowflake.hooks.snowflake.Connection")
        mock_azure_base_hook = mock_connection_class.get.return_value.get_hook.return_value
        mock_azure_base_hook.get_token.return_value.token = mock_azure_token

        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn_params = hook._get_conn_params()

        # Check AzureBaseHook initialization and get_token call args
        mock_connection_class.get.assert_called_once_with(azure_conn_id)
        mock_azure_base_hook.get_token.assert_called_once_with(SnowflakeHook.default_azure_oauth_scope)

        assert "authenticator" in conn_params
        assert conn_params["authenticator"] == "oauth"
        assert "token" in conn_params
        assert conn_params["token"] == mock_azure_token

        assert "user" not in conn_params
        assert "password" not in conn_params
        assert "refresh_token" not in conn_params
        # Mandatory fields to generate account_identifier `https://<account>.<region>`
        assert "region" in conn_params
        assert "account" in conn_params

    @mock.patch("requests.post")
    def test_get_conn_params_include_scope(self, mock_requests_post):
        """
        Verify that `_get_conn_params` includes the `scope` field when it is present
        in the connection extras.
        """
        mock_requests_post.return_value = Mock(
            status_code=200,
            json=lambda: {
                "access_token": "dummy",
                "expires_in": 600,
                "token_type": "Bearer",
                "username": "test_user",
            },
        )

        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "login": "test_client_id",
            "password": "test_client_secret",
            "extra": {
                "account": "airflow",
                "authenticator": "oauth",
                "grant_type": "client_credentials",
                "scope": "default",
            },
        }

        with mock.patch.dict(
            "os.environ",
            {"AIRFLOW_CONN_TEST_CONN": Connection(**connection_kwargs).get_uri()},
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            params = hook._get_conn_params()
        mock_requests_post.assert_called_once()
        assert "scope" in params
        assert params["scope"] == "default"

    def test_should_add_partner_info(self):
        with mock.patch.dict(
            "os.environ",
            AIRFLOW_CONN_TEST_CONN=Connection(**BASE_CONNECTION_KWARGS).get_uri(),
            AIRFLOW_SNOWFLAKE_PARTNER="PARTNER_NAME",
        ):
            assert (
                SnowflakeHook(snowflake_conn_id="test_conn")._get_conn_params()["application"]
                == "PARTNER_NAME"
            )

    def test_get_conn_should_call_connect(self):
        with (
            mock.patch.dict(
                "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**BASE_CONNECTION_KWARGS).get_uri()
            ),
            mock.patch("airflow.providers.snowflake.hooks.snowflake.connector") as mock_connector,
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn = hook.get_conn()
            mock_connector.connect.assert_called_once_with(**hook._get_conn_params())
            assert mock_connector.connect.return_value == conn

    def test_get_sqlalchemy_engine_should_support_pass_auth(self):
        with (
            mock.patch.dict(
                "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**BASE_CONNECTION_KWARGS).get_uri()
            ),
            mock.patch("airflow.providers.snowflake.hooks.snowflake.create_engine") as mock_create_engine,
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn = hook.get_sqlalchemy_engine()
            mock_create_engine.assert_called_once_with(
                "snowflake://user:pw@airflow.af_region/db/public"
                "?application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh"
            )
            assert mock_create_engine.return_value == conn

    def test_get_sqlalchemy_engine_should_support_insecure_mode(self):
        connection_kwargs = deepcopy(BASE_CONNECTION_KWARGS)
        connection_kwargs["extra"]["extra__snowflake__insecure_mode"] = "True"

        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()),
            mock.patch("airflow.providers.snowflake.hooks.snowflake.create_engine") as mock_create_engine,
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn = hook.get_sqlalchemy_engine()
            mock_create_engine.assert_called_once_with(
                "snowflake://user:pw@airflow.af_region/db/public"
                "?application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh",
                connect_args={"insecure_mode": True},
            )
            assert mock_create_engine.return_value == conn

    def test_get_sqlalchemy_engine_should_support_json_result_force_utf8_decoding(self):
        connection_kwargs = deepcopy(BASE_CONNECTION_KWARGS)
        connection_kwargs["extra"]["extra__snowflake__json_result_force_utf8_decoding"] = "True"

        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()),
            mock.patch("airflow.providers.snowflake.hooks.snowflake.create_engine") as mock_create_engine,
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn = hook.get_sqlalchemy_engine()
            mock_create_engine.assert_called_once_with(
                "snowflake://user:pw@airflow.af_region/db/public"
                "?application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh",
                connect_args={"json_result_force_utf8_decoding": True},
            )
            assert mock_create_engine.return_value == conn

    def test_get_sqlalchemy_engine_should_support_session_parameters(self):
        connection_kwargs = deepcopy(BASE_CONNECTION_KWARGS)
        connection_kwargs["extra"]["session_parameters"] = {"TEST_PARAM": "AA", "TEST_PARAM_B": 123}

        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()),
            mock.patch("airflow.providers.snowflake.hooks.snowflake.create_engine") as mock_create_engine,
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn = hook.get_sqlalchemy_engine()
            mock_create_engine.assert_called_once_with(
                "snowflake://user:pw@airflow.af_region/db/public"
                "?application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh",
                connect_args={"session_parameters": {"TEST_PARAM": "AA", "TEST_PARAM_B": 123}},
            )
            assert mock_create_engine.return_value == conn

    def test_get_sqlalchemy_engine_should_support_private_key_auth(self, unencrypted_temporary_private_key):
        connection_kwargs = deepcopy(BASE_CONNECTION_KWARGS)
        connection_kwargs["password"] = ""
        connection_kwargs["extra"]["private_key_file"] = str(unencrypted_temporary_private_key)

        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()),
            mock.patch("airflow.providers.snowflake.hooks.snowflake.create_engine") as mock_create_engine,
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn = hook.get_sqlalchemy_engine()
            assert "private_key" in mock_create_engine.call_args.kwargs["connect_args"]
            assert mock_create_engine.return_value == conn

    def test_get_sqlalchemy_engine_should_support_ocsp_fail_open(self):
        connection_kwargs = deepcopy(BASE_CONNECTION_KWARGS)
        connection_kwargs["extra"]["ocsp_fail_open"] = "False"

        with (
            mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()),
            mock.patch("airflow.providers.snowflake.hooks.snowflake.create_engine") as mock_create_engine,
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            conn = hook.get_sqlalchemy_engine()
            mock_create_engine.assert_called_once_with(
                "snowflake://user:pw@airflow.af_region/db/public"
                "?application=AIRFLOW&authenticator=snowflake&role=af_role&warehouse=af_wh",
                connect_args={"ocsp_fail_open": False},
            )
            assert mock_create_engine.return_value == conn

    def test_hook_parameters_should_take_precedence(self):
        with mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ):
            hook = SnowflakeHook(
                snowflake_conn_id="test_conn",
                account="TEST_ACCOUNT",
                warehouse="TEST_WAREHOUSE",
                database="TEST_DATABASE",
                region="TEST_REGION",
                role="TEST_ROLE",
                schema="TEST_SCHEMA",
                authenticator="TEST_AUTH",
                session_parameters={"AA": "AAA"},
            )
            assert hook._get_conn_params() == {
                "account": "TEST_ACCOUNT",
                "application": "AIRFLOW",
                "authenticator": "TEST_AUTH",
                "database": "TEST_DATABASE",
                "password": "pw",
                "region": "TEST_REGION",
                "role": "TEST_ROLE",
                "schema": "TEST_SCHEMA",
                "session_parameters": {"AA": "AAA"},
                "user": "user",
                "warehouse": "TEST_WAREHOUSE",
            }
            assert hook.get_uri() == (
                "snowflake://user:pw@TEST_ACCOUNT.TEST_REGION/TEST_DATABASE/TEST_SCHEMA"
                "?application=AIRFLOW&authenticator=TEST_AUTH&role=TEST_ROLE&warehouse=TEST_WAREHOUSE"
            )

    @pytest.mark.parametrize(
        ("sql", "expected_sql", "expected_query_ids"),
        [
            ("select * from table", ["select * from table"], ["uuid"]),
            (
                "select * from table;select * from table2",
                ["select * from table;", "select * from table2"],
                ["uuid1", "uuid2"],
            ),
            (["select * from table;"], ["select * from table;"], ["uuid1"]),
            (
                ["select * from table;", "select * from table2;"],
                ["select * from table;", "select * from table2;"],
                ["uuid1", "uuid2"],
            ),
        ],
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.get_conn")
    def test_run_storing_query_ids_extra(self, mock_conn, sql, expected_sql, expected_query_ids):
        hook = SnowflakeHook()
        conn = mock_conn.return_value
        cur = mock.MagicMock(rowcount=0)
        conn.cursor.return_value = cur
        type(cur).sfqid = mock.PropertyMock(side_effect=expected_query_ids)
        mock_params = {"mock_param": "mock_param"}
        hook.run(sql, parameters=mock_params)

        cur.execute.assert_has_calls([mock.call(query, mock_params) for query in expected_sql])
        assert hook.query_ids == expected_query_ids
        cur.close.assert_called()

    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_first")
    def test_connection_success(self, mock_get_first):
        with mock.patch.dict(
            "os.environ", AIRFLOW_CONN_SNOWFLAKE_DEFAULT=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ):
            hook = SnowflakeHook()
            mock_get_first.return_value = [{"1": 1}]
            status, msg = hook.test_connection()
            assert status is True
            assert msg == "Connection successfully tested"
            mock_get_first.assert_called_once_with("select 1")

    @mock.patch(
        "airflow.providers.common.sql.hooks.sql.DbApiHook.get_first",
        side_effect=Exception("Connection Errors"),
    )
    def test_connection_failure(self, mock_get_first):
        with mock.patch.dict(
            "os.environ", AIRFLOW_CONN_SNOWFLAKE_DEFAULT=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ):
            hook = SnowflakeHook()
            status, msg = hook.test_connection()
            assert status is False
            assert msg == "Connection Errors"
            mock_get_first.assert_called_once_with("select 1")

    def test_empty_sql_parameter(self):
        hook = SnowflakeHook()

        for empty_statement in ([], "", "\n"):
            with pytest.raises(ValueError, match="List of SQL statements is empty"):
                hook.run(sql=empty_statement)

    def test_get_openlineage_default_schema_with_no_schema_set(self):
        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "schema": "PUBLIC",
        }
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri()):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            assert hook.get_openlineage_default_schema() == "PUBLIC"

    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.get_first")
    def test_get_openlineage_default_schema_with_schema_set(self, mock_get_first):
        with mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            assert hook.get_openlineage_default_schema() == BASE_CONNECTION_KWARGS["schema"]
            mock_get_first.assert_not_called()

            hook_with_schema_param = SnowflakeHook(snowflake_conn_id="test_conn", schema="my_schema")
            assert hook_with_schema_param.get_openlineage_default_schema() == "my_schema"
            mock_get_first.assert_not_called()

    @mock.patch("airflow.providers.snowflake.utils.openlineage.emit_openlineage_events_for_snowflake_queries")
    def test_get_openlineage_database_specific_lineage_with_no_query_ids(self, mock_emit):
        hook = SnowflakeHook(snowflake_conn_id="test_conn")
        assert hook.query_ids == []

        result = hook.get_openlineage_database_specific_lineage(None)
        mock_emit.assert_not_called()
        assert result is None

    @mock.patch("airflow.providers.snowflake.utils.openlineage.emit_openlineage_events_for_snowflake_queries")
    def test_get_openlineage_database_specific_lineage_with_single_query_id(self, mock_emit):
        from airflow.providers.common.compat.openlineage.facet import ExternalQueryRunFacet
        from airflow.providers.openlineage.extractors import OperatorLineage

        hook = SnowflakeHook(snowflake_conn_id="test_conn")
        hook.query_ids = ["query1"]
        hook.get_connection = mock.MagicMock()
        hook.get_openlineage_database_info = lambda x: mock.MagicMock(authority="auth", scheme="scheme")

        ti = mock.MagicMock()

        result = hook.get_openlineage_database_specific_lineage(ti)
        mock_emit.assert_called_once_with(
            **{
                "hook": hook,
                "query_ids": ["query1"],
                "query_source_namespace": "scheme://auth",
                "task_instance": ti,
                "query_for_extra_metadata": True,
            }
        )
        assert result == OperatorLineage(
            run_facets={
                "externalQuery": ExternalQueryRunFacet(externalQueryId="query1", source="scheme://auth")
            }
        )

    @mock.patch("airflow.providers.snowflake.utils.openlineage.emit_openlineage_events_for_snowflake_queries")
    def test_get_openlineage_database_specific_lineage_with_multiple_query_ids(self, mock_emit):
        hook = SnowflakeHook(snowflake_conn_id="test_conn")
        hook.query_ids = ["query1", "query2"]
        hook.get_connection = mock.MagicMock()
        hook.get_openlineage_database_info = lambda x: mock.MagicMock(authority="auth", scheme="scheme")

        ti = mock.MagicMock()

        result = hook.get_openlineage_database_specific_lineage(ti)
        mock_emit.assert_called_once_with(
            **{
                "hook": hook,
                "query_ids": ["query1", "query2"],
                "query_source_namespace": "scheme://auth",
                "task_instance": ti,
                "query_for_extra_metadata": True,
            }
        )
        assert result is None

    @mock.patch("importlib.metadata.version", return_value="1.99.0")
    def test_get_openlineage_database_specific_lineage_with_old_openlineage_provider(self, mock_version):
        hook = SnowflakeHook(snowflake_conn_id="test_conn")
        hook.query_ids = ["query1", "query2"]
        hook.get_connection = mock.MagicMock()
        hook.get_openlineage_database_info = lambda x: mock.MagicMock(authority="auth", scheme="scheme")

        expected_err = (
            "OpenLineage provider version `1.99.0` is lower than required `2.5.0`, "
            "skipping function `emit_openlineage_events_for_snowflake_queries` execution"
        )
        with pytest.raises(AirflowOptionalProviderFeatureException, match=expected_err):
            hook.get_openlineage_database_specific_lineage(mock.MagicMock())

    @pytest.mark.skipif(sys.version_info >= (3, 12), reason="Snowpark Python doesn't support Python 3.12 yet")
    @mock.patch("snowflake.snowpark.Session.builder")
    def test_get_snowpark_session(self, mock_session_builder):
        from airflow import __version__ as airflow_version
        from airflow.providers.snowflake import __version__ as provider_version

        mock_session = mock.MagicMock()
        mock_session_builder.configs.return_value.create.return_value = mock_session

        with mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            session = hook.get_snowpark_session()
            assert session == mock_session

            mock_session_builder.configs.assert_called_once_with(hook._get_conn_params())

            # Verify that update_query_tag was called with the expected tag dictionary
            mock_session.update_query_tag.assert_called_once_with(
                {
                    "airflow_version": airflow_version,
                    "airflow_provider_version": provider_version,
                }
            )

    @mock.patch("airflow.providers.snowflake.hooks.snowflake.HTTPBasicAuth")
    @mock.patch("requests.post")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook._get_conn_params",
        new_callable=PropertyMock,
    )
    def test_get_oauth_token(self, mock_conn_param, requests_post, mock_auth):
        """Test get_oauth_token method makes the right http request"""
        basic_auth = {"Authorization": "Basic usernamepassword"}
        mock_conn_param.return_value = CONN_PARAMS_OAUTH
        requests_post.return_value.status_code = 200
        mock_auth.return_value = basic_auth
        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        hook.get_oauth_token(conn_config=CONN_PARAMS_OAUTH)
        requests_post.assert_called_once_with(
            f"https://{CONN_PARAMS_OAUTH['account']}.snowflakecomputing.com/oauth/token-request",
            data={
                "grant_type": "refresh_token",
                "refresh_token": CONN_PARAMS_OAUTH["refresh_token"],
                "redirect_uri": "https://localhost.com",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=basic_auth,
            timeout=30,
        )

    @mock.patch("airflow.providers.snowflake.hooks.snowflake.HTTPBasicAuth")
    @mock.patch("requests.post")
    @mock.patch(
        "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook._get_conn_params",
        new_callable=PropertyMock,
    )
    def test_get_oauth_token_with_token_endpoint(self, mock_conn_param, requests_post, mock_auth):
        """Test get_oauth_token method makes the right http request"""
        basic_auth = {"Authorization": "Basic usernamepassword"}
        token_endpoint = "https://example.com/oauth/token"
        mock_conn_param.return_value = CONN_PARAMS_OAUTH
        requests_post.return_value.status_code = 200
        mock_auth.return_value = basic_auth

        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        hook.get_oauth_token(conn_config=CONN_PARAMS_OAUTH, token_endpoint=token_endpoint)

        requests_post.assert_called_once_with(
            token_endpoint,
            data={
                "grant_type": "refresh_token",
                "refresh_token": CONN_PARAMS_OAUTH["refresh_token"],
                "redirect_uri": "https://localhost.com",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            auth=basic_auth,
            timeout=30,
        )

    def test_get_azure_oauth_token(self, mocker):
        """Test get_azure_oauth_token method gets token from provided connection id"""
        azure_conn_id = "azure_test_conn"
        mock_azure_token = "azure_test_token"

        mock_connection_class = mocker.patch("airflow.providers.snowflake.hooks.snowflake.Connection")
        mock_azure_base_hook = mock_connection_class.get.return_value.get_hook.return_value
        mock_azure_base_hook.get_token.return_value.token = mock_azure_token

        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        token = hook.get_azure_oauth_token(azure_conn_id)

        # Check AzureBaseHook initialization and get_token call args
        mock_connection_class.get.assert_called_once_with(azure_conn_id)
        mock_azure_base_hook.get_token.assert_called_once_with(SnowflakeHook.default_azure_oauth_scope)
        assert token == mock_azure_token

    def test_get_azure_oauth_token_expect_failure_on_older_azure_provider_package(self, mocker):
        class MockAzureBaseHookOldVersion:
            """Simulate an old version of AzureBaseHook where sdk_client is required."""

            def __init__(self, sdk_client, conn_id="azure_default"):
                pass

        azure_conn_id = "azure_test_conn"
        mock_connection_class = mocker.patch("airflow.providers.snowflake.hooks.snowflake.Connection")
        mock_connection_class.get.return_value.get_hook = MockAzureBaseHookOldVersion

        hook = SnowflakeHook(snowflake_conn_id="mock_conn_id")
        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match=(
                "Getting azure token is not supported.*"
                "Please upgrade apache-airflow-providers-microsoft-azure>="
            ),
        ):
            hook.get_azure_oauth_token(azure_conn_id)

        # Check AzureBaseHook initialization
        mock_connection_class.get.assert_called_once_with(azure_conn_id)

    @mock.patch("requests.post")
    def test_get_oauth_token_with_scope(self, mock_requests_post):
        """
        Verify that `get_oauth_token` returns an access token and includes the
        provided scope in the outgoing OAuth request payload.
        """

        mock_requests_post.return_value = Mock(
            status_code=200,
            json=lambda: {"access_token": "dummy_token", "expires_in": 600},
        )

        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "login": "client_id",
            "password": "client_secret",
            "extra": {
                "account": "airflow",
                "authenticator": "oauth",
                "grant_type": "client_credentials",
                "scope": "default",
            },
        }

        with mock.patch.dict(
            "os.environ",
            {"AIRFLOW_CONN_TEST_CONN": Connection(**connection_kwargs).get_uri()},
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            token = hook.get_oauth_token(grant_type="client_credentials")

        assert token == "dummy_token"

        called_data = mock_requests_post.call_args.kwargs["data"]

        assert called_data["scope"] == "default"
        assert called_data["grant_type"] == "client_credentials"

    @mock.patch("requests.post")
    def test_get_oauth_token_without_scope(self, mock_requests_post):
        """
        Verify that `get_oauth_token` returns an access token`
        when no scope is defined in the connection extras.
        """
        mock_requests_post.return_value = Mock(
            status_code=200,
            json=lambda: {"access_token": "dummy_token", "expires_in": 600},
        )

        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "login": "client_id",
            "password": "client_secret",
            "extra": {"account": "airflow", "authenticator": "oauth", "grant_type": "client_credentials"},
        }

        with mock.patch.dict(
            "os.environ",
            {"AIRFLOW_CONN_TEST_CONN": Connection(**connection_kwargs).get_uri()},
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            token = hook.get_oauth_token(grant_type="client_credentials")

        assert token == "dummy_token"

        called_data = mock_requests_post.call_args.kwargs["data"]

        assert "scope" not in called_data
        assert called_data["grant_type"] == "client_credentials"

    @mock.patch("requests.post")
    @mock.patch("airflow.providers.snowflake.hooks.snowflake.timezone.utcnow")
    def test_oauth_token_refresh_after_expiry(self, mock_timezone_utcnow, mock_requests_post):
        """
        Ensure OAuth tokens are refreshed after expiry for a reused SnowflakeHook,
        without mutating static connection parameters.
        """

        t0 = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

        # _get_valid_oauth_token calls utcnow twice per refresh:
        #   1) validity check
        #   2) issued_at
        mock_timezone_utcnow.side_effect = [
            t0,
            t0,
            t0 + timedelta(minutes=11),
            t0 + timedelta(minutes=11),
        ]

        mock_requests_post.side_effect = [
            Mock(
                status_code=200,
                json=lambda: {"access_token": "token1", "expires_in": 600},
                raise_for_status=lambda: None,
            ),
            Mock(
                status_code=200,
                json=lambda: {"access_token": "token2", "expires_in": 600},
                raise_for_status=lambda: None,
            ),
        ]

        connection_kwargs = {
            **BASE_CONNECTION_KWARGS,
            "login": "client_id",
            "password": "client_secret",
            "extra": {
                "account": "airflow",
                "authenticator": "oauth",
                "grant_type": "refresh_token",
                "refresh_token": "secret_token",
            },
        }

        with mock.patch.dict(
            "os.environ",
            {"AIRFLOW_CONN_TEST_CONN": Connection(**connection_kwargs).get_uri()},
        ):
            hook = SnowflakeHook(snowflake_conn_id="test_conn")
            # First resolution (initial token)
            conn_params_1 = hook._get_conn_params()

            # Second resolution (after token expiry)
            conn_params_2 = hook._get_conn_params()

        # Token must be refreshed
        assert conn_params_1["token"] == "token1"
        assert conn_params_2["token"] == "token2"

        # Static params must not change
        assert {k: v for k, v in conn_params_1.items() if k != "token"} == {
            k: v for k, v in conn_params_2.items() if k != "token"
        }

        # Ensure refresh actually happened
        assert mock_requests_post.call_count == 2
