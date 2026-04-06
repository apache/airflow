# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for AkeylessBackend secrets backend."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

MODULE = "airflow.providers.akeyless._internal_client.akeyless_client._AkeylessClient"


def _backend(**overrides):
    from airflow.providers.akeyless.secrets.akeyless import AkeylessBackend

    defaults = dict(
        api_url="https://api.akeyless.io",
        access_id="p-test123",
        access_key="test-key",
        access_type="api_key",
    )
    defaults.update(overrides)
    return AkeylessBackend(**defaults)


class TestAkeylessBackend:
    @patch(f"{MODULE}.get_secret_value", return_value="postgresql://user:pw@host/db")
    def test_get_connection_uri(self, mock_get):
        backend = _backend()
        conn = backend.get_connection("postgres_default")
        assert conn is not None
        assert conn.conn_type == "postgresql"
        mock_get.assert_called_once_with("/airflow/connections/postgres_default")

    @patch(
        f"{MODULE}.get_secret_value",
        return_value=json.dumps({"conn_uri": "postgresql://u:p@h/d"}),
    )
    def test_get_connection_json_uri(self, mock_get):
        backend = _backend()
        conn = backend.get_connection("pg")
        assert conn is not None
        assert conn.conn_type == "postgresql"

    @patch(
        f"{MODULE}.get_secret_value",
        return_value=json.dumps(
            {
                "conn_type": "postgres",
                "host": "db.example.com",
                "login": "admin",
                "password": "s3cr3t",
                "schema": "mydb",
            }
        ),
    )
    def test_get_connection_json_kwargs(self, mock_get):
        backend = _backend()
        conn = backend.get_connection("pg_kwargs")
        assert conn is not None
        assert conn.host == "db.example.com"
        assert conn.login == "admin"

    @patch(f"{MODULE}.get_secret_value", return_value=None)
    def test_get_connection_not_found(self, mock_get):
        backend = _backend()
        conn = backend.get_connection("nonexistent")
        assert conn is None

    @patch(f"{MODULE}.get_secret_value", return_value=None)
    def test_get_connection_disabled(self, mock_get):
        backend = _backend(connections_path=None)
        conn = backend.get_connection("anything")
        assert conn is None
        mock_get.assert_not_called()

    @patch(f"{MODULE}.get_secret_value", return_value="plain-value")
    def test_get_variable_plain(self, mock_get):
        backend = _backend()
        val = backend.get_variable("my_var")
        assert val == "plain-value"
        mock_get.assert_called_once_with("/airflow/variables/my_var")

    @patch(
        f"{MODULE}.get_secret_value",
        return_value=json.dumps({"value": "json-wrapped"}),
    )
    def test_get_variable_json_value_key(self, mock_get):
        backend = _backend()
        val = backend.get_variable("my_var")
        assert val == "json-wrapped"

    @patch(f"{MODULE}.get_secret_value", return_value=None)
    def test_get_variable_not_found(self, mock_get):
        backend = _backend()
        val = backend.get_variable("missing")
        assert val is None

    @patch(f"{MODULE}.get_secret_value", return_value=None)
    def test_get_variable_disabled(self, mock_get):
        backend = _backend(variables_path=None)
        val = backend.get_variable("anything")
        assert val is None
        mock_get.assert_not_called()

    @patch(f"{MODULE}.get_secret_value", return_value="config-val")
    def test_get_config_plain(self, mock_get):
        backend = _backend()
        val = backend.get_config("smtp_host")
        assert val == "config-val"
        mock_get.assert_called_once_with("/airflow/config/smtp_host")

    @patch(
        f"{MODULE}.get_secret_value",
        return_value=json.dumps({"value": "wrapped-config"}),
    )
    def test_get_config_json_value_key(self, mock_get):
        backend = _backend()
        val = backend.get_config("smtp_host")
        assert val == "wrapped-config"

    @patch(f"{MODULE}.get_secret_value", return_value=None)
    def test_get_config_not_found(self, mock_get):
        backend = _backend()
        val = backend.get_config("missing")
        assert val is None

    @patch(f"{MODULE}.get_secret_value", return_value=None)
    def test_get_config_disabled(self, mock_get):
        backend = _backend(config_path=None)
        val = backend.get_config("anything")
        assert val is None
        mock_get.assert_not_called()

    @patch(f"{MODULE}.get_secret_value", return_value="val")
    def test_custom_separator(self, mock_get):
        backend = _backend(variables_path="/vars", sep="-")
        val = backend.get_variable("key")
        mock_get.assert_called_once_with("/vars-key")


class TestAkeylessClientValidation:
    def test_invalid_auth_type(self):
        from airflow.providers.akeyless._internal_client.akeyless_client import (
            AkeylessClientError,
            _AkeylessClient,
        )

        with pytest.raises(AkeylessClientError, match="Unsupported access_type"):
            _AkeylessClient(access_type="invalid", access_id="x", access_key="y")

    def test_api_key_missing_access_key(self):
        from airflow.providers.akeyless._internal_client.akeyless_client import (
            AkeylessClientError,
            _AkeylessClient,
        )

        with pytest.raises(AkeylessClientError, match="access_key"):
            _AkeylessClient(access_type="api_key", access_id="x")

    def test_uid_missing_token(self):
        from airflow.providers.akeyless._internal_client.akeyless_client import (
            AkeylessClientError,
            _AkeylessClient,
        )

        with pytest.raises(AkeylessClientError, match="uid_token"):
            _AkeylessClient(access_type="uid")

    def test_jwt_missing_jwt(self):
        from airflow.providers.akeyless._internal_client.akeyless_client import (
            AkeylessClientError,
            _AkeylessClient,
        )

        with pytest.raises(AkeylessClientError, match="'jwt' auth requires 'jwt'"):
            _AkeylessClient(access_type="jwt", access_id="x")

    def test_certificate_missing_key(self):
        from airflow.providers.akeyless._internal_client.akeyless_client import (
            AkeylessClientError,
            _AkeylessClient,
        )

        with pytest.raises(AkeylessClientError, match="certificate_data"):
            _AkeylessClient(access_type="certificate", access_id="x")
