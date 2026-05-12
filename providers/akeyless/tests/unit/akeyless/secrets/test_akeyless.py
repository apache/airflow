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
"""Tests for AkeylessBackend secrets backend."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from tests_common.test_utils.config import conf_vars

BACKEND_MODULE = "airflow.providers.akeyless.secrets.akeyless"


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
    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_connection_uri(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {
            "/airflow/connections/postgres_default": "postgresql://user:secret123@host/db"
        }
        backend = _backend()
        conn = backend.get_connection("postgres_default")
        assert conn is not None
        assert conn.host == "host"
        assert conn.login == "user"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_connection_json_uri(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {
            "/airflow/connections/pg": json.dumps({"conn_uri": "postgresql://user:secret123@dbhost/mydb"})
        }
        backend = _backend()
        conn = backend.get_connection("pg")
        assert conn is not None
        assert conn.host == "dbhost"
        assert conn.login == "user"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_connection_json_kwargs(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {
            "/airflow/connections/pg_kwargs": json.dumps(
                {
                    "conn_type": "postgres",
                    "host": "db.example.com",
                    "login": "admin",
                    "password": "s3cr3t",
                    "schema": "mydb",
                }
            )
        }
        backend = _backend()
        conn = backend.get_connection("pg_kwargs")
        assert conn is not None
        assert conn.host == "db.example.com"
        assert conn.login == "admin"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_connection_not_found(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        mock_sdk.ApiException = Exception
        api.get_secret_value.side_effect = Exception("not found")
        backend = _backend()
        conn = backend.get_connection("nonexistent")
        assert conn is None

    def test_get_connection_disabled(self):
        backend = _backend(connections_path=None)
        conn = backend.get_connection("anything")
        assert conn is None

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_plain(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/my_var": "plain-value"}
        backend = _backend()
        val = backend.get_variable("my_var")
        assert val == "plain-value"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_json_value_key(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {
            "/airflow/variables/my_var": json.dumps({"value": "json-wrapped"})
        }
        backend = _backend()
        val = backend.get_variable("my_var")
        assert val == "json-wrapped"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_not_found(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        mock_sdk.ApiException = Exception
        api.get_secret_value.side_effect = Exception("not found")
        backend = _backend()
        val = backend.get_variable("missing")
        assert val is None

    def test_get_variable_disabled(self):
        backend = _backend(variables_path=None)
        val = backend.get_variable("anything")
        assert val is None

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_config_plain(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/config/smtp_host": "config-val"}
        backend = _backend()
        val = backend.get_config("smtp_host")
        assert val == "config-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_config_json_value_key(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {
            "/airflow/config/smtp_host": json.dumps({"value": "wrapped-config"})
        }
        backend = _backend()
        val = backend.get_config("smtp_host")
        assert val == "wrapped-config"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_config_not_found(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        mock_sdk.ApiException = Exception
        api.get_secret_value.side_effect = Exception("not found")
        backend = _backend()
        val = backend.get_config("missing")
        assert val is None

    def test_get_config_disabled(self):
        backend = _backend(config_path=None)
        val = backend.get_config("anything")
        assert val is None

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_custom_separator(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/vars-key": "val"}
        backend = _backend(variables_path="/vars", sep="-")
        val = backend.get_variable("key")
        assert val == "val"

    def test_unsupported_access_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported access_type"):
            _backend(access_type="aws_iam")

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_token_caching(self, mock_sdk):
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="cached-token")
        api.get_secret_value.return_value = {"/airflow/variables/a": "v1"}
        backend = _backend()
        backend.get_variable("a")
        backend.get_variable("a")
        assert api.auth.call_count == 1

    # ------------------------------------------------------------------
    # Multi-team tests
    # ------------------------------------------------------------------

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_team_scoped(self, mock_sdk):
        """When multi_team is enabled, look up under {base}/{team}/{key}."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/analytics/my_var": "team-val"}
        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            val = backend.get_variable("my_var", team_name="analytics")
        assert val == "team-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_team_fallback_to_global(self, mock_sdk):
        """Team lookup misses, falls back to {base}/{key}."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        mock_sdk.ApiException = Exception
        api.get_secret_value.side_effect = [
            Exception("not found"),
            {"/airflow/variables/my_var": "global-val"},
        ]
        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            val = backend.get_variable("my_var", team_name="analytics")
        assert val == "global-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_team_fallback_to_global_path(self, mock_sdk):
        """Team lookup misses, falls back to {base}/{global_path}/{key}."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        mock_sdk.ApiException = Exception
        api.get_secret_value.side_effect = [
            Exception("not found"),
            {"/airflow/variables/global/my_var": "shared-val"},
        ]
        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend(global_secrets_path="global")
            val = backend.get_variable("my_var", team_name="analytics")
        assert val == "shared-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_no_team_separation(self, mock_sdk):
        """use_team_secrets_path=False skips team prefix even with team_name."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/my_var": "plain"}
        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend(use_team_secrets_path=False)
            val = backend.get_variable("my_var", team_name="analytics")
        assert val == "plain"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_connection_team_scoped(self, mock_sdk):
        """Team-scoped connection lookup."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {
            "/airflow/connections/team_a/pg": "postgresql://user:secret123@dbhost/mydb"
        }
        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            conn = backend.get_connection("pg", team_name="team_a")
        assert conn is not None
        assert conn.host == "dbhost"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_no_team_when_multi_team_off(self, mock_sdk):
        """Without multi_team config, team_name is ignored."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/my_var": "plain"}
        backend = _backend()
        val = backend.get_variable("my_var", team_name="analytics")
        assert val == "plain"
        mock_sdk.GetSecretValue.assert_called_with(names=["/airflow/variables/my_var"], token="t")
