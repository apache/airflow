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
            "/airflow/connections/postgres_default": "postgresql://user:pw@host/db"
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
            "/airflow/connections/pg": json.dumps({"conn_uri": "postgresql://u:p@h/d"})
        }
        backend = _backend()
        conn = backend.get_connection("pg")
        assert conn is not None
        assert conn.host == "h"
        assert conn.login == "u"

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
