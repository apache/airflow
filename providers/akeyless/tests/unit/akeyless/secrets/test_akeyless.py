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
            _backend(access_type="ldap")

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

    # ------------------------------------------------------------------
    # Cloud-based authentication tests
    # ------------------------------------------------------------------

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_aws_iam_auth(self, mock_sdk):
        """aws_iam auth generates a cloud ID and authenticates."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="aws-token")
        api.get_secret_value.return_value = {"/airflow/variables/v": "aws-val"}

        mock_cloud_id = MagicMock()
        mock_cloud_id.generate.return_value = "fake-aws-cloud-id"

        with patch(f"{BACKEND_MODULE}.AkeylessBackend._get_cloud_id", return_value="fake-aws-cloud-id"):
            backend = _backend(access_type="aws_iam", access_key=None)
            val = backend.get_variable("v")

        assert val == "aws-val"
        mock_sdk.Auth.assert_called_once_with(
            access_id="p-test123", access_type="aws_iam", cloud_id="fake-aws-cloud-id"
        )

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_gcp_auth(self, mock_sdk):
        """gcp auth generates a GCP cloud ID and authenticates."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="gcp-token")
        api.get_secret_value.return_value = {"/airflow/variables/v": "gcp-val"}

        with patch(f"{BACKEND_MODULE}.AkeylessBackend._get_cloud_id", return_value="fake-gcp-cloud-id"):
            backend = _backend(access_type="gcp", access_key=None, gcp_audience="my-audience")
            val = backend.get_variable("v")

        assert val == "gcp-val"
        mock_sdk.Auth.assert_called_once_with(
            access_id="p-test123", access_type="gcp", cloud_id="fake-gcp-cloud-id"
        )

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_azure_ad_auth(self, mock_sdk):
        """azure_ad auth generates an Azure cloud ID and authenticates."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="azure-token")
        api.get_secret_value.return_value = {"/airflow/variables/v": "azure-val"}

        with patch(f"{BACKEND_MODULE}.AkeylessBackend._get_cloud_id", return_value="fake-azure-id"):
            backend = _backend(access_type="azure_ad", access_key=None, azure_object_id="obj-123")
            val = backend.get_variable("v")

        assert val == "azure-val"
        mock_sdk.Auth.assert_called_once_with(
            access_id="p-test123", access_type="azure_ad", cloud_id="fake-azure-id"
        )

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_aws_iam_cloud_id_integration(self, mock_sdk):
        """aws_iam calls CloudId.generate() to produce the cloud identity."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/v": "val"}

        mock_cid_instance = MagicMock()
        mock_cid_instance.generate.return_value = "real-aws-cloud-id"
        mock_cloud_id_cls = MagicMock(return_value=mock_cid_instance)

        with patch.dict("sys.modules", {"akeyless_cloud_id": MagicMock(CloudId=mock_cloud_id_cls)}):
            backend = _backend(access_type="aws_iam", access_key=None)
            backend.get_variable("v")

        mock_cid_instance.generate.assert_called_once()

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_gcp_cloud_id_passes_audience(self, mock_sdk):
        """gcp auth passes gcp_audience to CloudId.generateGcp()."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/v": "val"}

        mock_cid_instance = MagicMock()
        mock_cid_instance.generateGcp.return_value = "gcp-id"
        mock_cloud_id_cls = MagicMock(return_value=mock_cid_instance)

        with patch.dict("sys.modules", {"akeyless_cloud_id": MagicMock(CloudId=mock_cloud_id_cls)}):
            backend = _backend(access_type="gcp", access_key=None, gcp_audience="my-aud")
            backend.get_variable("v")

        mock_cid_instance.generateGcp.assert_called_once_with("my-aud")

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_azure_cloud_id_passes_object_id(self, mock_sdk):
        """azure_ad auth passes azure_object_id to CloudId.generateAzure()."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/v": "val"}

        mock_cid_instance = MagicMock()
        mock_cid_instance.generateAzure.return_value = "azure-id"
        mock_cloud_id_cls = MagicMock(return_value=mock_cid_instance)

        with patch.dict("sys.modules", {"akeyless_cloud_id": MagicMock(CloudId=mock_cloud_id_cls)}):
            backend = _backend(access_type="azure_ad", access_key=None, azure_object_id="obj-456")
            backend.get_variable("v")

        mock_cid_instance.generateAzure.assert_called_once_with("obj-456")

    def test_cloud_auth_missing_package_raises(self):
        """Cloud auth raises ImportError when akeyless_cloud_id is not installed."""
        import sys

        backend = _backend(access_type="aws_iam", access_key=None)
        with patch.dict(sys.modules, {"akeyless_cloud_id": None}):
            with pytest.raises(ImportError, match="akeyless_cloud_id"):
                backend._get_cloud_id()

    # ------------------------------------------------------------------
    # Cross-team namespace escape
    # ------------------------------------------------------------------

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_variable_cannot_reach_another_teams_namespace(self, mock_sdk):
        """A key containing the separator must not resolve another team's secret.

        The team-scoped lookup misses and the team-agnostic fallback resolves
        ``{base}/{key}`` -- which is the prefix every other team's secrets live under.
        The backend is wired here so that the cross-team path *would* return a value,
        so the assertion is that team beta's secret does not come back, not merely
        that some guard ran.
        """
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        mock_sdk.ApiException = Exception
        api.get_secret_value.return_value = {"/airflow/variables/beta/db_password": "beta-secret"}

        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            val = backend.get_variable("beta/db_password", team_name="alpha")

        assert val is None
        api.get_secret_value.assert_not_called()

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_get_connection_cannot_reach_another_teams_namespace(self, mock_sdk):
        """The same escape is refused for connections."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        mock_sdk.ApiException = Exception
        api.get_secret_value.return_value = {"/airflow/connections/beta/prod_db": "postgres://u:p@h/d"}

        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            conn = backend.get_connection("beta/prod_db", team_name="alpha")

        assert conn is None
        api.get_secret_value.assert_not_called()

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_nested_config_ids_still_resolve_in_multi_team_mode(self, mock_sdk):
        """Config lookups are global and must keep working with subfolder layouts.

        ``get_config`` takes no ``team_name`` and Airflow does not do team-scoped config
        lookups through a secrets backend, so there is no boundary for a config id to
        cross. Guarding it would silently break subfolder config layouts on upgrade.
        """
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/config/db/sql_alchemy_conn": "postgres://x"}

        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            val = backend.get_config("db/sql_alchemy_conn")

        assert val == "postgres://x"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_nested_keys_still_resolve_when_team_paths_are_disabled(self, mock_sdk):
        """``use_team_secrets_path=False`` builds no team path, so nothing is refused.

        A deployment can run multi-team for other features while keeping Akeyless as a
        single flat namespace. Banning separators there would be a pure regression.
        """
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/nested/my_var": "nested-val"}

        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend(use_team_secrets_path=False)
            val = backend.get_variable("nested/my_var", team_name="alpha")

        assert val == "nested-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_nested_keys_still_resolve_for_a_caller_with_no_team(self, mock_sdk):
        """With no team_name the lookup resolves in the shared namespace directly."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/nested/my_var": "nested-val"}

        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            val = backend.get_variable("nested/my_var")

        assert val == "nested-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_a_team_scoped_key_without_the_separator_still_resolves(self, mock_sdk):
        """The guard must not break ordinary team-scoped lookups."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/alpha/my_var": "team-val"}

        with conf_vars({("core", "multi_team"): "True"}):
            backend = _backend()
            val = backend.get_variable("my_var", team_name="alpha")

        assert val == "team-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_nested_keys_still_work_outside_multi_team_mode(self, mock_sdk):
        """Without team namespaces a separator in a key is an ordinary nested path."""
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/nested/my_var": "nested-val"}

        with conf_vars({("core", "multi_team"): "False"}):
            backend = _backend()
            val = backend.get_variable("nested/my_var")

        assert val == "nested-val"

    @patch(f"{BACKEND_MODULE}.akeyless")
    def test_multi_team_disabled_is_read_as_a_boolean(self, mock_sdk):
        """``multi_team = False`` must not select the multi-team code paths.

        The option was previously read with ``conf.get``, which yields the string
        ``"False"`` -- truthy -- so the global-path branch was taken even with
        multi-team off.
        """
        api = mock_sdk.V2Api.return_value
        api.auth.return_value = MagicMock(token="t")
        api.get_secret_value.return_value = {"/airflow/variables/my_var": "plain-val"}

        with conf_vars({("core", "multi_team"): "False"}):
            backend = _backend(global_secrets_path="global")
            val = backend.get_variable("my_var")

        assert val == "plain-val"
        # The global-secrets path must not have been consulted at all.
        requested = [c.kwargs["names"][0] for c in mock_sdk.GetSecretValue.call_args_list]
        assert requested == ["/airflow/variables/my_var"]
