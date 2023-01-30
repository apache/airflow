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
from unittest.mock import PropertyMock, mock_open, patch

import pytest
from hvac.exceptions import VaultError

from airflow.providers.hashicorp.hooks.vault import VaultHook


class TestVaultHook:
    @staticmethod
    def get_mock_connection(
        conn_type="vault", schema="secret", host="localhost", port=8180, user="user", password="pass"
    ):
        mock_connection = mock.MagicMock()
        type(mock_connection).conn_type = PropertyMock(return_value=conn_type)
        type(mock_connection).host = PropertyMock(return_value=host)
        type(mock_connection).port = PropertyMock(return_value=port)
        type(mock_connection).login = PropertyMock(return_value=user)
        type(mock_connection).password = PropertyMock(return_value=password)
        type(mock_connection).schema = PropertyMock(return_value=schema)
        return mock_connection

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_version_not_int(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {"auth_type": "userpass", "kv_engine_version": "text"}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        with pytest.raises(VaultError, match="The version is not an int: text"):
            VaultHook(**kwargs)

    @pytest.mark.parametrize(
        "version, expected_version",
        [
            ("2", 2),
            (1, 1),
        ],
    )
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_version(self, mock_hvac, mock_get_connection, version, expected_version):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {"auth_type": "userpass", "kv_engine_version": version}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        assert expected_version == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_custom_mount_point_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection(schema="custom")
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        assert "custom" == test_hook.vault_client.mount_point

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_custom_auth_mount_point_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_mount_point": "custom"}
        test_hook = VaultHook(**kwargs)
        assert "secret" == test_hook.vault_client.mount_point
        assert "custom" == test_hook.vault_client.auth_mount_point

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_custom_auth_mount_point_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {"auth_type": "userpass", "auth_mount_point": "custom"}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        assert "secret" == test_hook.vault_client.mount_point
        assert "custom" == test_hook.vault_client.auth_mount_point

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_version_one_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "kv_engine_version": 1,
            "vault_conn_id": "vault_conn_id",
        }
        test_hook = VaultHook(**kwargs)
        assert 1 == test_hook.vault_client.kv_engine_version

    @pytest.mark.parametrize(
        "protocol, expected_url",
        [
            ("vaults", "https://localhost:8180"),
            ("http", "http://localhost:8180"),
            ("https", "https://localhost:8180"),
        ],
    )
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_protocol(self, mock_hvac, mock_get_connection, protocol, expected_url):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection(conn_type=protocol)
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "approle",
            "kv_engine_version": 2,
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url=expected_url)
        test_client.auth.approle.login.assert_called_with(role_id="user", secret_id="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_approle_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "approle",
            "kv_engine_version": 2,
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.approle.login.assert_called_with(role_id="user", secret_id="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_approle_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {"auth_type": "approle"}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.approle.login.assert_called_with(role_id="user", secret_id="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_VAULT_CONN_ID="https://role:secret@vault.example.com?auth_type=approle",
    )
    def test_approle_uri(self, mock_hvac):
        test_hook = VaultHook(vault_conn_id="vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="https://vault.example.com")
        test_client.auth.approle.login.assert_called_with(role_id="role", secret_id="secret")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_aws_iam_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "aws_iam", "role_id": "role"}

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth_aws_iam.assert_called_with(
            access_key="user",
            secret_key="pass",
            role="role",
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_aws_iam_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {"auth_type": "aws_iam", "role_id": "role"}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth_aws_iam.assert_called_with(
            access_key="user",
            secret_key="pass",
            role="role",
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_VAULT_CONN_ID="https://login:pass@vault.example.com?auth_type=aws_iam&role_id=role",
    )
    def test_aws_uri(self, mock_hvac):
        test_hook = VaultHook(vault_conn_id="vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="https://vault.example.com")
        test_client.auth_aws_iam.assert_called_with(
            access_key="login",
            secret_key="pass",
            role="role",
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_azure_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "azure",
            "azure_tenant_id": "tenant_id",
            "azure_resource": "resource",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.azure.configure.assert_called_with(
            tenant_id="tenant_id",
            resource="resource",
            client_id="user",
            client_secret="pass",
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_azure_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "azure",
            "azure_tenant_id": "tenant_id",
            "azure_resource": "resource",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.azure.configure.assert_called_with(
            tenant_id="tenant_id",
            resource="resource",
            client_id="user",
            client_secret="pass",
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_gcp_init_params(self, mock_hvac, mock_get_connection, mock_get_credentials, mock_get_scopes):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_get_scopes.return_value = ["scope1", "scope2"]
        mock_get_credentials.return_value = ("credentials", "project_id")

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
            "auth_type": "gcp",
            "gcp_key_path": "path.json",
            "gcp_scopes": "scope1,scope2",
        }

        test_hook = VaultHook(**kwargs)
        test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path="path.json", keyfile_dict=None, scopes=["scope1", "scope2"]
        )
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.gcp.configure.assert_called_with(
            credentials="credentials",
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_gcp_dejson(self, mock_hvac, mock_get_connection, mock_get_credentials, mock_get_scopes):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_get_scopes.return_value = ["scope1", "scope2"]
        mock_get_credentials.return_value = ("credentials", "project_id")

        connection_dict = {
            "auth_type": "gcp",
            "gcp_key_path": "path.json",
            "gcp_scopes": "scope1,scope2",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path="path.json", keyfile_dict=None, scopes=["scope1", "scope2"]
        )
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.gcp.configure.assert_called_with(
            credentials="credentials",
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_gcp_dict_dejson(self, mock_hvac, mock_get_connection, mock_get_credentials, mock_get_scopes):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_get_scopes.return_value = ["scope1", "scope2"]
        mock_get_credentials.return_value = ("credentials", "project_id")

        connection_dict = {
            "auth_type": "gcp",
            "gcp_keyfile_dict": '{"key": "value"}',
            "gcp_scopes": "scope1,scope2",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path=None, keyfile_dict={"key": "value"}, scopes=["scope1", "scope2"]
        )
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.gcp.configure.assert_called_with(
            credentials="credentials",
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_github_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "github",
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.github.login.assert_called_with(token="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_github_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "github",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.github.login.assert_called_with(token="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes_default_path(self, mock_kubernetes, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "kubernetes",
            "kubernetes_role": "kube_role",
            "vault_conn_id": "vault_conn_id",
        }

        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            test_hook = VaultHook(**kwargs)
            test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_file.assert_called_with("/var/run/secrets/kubernetes.io/serviceaccount/token")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(role="kube_role", jwt="data")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes_init_params(self, mock_kubernetes, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "kubernetes_role": "kube_role",
            "kubernetes_jwt_path": "path",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "kubernetes",
            "vault_conn_id": "vault_conn_id",
        }
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            test_hook = VaultHook(**kwargs)
            test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(role="kube_role", jwt="data")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes_dejson(self, mock_kubernetes, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "kubernetes_role": "kube_role",
            "kubernetes_jwt_path": "path",
            "auth_type": "kubernetes",
            "vault_conn_id": "vault_conn_id",
        }
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            test_hook = VaultHook(**kwargs)
            test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(role="kube_role", jwt="data")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_client_kwargs(self, mock_hvac, mock_get_connection):
        """This test checks that values in connection extras keyed with 'client_kwargs' will be
        consumed by the underlying Hashicorp Vault client init. The order of precedence should
        be kwargs (passed through the hook init) > client_kwargs (found in connection extras).
        """
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "client_kwargs": {"namespace": "name", "timeout": 50, "generic_arg": "generic_val1"}
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "generic_arg": "generic_val0"}
        test_hook = VaultHook(**kwargs)
        test_client = test_hook.get_conn()
        mock_get_connection.assert_called_with("vault_conn_id")
        mock_hvac.Client.assert_called_with(
            url="http://localhost:8180", namespace="name", timeout=50, generic_arg="generic_val0"
        )
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_ldap_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "ldap",
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.ldap.login.assert_called_with(username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_ldap_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "ldap",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.ldap.login.assert_called_with(username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "radius",
            "radius_host": "radhost",
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.radius.configure.assert_called_with(host="radhost", secret="pass", port=None)
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_init_params_port(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "auth_type": "radius",
            "radius_host": "radhost",
            "radius_port": 8123,
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.radius.configure.assert_called_with(host="radhost", secret="pass", port=8123)
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "radius",
            "radius_host": "radhost",
            "radius_port": "8123",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth.radius.configure.assert_called_with(host="radhost", secret="pass", port=8123)
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_dejson_wrong_port(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "radius",
            "radius_host": "radhost",
            "radius_port": "wrong",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        with pytest.raises(VaultError, match="Radius port was wrong: wrong"):
            VaultHook(**kwargs)

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_token_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        connection_dict = {}
        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.is_authenticated.assert_called_with()
        assert "pass" == test_client.token
        assert 2 == test_hook.vault_client.kv_engine_version
        assert "secret" == test_hook.vault_client.mount_point

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_token_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "token",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.is_authenticated.assert_called_with()
        assert "pass" == test_client.token
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_userpass_init_params(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "userpass", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth_userpass.assert_called_with(username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_userpass_dejson(self, mock_hvac, mock_get_connection):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection

        connection_dict = {
            "auth_type": "userpass",
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {
            "vault_conn_id": "vault_conn_id",
        }

        test_hook = VaultHook(**kwargs)
        mock_get_connection.assert_called_with("vault_conn_id")
        test_client = test_hook.get_conn()
        mock_hvac.Client.assert_called_with(url="http://localhost:8180")
        test_client.auth_userpass.assert_called_with(username="user", password="pass")
        test_client.is_authenticated.assert_called_with()
        assert 2 == test_hook.vault_client.kv_engine_version

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "request_id": "94011e25-f8dc-ec29-221b-1f9c1d9ad2ae",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 0,
            "data": {
                "data": {"secret_key": "secret_value"},
                "metadata": {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 1,
                },
            },
            "wrap_info": None,
            "warnings": None,
            "auth": None,
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        secret = test_hook.get_secret(secret_path="missing")
        assert {"secret_key": "secret_value"} == secret
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="missing", version=None
        )

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v2_version(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "request_id": "94011e25-f8dc-ec29-221b-1f9c1d9ad2ae",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 0,
            "data": {
                "data": {"secret_key": "secret_value"},
                "metadata": {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 1,
                },
            },
            "wrap_info": None,
            "warnings": None,
            "auth": None,
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        secret = test_hook.get_secret(secret_path="missing", secret_version=1)
        assert {"secret_key": "secret_value"} == secret
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="missing", version=1
        )

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v1.read_secret.return_value = {
            "request_id": "182d0673-618c-9889-4cba-4e1f4cfe4b4b",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 2764800,
            "data": {"value": "world"},
            "wrap_info": None,
            "warnings": None,
            "auth": None,
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 1}

        test_hook = VaultHook(**kwargs)
        secret = test_hook.get_secret(secret_path="missing")
        assert {"value": "world"} == secret
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(mount_point="secret", path="missing")

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_secret_metadata_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_metadata.return_value = {
            "request_id": "94011e25-f8dc-ec29-221b-1f9c1d9ad2ae",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 0,
            "metadata": [
                {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 1,
                },
                {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 2,
                },
            ],
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        metadata = test_hook.get_secret_metadata(secret_path="missing")
        assert {
            "request_id": "94011e25-f8dc-ec29-221b-1f9c1d9ad2ae",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 0,
            "metadata": [
                {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 1,
                },
                {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 2,
                },
            ],
        } == metadata
        mock_client.secrets.kv.v2.read_secret_metadata.assert_called_once_with(
            mount_point="secret", path="missing"
        )

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_secret_including_metadata_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "request_id": "94011e25-f8dc-ec29-221b-1f9c1d9ad2ae",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 0,
            "data": {
                "data": {"secret_key": "secret_value"},
                "metadata": {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 1,
                },
            },
            "wrap_info": None,
            "warnings": None,
            "auth": None,
        }

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        metadata = test_hook.get_secret_including_metadata(secret_path="missing")
        assert {
            "request_id": "94011e25-f8dc-ec29-221b-1f9c1d9ad2ae",
            "lease_id": "",
            "renewable": False,
            "lease_duration": 0,
            "data": {
                "data": {"secret_key": "secret_value"},
                "metadata": {
                    "created_time": "2020-03-16T21:01:43.331126Z",
                    "deletion_time": "",
                    "destroyed": False,
                    "version": 1,
                },
            },
            "wrap_info": None,
            "warnings": None,
            "auth": None,
        } == metadata
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="missing", version=None
        )

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v2(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        test_hook.create_or_update_secret(secret_path="path", secret={"key": "value"})
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point="secret", secret_path="path", secret={"key": "value"}, cas=None
        )

    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v2_cas(self, mock_hvac, mock_get_connection):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 2}

        test_hook = VaultHook(**kwargs)
        test_hook.create_or_update_secret(secret_path="path", secret={"key": "value"}, cas=10)
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point="secret", secret_path="path", secret={"key": "value"}, cas=10
        )

    @pytest.mark.parametrize(
        "method, expected_method",
        [
            (None, None),
            ("post", "post"),
        ],
    )
    @mock.patch("airflow.providers.hashicorp.hooks.vault.VaultHook.get_connection")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v1(self, mock_hvac, mock_get_connection, method, expected_method):
        mock_connection = self.get_mock_connection()
        mock_get_connection.return_value = mock_connection
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        connection_dict = {}

        mock_connection.extra_dejson.get.side_effect = connection_dict.get
        kwargs = {"vault_conn_id": "vault_conn_id", "auth_type": "token", "kv_engine_version": 1}

        test_hook = VaultHook(**kwargs)
        test_hook.create_or_update_secret(secret_path="path", secret={"key": "value"}, method=method)
        mock_client.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            mount_point="secret", secret_path="path", secret={"key": "value"}, method=expected_method
        )
