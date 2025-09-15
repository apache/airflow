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

import json
import time
from unittest import mock
from unittest.mock import mock_open, patch

import pytest
from hvac.exceptions import InvalidPath, VaultError
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from airflow.providers.hashicorp._internal_client.vault_client import _VaultClient


class TestVaultClient:
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_version_wrong(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="The version is not supported: 4"):
            _VaultClient(auth_type="approle", kv_engine_version=4)

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_custom_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="userpass", mount_point="custom")
        assert vault_client.mount_point == "custom"

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_version_one_init(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(auth_type="userpass", kv_engine_version=1)
        assert vault_client.kv_engine_version == 1

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_default_session_retry(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="approle",
            role_id="role",
            url="http://localhost:8180",
            secret_id="pass",
        )
        _ = vault_client.client

        default_session = vault_client.kwargs["session"]
        assert isinstance(default_session, Session)
        adapter = default_session.get_adapter(url="http://localhost:8180")
        assert isinstance(adapter, HTTPAdapter)
        max_retries = adapter.max_retries
        assert isinstance(max_retries, Retry)
        assert (max_retries.total if max_retries.total else 0) > 1
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=default_session)

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_approle(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="approle", role_id="role", url="http://localhost:8180", secret_id="pass", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.approle.login.assert_called_with(role_id="role", secret_id="pass")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_approle_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="approle",
            role_id="role",
            url="http://localhost:8180",
            secret_id="pass",
            auth_mount_point="other",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.approle.login.assert_called_with(role_id="role", secret_id="pass", mount_point="other")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_approle_missing_role(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="requires 'role_id'"):
            _VaultClient(auth_type="approle", url="http://localhost:8180", secret_id="pass")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_aws_iam(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="aws_iam",
            role_id="role",
            url="http://localhost:8180",
            key_id="user",
            secret_id="pass",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.aws.iam_login.assert_called_with(
            access_key="user",
            secret_key="pass",
            role="role",
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_aws_iam_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="aws_iam",
            role_id="role",
            url="http://localhost:8180",
            key_id="user",
            secret_id="pass",
            auth_mount_point="other",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.aws.iam_login.assert_called_with(
            access_key="user", secret_key="pass", role="role", mount_point="other"
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_aws_iam_different_region(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="aws_iam",
            role_id="role",
            url="http://localhost:8180",
            key_id="user",
            secret_id="pass",
            session=None,
            region="us-east-2",
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.aws.iam_login.assert_called_with(
            access_key="user",
            secret_key="pass",
            role="role",
            region="us-east-2",
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_azure(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="azure",
            azure_tenant_id="tenant_id",
            azure_resource="resource",
            url="http://localhost:8180",
            key_id="user",
            secret_id="pass",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.azure.configure.assert_called_with(
            tenant_id="tenant_id",
            resource="resource",
            client_id="user",
            client_secret="pass",
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_azure_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="azure",
            azure_tenant_id="tenant_id",
            azure_resource="resource",
            url="http://localhost:8180",
            key_id="user",
            secret_id="pass",
            auth_mount_point="other",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.azure.configure.assert_called_with(
            tenant_id="tenant_id",
            resource="resource",
            client_id="user",
            client_secret="pass",
            mount_point="other",
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_azure_missing_resource(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="requires 'azure_resource'"):
            _VaultClient(
                auth_type="azure",
                azure_tenant_id="tenant_id",
                url="http://localhost:8180",
                key_id="user",
                secret_id="pass",
            )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_azure_missing_tenant_id(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="requires 'azure_tenant_id'"):
            _VaultClient(
                auth_type="azure",
                azure_resource="resource",
                url="http://localhost:8180",
                key_id="user",
                secret_id="pass",
            )

    @mock.patch("builtins.open", create=True)
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac.Client")
    @mock.patch("googleapiclient.discovery.build")
    def test_gcp(self, mock_google_build, mock_hvac_client, mock_get_credentials, mock_get_scopes, mock_open):
        # Mock the content of the file 'path.json'
        mock_file = mock.MagicMock()
        mock_file.read.return_value = '{"client_email": "service_account_email"}'
        mock_open.return_value.__enter__.return_value = mock_file

        mock_client = mock.MagicMock()
        mock_hvac_client.return_value = mock_client
        mock_get_scopes.return_value = ["scope1", "scope2"]
        mock_get_credentials.return_value = ("credentials", "project_id")

        # Mock the current time to use for iat and exp
        current_time = int(time.time())
        iat = current_time
        exp = iat + 3600  # 1 hour after iat

        # Mock the signJwt API to return the expected payload
        mock_sign_jwt = (
            mock_google_build.return_value.projects.return_value.serviceAccounts.return_value.signJwt
        )
        mock_sign_jwt.return_value.execute.return_value = {"signedJwt": "mocked_jwt"}

        vault_client = _VaultClient(
            auth_type="gcp",
            gcp_key_path="path.json",
            gcp_scopes="scope1,scope2",
            role_id="role",
            url="http://localhost:8180",
            session=None,
        )

        # Preserve the original json.dumps
        original_json_dumps = json.dumps

        # Inject the mocked payload into the JWT signing process
        with mock.patch("json.dumps") as mock_json_dumps:

            def mocked_json_dumps(payload):
                # Override the payload to inject controlled iat and exp values
                payload["iat"] = iat
                payload["exp"] = exp
                return original_json_dumps(payload)  # Use the original json.dumps

            mock_json_dumps.side_effect = mocked_json_dumps

            client = vault_client.client  # Trigger the Vault client creation

        # Validate that the HVAC client and other mocks are called correctly
        mock_hvac_client.assert_called_with(url="http://localhost:8180", session=None)
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path="path.json", keyfile_dict=None, scopes=["scope1", "scope2"]
        )

        # Extract the arguments passed to the mocked signJwt API
        args, kwargs = mock_sign_jwt.call_args
        payload = json.loads(kwargs["body"]["payload"])

        # Assert iat and exp values are as expected
        assert payload["iat"] == iat
        assert payload["exp"] == exp
        assert abs(payload["exp"] - (payload["iat"] + 3600)) < 10  # Validate exp is 3600 seconds after iat

        client.auth.gcp.login.assert_called_with(role="role", jwt="mocked_jwt")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("builtins.open", create=True)
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac.Client")
    @mock.patch("googleapiclient.discovery.build")
    def test_gcp_different_auth_mount_point(
        self, mock_google_build, mock_hvac_client, mock_get_credentials, mock_get_scopes, mock_open
    ):
        # Mock the content of the file 'path.json'
        mock_file = mock.MagicMock()
        mock_file.read.return_value = '{"client_email": "service_account_email"}'
        mock_open.return_value.__enter__.return_value = mock_file

        mock_client = mock.MagicMock()
        mock_hvac_client.return_value = mock_client
        mock_get_scopes.return_value = ["scope1", "scope2"]
        mock_get_credentials.return_value = ("credentials", "project_id")

        mock_sign_jwt = (
            mock_google_build.return_value.projects.return_value.serviceAccounts.return_value.signJwt
        )
        mock_sign_jwt.return_value.execute.return_value = {"signedJwt": "mocked_jwt"}

        # Generate realistic iat and exp values
        current_time = int(time.time())
        iat = current_time
        exp = current_time + 3600  # 1 hour later

        vault_client = _VaultClient(
            auth_type="gcp",
            gcp_key_path="path.json",
            gcp_scopes="scope1,scope2",
            role_id="role",
            url="http://localhost:8180",
            auth_mount_point="other",
            session=None,
        )

        # Preserve the original json.dumps
        original_json_dumps = json.dumps

        # Inject the mocked payload into the JWT signing process
        with mock.patch("json.dumps") as mock_json_dumps:

            def mocked_json_dumps(payload):
                # Override the payload to inject controlled iat and exp values
                payload["iat"] = iat
                payload["exp"] = exp
                return original_json_dumps(payload)  # Use the original json.dumps

            mock_json_dumps.side_effect = mocked_json_dumps

            client = vault_client.client  # Trigger the Vault client creation

        # Assertions
        mock_hvac_client.assert_called_with(url="http://localhost:8180", session=None)
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path="path.json", keyfile_dict=None, scopes=["scope1", "scope2"]
        )
        # Extract the arguments passed to the mocked signJwt API
        args, kwargs = mock_sign_jwt.call_args
        payload = json.loads(kwargs["body"]["payload"])

        # Assert iat and exp values are as expected
        assert payload["iat"] == iat
        assert payload["exp"] == exp
        assert abs(payload["exp"] - (payload["iat"] + 3600)) < 10  # Validate exp is 3600 seconds after iat

        client.auth.gcp.login.assert_called_with(role="role", jwt="mocked_jwt", mount_point="other")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch(
        "builtins.open", new_callable=mock_open, read_data='{"client_email": "service_account_email"}'
    )
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._get_scopes")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac.Client")
    @mock.patch("googleapiclient.discovery.build")
    def test_gcp_dict(
        self, mock_google_build, mock_hvac_client, mock_get_credentials, mock_get_scopes, mock_file
    ):
        mock_client = mock.MagicMock()
        mock_hvac_client.return_value = mock_client
        mock_get_scopes.return_value = ["scope1", "scope2"]
        mock_get_credentials.return_value = ("credentials", "project_id")

        mock_sign_jwt = (
            mock_google_build.return_value.projects.return_value.serviceAccounts.return_value.signJwt
        )
        mock_sign_jwt.return_value.execute.return_value = {"signedJwt": "mocked_jwt"}

        # Generate realistic iat and exp values
        current_time = int(time.time())
        iat = current_time
        exp = current_time + 3600  # 1 hour later

        vault_client = _VaultClient(
            auth_type="gcp",
            gcp_keyfile_dict={"client_email": "service_account_email"},
            gcp_scopes="scope1,scope2",
            role_id="role",
            url="http://localhost:8180",
            session=None,
        )

        # Preserve the original json.dumps
        original_json_dumps = json.dumps

        # Inject the mocked payload into the JWT signing process
        with mock.patch("json.dumps") as mock_json_dumps:

            def mocked_json_dumps(payload):
                # Override the payload to inject controlled iat and exp values
                payload["iat"] = iat
                payload["exp"] = exp
                return original_json_dumps(payload)  # Use the original json.dumps

            mock_json_dumps.side_effect = mocked_json_dumps

            client = vault_client.client  # Trigger the Vault client creation

        # Assertions
        mock_hvac_client.assert_called_with(url="http://localhost:8180", session=None)
        mock_get_scopes.assert_called_with("scope1,scope2")
        mock_get_credentials.assert_called_with(
            key_path=None, keyfile_dict={"client_email": "service_account_email"}, scopes=["scope1", "scope2"]
        )
        # Extract the arguments passed to the mocked signJwt API
        args, kwargs = mock_sign_jwt.call_args
        payload = json.loads(kwargs["body"]["payload"])

        # Assert iat and exp values are as expected
        assert payload["iat"] == iat
        assert payload["exp"] == exp
        assert abs(payload["exp"] - (payload["iat"] + 3600)) < 10  # Validate exp is 3600 seconds after iat

        client.auth.gcp.login.assert_called_with(role="role", jwt="mocked_jwt")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_github(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="github", token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", url="http://localhost:8180", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.github.login.assert_called_with(token="s.7AU0I51yv1Q1lxOIg1F3ZRAS")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_github_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="github",
            token="s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            url="http://localhost:8180",
            auth_mount_point="other",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.github.login.assert_called_with(token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", mount_point="other")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_github_missing_token(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="'github' authentication type requires 'token'"):
            _VaultClient(auth_type="github", url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes_default_path(self, mock_kubernetes, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="kubernetes", kubernetes_role="kube_role", url="http://localhost:8180", session=None
        )
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            client = vault_client.client
        mock_file.assert_called_with("/var/run/secrets/kubernetes.io/serviceaccount/token")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(role="kube_role", jwt="data")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes(self, mock_kubernetes, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="kubernetes",
            kubernetes_role="kube_role",
            kubernetes_jwt_path="path",
            url="http://localhost:8180",
            session=None,
        )
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            client = vault_client.client
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(role="kube_role", jwt="data")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes_different_auth_mount_point(self, mock_kubernetes, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="kubernetes",
            kubernetes_role="kube_role",
            kubernetes_jwt_path="path",
            auth_mount_point="other",
            url="http://localhost:8180",
            session=None,
        )
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            client = vault_client.client
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(
            role="kube_role", jwt="data", mount_point="other"
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_kubernetes_missing_role(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="requires 'kubernetes_role'"):
            _VaultClient(auth_type="kubernetes", kubernetes_jwt_path="path", url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_kubernetes_kubernetes_jwt_path_none(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="requires 'kubernetes_jwt_path'"):
            _VaultClient(
                auth_type="kubernetes",
                kubernetes_role="kube_role",
                kubernetes_jwt_path=None,
                url="http://localhost:8180",
            )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes_with_audience(self, mock_kubernetes, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="kubernetes",
            kubernetes_role="kube_role",
            kubernetes_jwt_path="path",
            kubernetes_audience="test-audience",
            url="http://localhost:8180",
            session=None,
        )
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            client = vault_client.client
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(
            role="kube_role", jwt="data", audience="test-audience"
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.Kubernetes")
    def test_kubernetes_with_audience_and_mount_point(self, mock_kubernetes, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="kubernetes",
            kubernetes_role="kube_role",
            kubernetes_jwt_path="path",
            kubernetes_audience="test-audience",
            auth_mount_point="other",
            url="http://localhost:8180",
            session=None,
        )
        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            client = vault_client.client
        mock_file.assert_called_with("path")
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        mock_kubernetes.assert_called_with(mock_client.adapter)
        mock_kubernetes.return_value.login.assert_called_with(
            role="kube_role", jwt="data", audience="test-audience", mount_point="other"
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_ldap(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="ldap", username="user", password="pass", url="http://localhost:8180", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.ldap.login.assert_called_with(username="user", password="pass")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_ldap_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="ldap",
            username="user",
            password="pass",
            auth_mount_point="other",
            url="http://localhost:8180",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.ldap.login.assert_called_with(username="user", password="pass", mount_point="other")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_missing_host(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="radius_host"):
            _VaultClient(auth_type="radius", radius_secret="pass", url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_missing_secret(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="radius_secret"):
            _VaultClient(auth_type="radius", radius_host="radhost", url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_secret="pass",
            url="http://localhost:8180",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.radius.configure.assert_called_with(host="radhost", secret="pass", port=None)
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_secret="pass",
            auth_mount_point="other",
            url="http://localhost:8180",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.radius.configure.assert_called_with(
            host="radhost", secret="pass", port=None, mount_point="other"
        )
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_radius_port(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.radius.configure.assert_called_with(host="radhost", secret="pass", port=8110)
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_token_missing_token(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with pytest.raises(VaultError, match="'token' authentication type requires 'token'"):
            _VaultClient(auth_type="token", url="http://localhost:8180")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_token(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="token", token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", url="http://localhost:8180", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.is_authenticated.assert_called_with()
        assert client.token == "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        assert vault_client.kv_engine_version == 2
        assert vault_client.mount_point == "secret"

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_token_in_env(self, mock_hvac, monkeypatch):
        monkeypatch.setenv("VAULT_TOKEN", "s.7AU0I51yv1Q1lxOIg1F3ZRAS")
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(auth_type="token", url="http://localhost:8180", session=None)
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.is_authenticated.assert_called_with()
        assert client.token == "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        assert vault_client.kv_engine_version == 2
        assert vault_client.mount_point == "secret"

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_token_path(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with open("/tmp/test_token.txt", "w+") as the_file:
            the_file.write("s.7AU0I51yv1Q1lxOIg1F3ZRAS")
        vault_client = _VaultClient(
            auth_type="token", token_path="/tmp/test_token.txt", url="http://localhost:8180", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.is_authenticated.assert_called_with()
        assert client.token == "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        assert vault_client.kv_engine_version == 2
        assert vault_client.mount_point == "secret"

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_token_path_strip(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        with open("/tmp/test_token.txt", "w+") as the_file:
            the_file.write("  s.7AU0I51yv1Q1lxOIg1F3ZRAS\n")
        vault_client = _VaultClient(
            auth_type="token", token_path="/tmp/test_token.txt", url="http://localhost:8180", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.is_authenticated.assert_called_with()
        assert client.token == "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        assert vault_client.kv_engine_version == 2
        assert vault_client.mount_point == "secret"

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_default_auth_type(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", url="http://localhost:8180", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.is_authenticated.assert_called_with()
        assert client.token == "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        assert vault_client.auth_type == "token"
        assert vault_client.kv_engine_version == 2
        assert vault_client.mount_point == "secret"

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_userpass(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="userpass", username="user", password="pass", url="http://localhost:8180", session=None
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.userpass.login.assert_called_with(username="user", password="pass")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_userpass_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="userpass",
            username="user",
            password="pass",
            auth_mount_point="other",
            url="http://localhost:8180",
            session=None,
        )
        client = vault_client.client
        mock_hvac.Client.assert_called_with(url="http://localhost:8180", session=None)
        client.auth.userpass.login.assert_called_with(username="user", password="pass", mount_point="other")
        client.is_authenticated.assert_called_with()
        assert vault_client.kv_engine_version == 2

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_non_existing_key_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()
        vault_client = _VaultClient(
            auth_type="token", token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", url="http://localhost:8180"
        )
        secret = vault_client.get_secret(secret_path="missing")
        assert secret is None
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="missing", version=None, raise_on_deleted_version=True
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_non_existing_key_v2_different_auth(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()
        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
        )
        secret = vault_client.get_secret(secret_path="missing")
        assert secret is None
        assert vault_client.mount_point == "secret"
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="missing", version=None, raise_on_deleted_version=True
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_non_existing_key_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v1.read_secret.side_effect = InvalidPath()
        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )
        secret = vault_client.get_secret(secret_path="missing")
        assert secret is None
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(mount_point="secret", path="missing")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
        )
        secret = vault_client.get_secret(secret_path="path/to/secret")
        assert secret == {"secret_key": "secret_value"}
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="path/to/secret", version=None, raise_on_deleted_version=True
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v2_without_preconfigured_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
            mount_point=None,
        )
        secret = vault_client.get_secret(secret_path="mount_point/path/to/secret")
        assert secret == {"secret_key": "secret_value"}
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="mount_point", path="path/to/secret", version=None, raise_on_deleted_version=True
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v2_version(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
        )
        secret = vault_client.get_secret(secret_path="missing", secret_version=1)
        assert secret == {"secret_key": "secret_value"}
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="missing", version=1, raise_on_deleted_version=True
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )
        secret = vault_client.get_secret(secret_path="/path/to/secret")
        assert secret == {"value": "world"}
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point="secret", path="/path/to/secret"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1_ssl_verify_false(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
            verify=False,
        )
        secret = vault_client.get_secret(secret_path="/path/to/secret")
        assert secret == {"value": "world"}
        assert not vault_client.kwargs["session"].verify
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point="secret", path="/path/to/secret"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1_trust_private_ca(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
            verify="/etc/ssl/certificates/ca-bundle.pem",
        )
        secret = vault_client.get_secret(secret_path="/path/to/secret")
        assert secret == {"value": "world"}
        assert vault_client.kwargs["session"].verify == "/etc/ssl/certificates/ca-bundle.pem"
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point="secret", path="/path/to/secret"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1_with_proxies_applied(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
            verify=False,
            proxies={
                "http": "http://10.10.1.10:3128",
                "https": "http://10.10.1.10:1080",
            },
        )
        secret = vault_client.get_secret(secret_path="/path/to/secret")
        assert secret == {"value": "world"}
        assert vault_client.kwargs["session"].proxies["http"] == "http://10.10.1.10:3128"
        assert vault_client.kwargs["session"].proxies["https"] == "http://10.10.1.10:1080"
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point="secret", path="/path/to/secret"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1_with_client_cert_applied(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
            verify=False,
            cert=("/path/client.cert", "/path/client.key"),
        )
        secret = vault_client.get_secret(secret_path="/path/to/secret")
        assert secret == {"value": "world"}
        assert vault_client.kwargs["session"].cert == ("/path/client.cert", "/path/client.key")
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point="secret", path="/path/to/secret"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1_without_preconfigured_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
            mount_point=None,
        )
        secret = vault_client.get_secret(secret_path="mount_point/path/to/secret")
        assert secret == {"value": "world"}
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point="mount_point", path="path/to/secret"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1_different_auth_mount_point(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            auth_mount_point="other",
            url="http://localhost:8180",
        )
        secret = vault_client.get_secret(secret_path="missing")
        assert secret == {"value": "world"}
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(mount_point="secret", path="missing")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_existing_key_v1_version(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        vault_client = _VaultClient(
            auth_type="token",
            token="s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            url="http://localhost:8180",
            kv_engine_version=1,
        )
        with pytest.raises(VaultError, match="Secret version"):
            vault_client.get_secret(secret_path="missing", secret_version=1)

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_secret_metadata_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
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
        vault_client = _VaultClient(
            auth_type="token", token="s.7AU0I51yv1Q1lxOIg1F3ZRAS", url="http://localhost:8180"
        )
        metadata = vault_client.get_secret_metadata(secret_path="missing")
        assert metadata == {
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
        mock_client.secrets.kv.v2.read_secret_metadata.assert_called_once_with(
            mount_point="secret", path="missing"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_secret_metadata_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )
        with pytest.raises(VaultError, match="Metadata might only be used with version 2 of the KV engine."):
            vault_client.get_secret_metadata(secret_path="missing")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_secret_including_metadata_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

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
        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
        )
        metadata = vault_client.get_secret_including_metadata(secret_path="missing")
        assert metadata == {
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
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret", path="missing", version=None, raise_on_deleted_version=True
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_get_secret_including_metadata_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )
        with pytest.raises(VaultError, match="Metadata might only be used with version 2 of the KV engine."):
            vault_client.get_secret_including_metadata(secret_path="missing")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v2(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
        )
        vault_client.create_or_update_secret(secret_path="path", secret={"key": "value"})
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point="secret", path="path", secret={"key": "value"}, cas=None
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v2_method(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
        )
        with pytest.raises(VaultError, match="The method parameter is only valid for version 1"):
            vault_client.create_or_update_secret(secret_path="path", secret={"key": "value"}, method="post")

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v2_cas(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            url="http://localhost:8180",
        )
        vault_client.create_or_update_secret(secret_path="path", secret={"key": "value"}, cas=10)
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point="secret", path="path", secret={"key": "value"}, cas=10
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )
        vault_client.create_or_update_secret(secret_path="path", secret={"key": "value"})
        mock_client.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            mount_point="secret", path="path", secret={"key": "value"}, method=None
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v1_cas(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )
        with pytest.raises(VaultError, match="The cas parameter is only valid for version 2"):
            vault_client.create_or_update_secret(secret_path="path", secret={"key": "value"}, cas=10)

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_create_or_update_secret_v1_post(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )
        vault_client.create_or_update_secret(secret_path="path", secret={"key": "value"}, method="post")
        mock_client.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            mount_point="secret", path="path", secret={"key": "value"}, method="post"
        )

    @mock.patch("airflow.providers.hashicorp._internal_client.vault_client.hvac")
    def test_cached_property_invalidates_on_auth_failure(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        vault_client = _VaultClient(
            auth_type="radius",
            radius_host="radhost",
            radius_port=8110,
            radius_secret="pass",
            kv_engine_version=1,
            url="http://localhost:8180",
        )

        # Assert that the original mock_client is returned
        assert vault_client.client == mock_client

        # Prove that the mock_client is cached by changing the return
        # value, but still receive the original mock client
        mock_client_2 = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client_2
        assert vault_client.client == mock_client
        mock_client.is_authenticated.return_value = False
        # assert that when the client is not authenticated the cache
        # is invalidated, therefore returning the second client
        assert vault_client.client == mock_client_2
