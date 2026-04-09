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
from unittest import mock

import pytest
from kubernetes.client.exceptions import ApiException

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend import (
    KubernetesSecretsBackend,
)

MODULE_PATH = "airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend"


def _make_secret(data: dict[str, str], name: str = "some-secret"):
    """Create a mock V1Secret with base64-encoded data."""
    encoded = {k: base64.b64encode(v.encode("utf-8")).decode("utf-8") for k, v in data.items()}
    secret = mock.MagicMock()
    secret.data = encoded
    secret.metadata.name = name
    return secret


def _make_secret_list(secrets: list):
    """Create a mock V1SecretList with the given items."""
    secret_list = mock.MagicMock()
    secret_list.items = secrets
    return secret_list


class TestKubernetesSecretsBackendConnections:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_conn_value_uri(self, mock_client, mock_namespace):
        """Test reading a connection URI from a Kubernetes secret."""
        uri = "postgresql://user:pass@host:5432/db"
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"value": uri})]
        )

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db")

        assert result == uri
        mock_client.return_value.list_namespaced_secret.assert_called_once_with(
            "default",
            label_selector="airflow.apache.org/connection-id=my_db",
            resource_version="0",
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_conn_value_json(self, mock_client, mock_namespace):
        """Test reading a JSON-formatted connection from a Kubernetes secret."""
        conn_json = json.dumps(
            {
                "conn_type": "postgres",
                "login": "user",
                "password": "pass",
                "host": "host",
                "port": 5432,
                "schema": "db",
            }
        )
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"value": conn_json})]
        )

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db")

        assert result == conn_json
        parsed = json.loads(result)
        assert parsed["conn_type"] == "postgres"

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_conn_value_not_found(self, mock_client, mock_namespace):
        """Test that a missing secret returns None."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list([])

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("nonexistent")

        assert result is None


class TestKubernetesSecretsBackendVariables:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_variable(self, mock_client, mock_namespace):
        """Test reading a variable from a Kubernetes secret."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"value": "my-value"})]
        )

        backend = KubernetesSecretsBackend()
        result = backend.get_variable("api_key")

        assert result == "my-value"
        mock_client.return_value.list_namespaced_secret.assert_called_once_with(
            "default",
            label_selector="airflow.apache.org/variable-key=api_key",
            resource_version="0",
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_variable_not_found(self, mock_client, mock_namespace):
        """Test that a missing variable secret returns None."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list([])

        backend = KubernetesSecretsBackend()
        result = backend.get_variable("nonexistent")

        assert result is None


class TestKubernetesSecretsBackendConfig:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_config(self, mock_client, mock_namespace):
        """Test reading a config value from a Kubernetes secret."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"value": "sqlite:///airflow.db"})]
        )

        backend = KubernetesSecretsBackend()
        result = backend.get_config("sql_alchemy_conn")

        assert result == "sqlite:///airflow.db"
        mock_client.return_value.list_namespaced_secret.assert_called_once_with(
            "default",
            label_selector="airflow.apache.org/config-key=sql_alchemy_conn",
            resource_version="0",
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_config_not_found(self, mock_client, mock_namespace):
        """Test that a missing config secret returns None."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list([])

        backend = KubernetesSecretsBackend()
        result = backend.get_config("nonexistent")

        assert result is None


class TestKubernetesSecretsBackendCustomConfig:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_custom_label(self, mock_client, mock_namespace):
        """Test using a custom label key."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"value": "postgresql://localhost/db"})]
        )

        backend = KubernetesSecretsBackend(connections_label="my-org/conn")
        result = backend.get_conn_value("my_db")

        assert result == "postgresql://localhost/db"
        mock_client.return_value.list_namespaced_secret.assert_called_once_with(
            "default",
            label_selector="my-org/conn=my_db",
            resource_version="0",
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_custom_data_key(self, mock_client, mock_namespace):
        """Test using a custom data key for connections."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"conn_uri": "postgresql://localhost/db"})]
        )

        backend = KubernetesSecretsBackend(connections_data_key="conn_uri")
        result = backend.get_conn_value("my_db")

        assert result == "postgresql://localhost/db"

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_missing_value_data_key_returns_none(self, mock_client, mock_namespace):
        """Test that a secret without the 'value' data key returns None."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"wrong_key": "some-value"})]
        )

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db")

        assert result is None

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_secret_with_none_data_returns_none(self, mock_client, mock_namespace):
        """Test that a secret with None data returns None."""
        secret = mock.MagicMock()
        secret.data = None
        secret.metadata.name = "some-secret"
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list([secret])

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db")

        assert result is None


class TestKubernetesSecretsBackendLabelNone:
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_connections_label_none(self, mock_client):
        """Test that setting connections_label to None skips connection lookups."""
        backend = KubernetesSecretsBackend(connections_label=None)
        result = backend.get_conn_value("my_db")

        assert result is None
        mock_client.return_value.list_namespaced_secret.assert_not_called()

    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_variables_label_none(self, mock_client):
        """Test that setting variables_label to None skips variable lookups."""
        backend = KubernetesSecretsBackend(variables_label=None)
        result = backend.get_variable("my_var")

        assert result is None
        mock_client.return_value.list_namespaced_secret.assert_not_called()

    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_config_label_none(self, mock_client):
        """Test that setting config_label to None skips config lookups."""
        backend = KubernetesSecretsBackend(config_label=None)
        result = backend.get_config("my_config")

        assert result is None
        mock_client.return_value.list_namespaced_secret.assert_not_called()


class TestKubernetesSecretsBackendMultipleMatches:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_multiple_secrets_uses_first_and_warns(self, mock_client, mock_namespace, caplog):
        """Test that multiple matching secrets uses the first and logs a warning."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [
                _make_secret({"value": "first-value"}, name="secret-1"),
                _make_secret({"value": "second-value"}, name="secret-2"),
            ]
        )

        backend = KubernetesSecretsBackend()
        import logging

        with caplog.at_level(logging.WARNING):
            result = backend.get_conn_value("my_db")

        assert result == "first-value"
        assert "Multiple secrets found" in caplog.text


class TestKubernetesSecretsBackendClientInit:
    @mock.patch("airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.CoreV1Api")
    @mock.patch("airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.ApiClient")
    @mock.patch("airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.load_incluster_config")
    def test_client_uses_incluster_config(self, mock_load_incluster, mock_api_client, mock_core_v1):
        """Test that the client is obtained via load_incluster_config directly."""
        backend = KubernetesSecretsBackend()
        result = backend.client

        mock_load_incluster.assert_called_once()
        mock_api_client.assert_called_once()
        mock_core_v1.assert_called_once_with(mock_api_client.return_value)
        assert result is mock_core_v1.return_value


class TestKubernetesSecretsBackendNamespace:
    @mock.patch("airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.Path")
    def test_namespace_auto_detected(self, mock_path_cls):
        """Test that namespace is auto-detected from the service account metadata."""
        mock_path_cls.return_value.read_text.return_value = "airflow\n"

        backend = KubernetesSecretsBackend()
        assert backend.namespace == "airflow"

        mock_path_cls.assert_called_once_with(KubernetesSecretsBackend.SERVICE_ACCOUNT_NAMESPACE_PATH)

    @mock.patch("airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.Path")
    def test_namespace_raises_when_not_found(self, mock_path_cls):
        """Test that namespace raises AirflowException when file is not found."""
        mock_path_cls.return_value.read_text.side_effect = FileNotFoundError

        backend = KubernetesSecretsBackend()
        with pytest.raises(
            AirflowException, match="Could not auto-detect Kubernetes namespace.*automountServiceAccountToken"
        ):
            _ = backend.namespace

    def test_namespace_explicit(self):
        """Test that an explicitly passed namespace is used without reading the file."""
        backend = KubernetesSecretsBackend(namespace="my-ns")
        assert backend.namespace == "my-ns"

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="airflow")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_namespace_used_in_api_calls(self, mock_client, mock_namespace):
        """Test that auto-detected namespace is used when listing secrets."""
        mock_client.return_value.list_namespaced_secret.return_value = _make_secret_list(
            [_make_secret({"value": "postgresql://localhost/db"})]
        )

        backend = KubernetesSecretsBackend()
        backend.get_conn_value("my_db")

        mock_client.return_value.list_namespaced_secret.assert_called_once_with(
            "airflow",
            label_selector="airflow.apache.org/connection-id=my_db",
            resource_version="0",
        )


class TestKubernetesSecretsBackendApiErrors:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_api_exception_is_raised(self, mock_client, mock_namespace):
        """Test that API exceptions are re-raised."""
        mock_client.return_value.list_namespaced_secret.side_effect = ApiException(status=403)

        backend = KubernetesSecretsBackend()

        with pytest.raises(ApiException) as exc_info:
            backend.get_conn_value("my_db")
        assert exc_info.value.status == 403
