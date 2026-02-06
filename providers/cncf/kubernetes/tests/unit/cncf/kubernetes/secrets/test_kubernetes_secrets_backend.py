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

from airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend import (
    KubernetesSecretsBackend,
)

MODULE_PATH = "airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend"


def _make_secret(data: dict[str, str]):
    """Create a mock V1Secret with base64-encoded data."""
    encoded = {k: base64.b64encode(v.encode("utf-8")).decode("utf-8") for k, v in data.items()}
    secret = mock.MagicMock()
    secret.data = encoded
    return secret


class TestKubernetesSecretsBackendConnections:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_conn_value_uri(self, mock_client, mock_namespace):
        """Test reading a connection URI from a Kubernetes secret."""
        uri = "postgresql://user:pass@host:5432/db"
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret({"value": uri})

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db")

        assert result == uri
        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-connections-my-db", "default"
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
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret({"value": conn_json})

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db")

        assert result == conn_json
        parsed = json.loads(result)
        assert parsed["conn_type"] == "postgres"

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_conn_value_not_found(self, mock_client, mock_namespace):
        """Test that a missing secret returns None."""
        mock_client.return_value.read_namespaced_secret.side_effect = ApiException(status=404)

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("nonexistent")

        assert result is None


class TestKubernetesSecretsBackendVariables:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_variable(self, mock_client, mock_namespace):
        """Test reading a variable from a Kubernetes secret."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret({"value": "my-value"})

        backend = KubernetesSecretsBackend()
        result = backend.get_variable("api_key")

        assert result == "my-value"
        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-variables-api-key", "default"
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_variable_not_found(self, mock_client, mock_namespace):
        """Test that a missing variable secret returns None."""
        mock_client.return_value.read_namespaced_secret.side_effect = ApiException(status=404)

        backend = KubernetesSecretsBackend()
        result = backend.get_variable("nonexistent")

        assert result is None


class TestKubernetesSecretsBackendConfig:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_config(self, mock_client, mock_namespace):
        """Test reading a config value from a Kubernetes secret."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret(
            {"value": "sqlite:///airflow.db"}
        )

        backend = KubernetesSecretsBackend()
        result = backend.get_config("sql_alchemy_conn")

        assert result == "sqlite:///airflow.db"
        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-config-sql-alchemy-conn", "default"
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_get_config_not_found(self, mock_client, mock_namespace):
        """Test that a missing config secret returns None."""
        mock_client.return_value.read_namespaced_secret.side_effect = ApiException(status=404)

        backend = KubernetesSecretsBackend()
        result = backend.get_config("nonexistent")

        assert result is None


class TestKubernetesSecretsBackendCustomConfig:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_custom_prefix(self, mock_client, mock_namespace):
        """Test using a custom prefix."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret(
            {"value": "postgresql://localhost/db"}
        )

        backend = KubernetesSecretsBackend(connections_prefix="my-prefix")
        result = backend.get_conn_value("my_db")

        assert result == "postgresql://localhost/db"
        mock_client.return_value.read_namespaced_secret.assert_called_once_with("my-prefix-my-db", "default")

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_custom_data_key(self, mock_client, mock_namespace):
        """Test using a custom data key for connections."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret(
            {"conn_uri": "postgresql://localhost/db"}
        )

        backend = KubernetesSecretsBackend(connections_data_key="conn_uri")
        result = backend.get_conn_value("my_db")

        assert result == "postgresql://localhost/db"

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_missing_data_key_returns_none(self, mock_client, mock_namespace):
        """Test that a secret with a missing data key returns None."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret(
            {"wrong_key": "some-value"}
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
        mock_client.return_value.read_namespaced_secret.return_value = secret

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db")

        assert result is None


class TestKubernetesSecretsBackendTeamName:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_team_name_does_not_affect_conn_lookup(self, mock_client, mock_namespace):
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret({"value": "uri://val"})

        backend = KubernetesSecretsBackend()
        result = backend.get_conn_value("my_db", team_name="my-team")

        assert result == "uri://val"
        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-connections-my-db", "default"
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_team_name_does_not_affect_variable_lookup(self, mock_client, mock_namespace):
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret({"value": "val"})

        backend = KubernetesSecretsBackend()
        result = backend.get_variable("my_key", team_name="my-team")

        assert result == "val"
        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-variables-my-key", "default"
        )


class TestKubernetesSecretsBackendNameSanitization:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_underscores_converted_to_hyphens(self, mock_client, mock_namespace):
        """Test that underscores in keys are converted to hyphens."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret(
            {"value": "postgresql://localhost/db"}
        )

        backend = KubernetesSecretsBackend()
        backend.get_conn_value("my_database_conn")

        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-connections-my-database-conn", "default"
        )

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_uppercase_converted_to_lowercase(self, mock_client, mock_namespace):
        """Test that uppercase characters are converted to lowercase."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret(
            {"value": "postgresql://localhost/db"}
        )

        backend = KubernetesSecretsBackend()
        backend.get_conn_value("MyDB")

        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-connections-mydb", "default"
        )

    @pytest.mark.parametrize(
        ("key", "expected_secret_name"),
        [
            pytest.param("my.dotted.key", "airflow-connections-my.dotted.key", id="dots-preserved"),
            pytest.param("MiXeD_Case_KEY", "airflow-connections-mixed-case-key", id="mixed-case"),
            pytest.param(
                "multiple___underscores", "airflow-connections-multiple---underscores", id="multi-underscore"
            ),
        ],
    )
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_special_character_keys(self, mock_client, mock_namespace, key, expected_secret_name):
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret({"value": "val"})

        backend = KubernetesSecretsBackend()
        backend.get_conn_value(key)

        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            expected_secret_name, "default"
        )


class TestKubernetesSecretsBackendPrefixNone:
    @mock.patch(f"{MODULE_PATH}._get_secret")
    def test_connections_prefix_none(self, mock_get_secret):
        """Test that setting connections_prefix to None skips connection lookups."""
        backend = KubernetesSecretsBackend(connections_prefix=None)
        result = backend.get_conn_value("my_db")

        assert result is None
        mock_get_secret.assert_not_called()

    @mock.patch(f"{MODULE_PATH}._get_secret")
    def test_variables_prefix_none(self, mock_get_secret):
        """Test that setting variables_prefix to None skips variable lookups."""
        backend = KubernetesSecretsBackend(variables_prefix=None)
        result = backend.get_variable("my_var")

        assert result is None
        mock_get_secret.assert_not_called()

    @mock.patch(f"{MODULE_PATH}._get_secret")
    def test_config_prefix_none(self, mock_get_secret):
        """Test that setting config_prefix to None skips config lookups."""
        backend = KubernetesSecretsBackend(config_prefix=None)
        result = backend.get_config("my_config")

        assert result is None
        mock_get_secret.assert_not_called()


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

        mock_path_cls.assert_called_once_with("/var/run/secrets/kubernetes.io/serviceaccount/namespace")

    @mock.patch("airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.Path")
    def test_namespace_falls_back_to_default(self, mock_path_cls):
        """Test that namespace falls back to 'default' when file is not found."""
        mock_path_cls.return_value.read_text.side_effect = FileNotFoundError

        backend = KubernetesSecretsBackend()
        assert backend.namespace == "default"

    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="airflow")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_namespace_used_in_api_calls(self, mock_client, mock_namespace):
        """Test that auto-detected namespace is used when reading secrets."""
        mock_client.return_value.read_namespaced_secret.return_value = _make_secret(
            {"value": "postgresql://localhost/db"}
        )

        backend = KubernetesSecretsBackend()
        backend.get_conn_value("my_db")

        mock_client.return_value.read_namespaced_secret.assert_called_once_with(
            "airflow-connections-my-db", "airflow"
        )


class TestKubernetesSecretsBackendApiErrors:
    @mock.patch(f"{MODULE_PATH}.namespace", new_callable=mock.PropertyMock, return_value="default")
    @mock.patch(f"{MODULE_PATH}.client", new_callable=mock.PropertyMock)
    def test_non_404_api_exception_is_raised(self, mock_client, mock_namespace):
        """Test that non-404 API exceptions are re-raised."""
        mock_client.return_value.read_namespaced_secret.side_effect = ApiException(status=403)

        backend = KubernetesSecretsBackend()

        with pytest.raises(ApiException) as exc_info:
            backend.get_conn_value("my_db")
        assert exc_info.value.status == 403
