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

import json
from unittest import mock
from unittest.mock import patch

import pytest

from airflow.sdk import Variable
from airflow.sdk.configuration import initialize_secrets_backends
from airflow.sdk.execution_time.comms import PutVariable, VariableResult
from airflow.sdk.execution_time.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

from tests_common.test_utils.config import conf_vars


class TestVariables:
    @pytest.mark.parametrize(
        ("deserialize_json", "value", "expected_value"),
        [
            pytest.param(
                False,
                "my_value",
                "my_value",
                id="simple-value",
            ),
            pytest.param(
                True,
                '{"key": "value", "number": 42, "flag": true}',
                {"key": "value", "number": 42, "flag": True},
                id="deser-object-value",
            ),
        ],
    )
    def test_var_get(self, deserialize_json, value, expected_value, mock_supervisor_comms):
        var_result = VariableResult(key="my_key", value=value)
        mock_supervisor_comms.send.return_value = var_result

        var = Variable.get(key="my_key", deserialize_json=deserialize_json)
        assert var is not None
        assert var == expected_value

    @pytest.mark.parametrize(
        ("key", "value", "description", "serialize_json"),
        [
            pytest.param(
                "key",
                "value",
                "description",
                False,
                id="simple-value",
            ),
            pytest.param(
                "key2",
                {"hi": "there", "hello": 42, "flag": True},
                "description2",
                True,
                id="serialize-json-value",
            ),
        ],
    )
    def test_var_set(self, key, value, description, serialize_json, mock_supervisor_comms):
        Variable.set(key=key, value=value, description=description, serialize_json=serialize_json)

        expected_value = value
        if serialize_json:
            expected_value = json.dumps(value, indent=2)

        mock_supervisor_comms.send.assert_called_once_with(
            msg=PutVariable(
                key=key, value=expected_value, description=description, serialize_json=serialize_json
            ),
        )


class TestVariableFromSecrets:
    def test_var_get_from_secrets_found(self, mock_supervisor_comms, tmp_path):
        """Tests getting a variable from secrets backend."""
        path = tmp_path / "var.env"
        path.write_text("VAR_A=some_value")

        with conf_vars(
            {
                (
                    "workers",
                    "secrets_backend",
                ): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
                ("workers", "secrets_backend_kwargs"): f'{{"variables_file_path": "{path}"}}',
            }
        ):
            retrieved_var = Variable.get(key="VAR_A")
            assert retrieved_var is not None
            assert retrieved_var == "some_value"

    def test_var_get_from_secrets_found_with_deserialize(self, mock_supervisor_comms, tmp_path):
        """Tests getting a variable from secrets backend when deserialize_json is provided."""
        path = tmp_path / "var.json"
        dict_data = {"num1": 23, "num2": 42}
        jsonified_dict_data = json.dumps(dict_data)
        data = {"VAR_A": jsonified_dict_data}
        path.write_text(json.dumps(data, indent=4))

        with conf_vars(
            {
                (
                    "workers",
                    "secrets_backend",
                ): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
                ("workers", "secrets_backend_kwargs"): f'{{"variables_file_path": "{path}"}}',
            }
        ):
            retrieved_var = Variable.get(key="VAR_A")
            assert retrieved_var == jsonified_dict_data

            retrieved_var_deser = Variable.get(key="VAR_A", deserialize_json=True)
            assert retrieved_var_deser == dict_data

    @patch("airflow.sdk.execution_time.context.mask_secret")
    def test_var_get_from_secrets_sensitive_key(self, mock_mask_secret, mock_supervisor_comms, tmp_path):
        """Tests getting a variable from secrets backend when deserialize_json is provided."""
        path = tmp_path / "var.json"
        data = {"secret": "super-secret"}
        path.write_text(json.dumps(data, indent=4))

        with conf_vars(
            {
                (
                    "workers",
                    "secrets_backend",
                ): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
                ("workers", "secrets_backend_kwargs"): f'{{"variables_file_path": "{path}"}}',
            }
        ):
            retrieved_var = Variable.get(key="secret")
            assert retrieved_var == "super-secret"

            mock_mask_secret.assert_called_with("super-secret", "secret")

    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_get_variable_env_var(self, mock_env_get, mock_supervisor_comms):
        """Tests getting a variable from environment variable."""
        mock_env_get.return_value = "fake_value"
        Variable.get(key="fake_var_key")
        mock_env_get.assert_called_once_with(key="fake_var_key")

    @conf_vars(
        {
            ("workers", "secrets_backend"): "airflow.secrets.local_filesystem.LocalFilesystemBackend",
            ("workers", "secrets_backend_kwargs"): '{"variables_file_path": "/files/var.json"}',
        }
    )
    @mock.patch(
        "airflow.secrets.local_filesystem.LocalFilesystemBackend.get_variable",
    )
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_backend_fallback_to_env_var(self, mock_get_variable, mock_env_get, mock_supervisor_comms):
        """Tests if variable retrieval falls back to environment variable backend if not found in secrets backend."""
        mock_get_variable.return_value = None
        mock_env_get.return_value = "fake_value"

        backends = initialize_secrets_backends(DEFAULT_SECRETS_SEARCH_PATH_WORKERS)
        # LocalFilesystemBackend (custom), EnvironmentVariablesBackend, ExecutionAPISecretsBackend
        assert len(backends) == 3
        backend_classes = [backend.__class__.__name__ for backend in backends]
        assert "LocalFilesystemBackend" in backend_classes
        assert "ExecutionAPISecretsBackend" in backend_classes

        var = Variable.get(key="fake_var_key")
        # mock_env is only called when LocalFilesystemBackend doesn't have it
        mock_env_get.assert_called()
        assert var == "fake_value"
