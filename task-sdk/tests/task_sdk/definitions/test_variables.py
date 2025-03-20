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

from unittest import mock

import pytest

from airflow.configuration import initialize_secrets_backends
from airflow.sdk import Variable
from airflow.sdk.execution_time.comms import VariableResult
from airflow.sdk.execution_time.supervisor import initialize_secrets_backend_on_workers
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

from tests_common.test_utils.config import conf_vars


class TestVariables:
    @pytest.mark.parametrize(
        "deserialize_json, value, expected_value",
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
        mock_supervisor_comms.get_message.return_value = var_result

        var = Variable.get(key="my_key", deserialize_json=deserialize_json)
        assert var is not None
        assert var == expected_value


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
            initialize_secrets_backend_on_workers()
            retrieved_var = Variable.get(key="VAR_A")
            assert retrieved_var is not None
            assert retrieved_var == "some_value"

    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_get_variable_env_var(self, mock_env_get, mock_supervisor_comms):
        """Tests getting a variable from environment variable."""
        initialize_secrets_backend_on_workers()
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
        initialize_secrets_backend_on_workers()
        mock_get_variable.return_value = None
        mock_env_get.return_value = "fake_value"

        backends = initialize_secrets_backends(DEFAULT_SECRETS_SEARCH_PATH_WORKERS)
        assert len(backends) == 2
        backend_classes = [backend.__class__.__name__ for backend in backends]
        assert "LocalFilesystemBackend" in backend_classes

        var = Variable.get(key="fake_var_key")
        # mock_env is only called when LocalFilesystemBackend doesn't have it
        mock_env_get.assert_called()
        assert var == "fake_value"
