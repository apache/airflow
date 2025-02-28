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
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from airflow.cli.cli_definition_loader import CliDefinitionLoader
from airflow.exceptions import (
    AirflowLoadCliDefinitionsException,
    AirflowLoadProviderCliDefinitionException,
)
from airflow.providers_manager import ProvidersManager

KUBERNETES_PROVIDER_NAME = "apache-airflow-providers-cncf-kubernetes"


class TestCliDefinitionLoader:
    @mock.patch("airflow.providers.cncf.kubernetes.cli.definition.KUBERNETES_GROUP_COMMANDS", autospec=True)
    def test__load_provider_cli_definitions(self, mock_provider_cli_definitions):
        mock_provider_cli_definitions = [MagicMock()]  # Ensure it's a list
        with patch.object(
            ProvidersManager, "providers", new_callable=PropertyMock
        ) as mock_provider_manager_providers:
            mock_provider_manager_providers.return_value = {KUBERNETES_PROVIDER_NAME: MagicMock()}
            with patch("airflow.cli.cli_definition_loader.import_string") as mock_import_string:
                mock_import_string.return_value = mock_provider_cli_definitions
                assert CliDefinitionLoader._load_provider_cli_definitions() == (
                    mock_provider_cli_definitions,
                    [],
                )
                mock_import_string.assert_called_once_with(
                    "airflow.providers.cncf.kubernetes.cli.definition.KUBERNETES_GROUP_COMMANDS"
                )

    @mock.patch.object(ProvidersManager, "providers", new_callable=PropertyMock)
    def test__load_provider_cli_definitions_with_error(self, mock_provider_manager_providers):
        mock_provider_manager_providers.return_value = {KUBERNETES_PROVIDER_NAME: MagicMock()}
        with patch("airflow.cli.cli_definition_loader.import_string") as mock_import_string:
            mock_import_string.side_effect = ImportError
            loaded_cli, errors = CliDefinitionLoader._load_provider_cli_definitions()
            assert loaded_cli == []
            assert len(errors) == 1
            assert str(errors[0]) == str(
                AirflowLoadProviderCliDefinitionException(
                    f"Failed to load CLI commands from provider: {KUBERNETES_PROVIDER_NAME}"
                )
            )

    def test_get_cli_commands(self):
        mock_command = MagicMock()
        with mock.patch.object(
            CliDefinitionLoader, "_load_provider_cli_definitions", return_value=([mock_command], [])
        ):
            commands = list(CliDefinitionLoader.get_cli_commands())
            assert len(commands) == 1
            assert commands[0] is mock_command

    def test_get_cli_commands_with_error(self):
        expected_provider_exception = AirflowLoadProviderCliDefinitionException("Test error")
        with mock.patch.object(
            CliDefinitionLoader,
            "_load_provider_cli_definitions",
            return_value=([], [expected_provider_exception]),
        ):
            with pytest.raises(AirflowLoadCliDefinitionsException) as excinfo:
                list(CliDefinitionLoader.get_cli_commands())
            assert expected_provider_exception in excinfo.value.args[0]
