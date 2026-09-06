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

from airflow.cli import cli_parser
from airflow.cli.commands import provider_command
from airflow.providers_manager import ProviderInfo

RST_DESCRIPTION = "`Apache Beam <https://beam.apache.org/>`__.\n"
PLAIN_DESCRIPTION = "Apache Beam https://beam.apache.org/"


class TestCliProviderGet:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @pytest.fixture
    def provider_info(self):
        return ProviderInfo(
            version="1.0.0",
            data={"package-name": "apache-airflow-providers-beam", "description": RST_DESCRIPTION},
        )

    @mock.patch("airflow.cli.commands.provider_command.AirflowConsole", autospec=True)
    @mock.patch("airflow.cli.commands.provider_command.ProvidersManager", autospec=True)
    def test_provider_get_full_keeps_providers_manager_data_intact(
        self, mock_providers_manager, mock_console, provider_info
    ):
        mock_providers_manager.return_value.providers = {
            "apache-airflow-providers-beam": provider_info,
        }

        provider_command.provider_get(
            self.parser.parse_args(["providers", "get", "apache-airflow-providers-beam", "--full"])
        )

        assert mock_console.return_value.print_as.call_args.kwargs["data"] == [
            {"package-name": "apache-airflow-providers-beam", "description": PLAIN_DESCRIPTION}
        ]
        assert provider_info.data["description"] == RST_DESCRIPTION
