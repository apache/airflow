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

import pytest
from airflowctl.api.datamodels.generated import ProviderResponse

from airflow.cli import cli_parser
from airflow.cli.commands import provider_command


class TestCliProviderGet:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @staticmethod
    def _providers() -> list[ProviderResponse]:
        return [
            ProviderResponse(
                package_name="apache-airflow-providers-amazon",
                description="Amazon provider",
                version="9.0.0",
                documentation_url="https://example.com",
            )
        ]

    def test_provider_get(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.providers.list.return_value.providers = self._providers()
        with stdout_capture as stdout:
            provider_command.provider_get(
                self.parser.parse_args(
                    ["providers", "get", "apache-airflow-providers-amazon", "--output", "json"]
                )
            )
        assert json.loads(stdout.getvalue()) == [
            {"Provider": "apache-airflow-providers-amazon", "Version": "9.0.0"}
        ]

    def test_provider_get_full(self, mock_cli_api_client, stdout_capture):
        mock_cli_api_client.providers.list.return_value.providers = self._providers()
        with stdout_capture as stdout:
            provider_command.provider_get(
                self.parser.parse_args(
                    ["providers", "get", "apache-airflow-providers-amazon", "--full", "--output", "json"]
                )
            )
        assert json.loads(stdout.getvalue()) == [
            {
                "package_name": "apache-airflow-providers-amazon",
                "version": "9.0.0",
                "description": "Amazon provider",
                "documentation_url": "https://example.com",
            }
        ]

    def test_provider_get_not_found(self, mock_cli_api_client):
        mock_cli_api_client.providers.list.return_value.providers = self._providers()
        with pytest.raises(SystemExit, match="No such provider installed: does-not-exist"):
            provider_command.provider_get(self.parser.parse_args(["providers", "get", "does-not-exist"]))
