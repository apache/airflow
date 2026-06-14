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

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import ProviderCollectionResponse, ProviderResponse
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import provider_command


class TestProviderCommands:
    parser = cli_parser.get_parser()
    provider = ProviderResponse(
        package_name="apache-airflow-providers-amazon",
        description="Amazon provider",
        version="9.0.0",
        documentation_url="https://example.com",
    )
    collection = ProviderCollectionResponse(providers=[provider], total_entries=1)

    def _client(self, api_client_maker):
        return api_client_maker(
            path="/api/v2/providers",
            response_json=self.collection.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

    def test_get(self, api_client_maker, capsys):
        provider_command.get(
            self.parser.parse_args(
                ["providers", "get", "apache-airflow-providers-amazon", "--output", "json"]
            ),
            api_client=self._client(api_client_maker),
        )
        assert json.loads(capsys.readouterr().out) == [
            {"package_name": "apache-airflow-providers-amazon", "version": "9.0.0"}
        ]

    def test_get_full(self, api_client_maker, capsys):
        provider_command.get(
            self.parser.parse_args(
                ["providers", "get", "apache-airflow-providers-amazon", "--full", "--output", "json"]
            ),
            api_client=self._client(api_client_maker),
        )
        assert json.loads(capsys.readouterr().out) == [self.provider.model_dump(mode="json")]

    def test_get_not_found(self, api_client_maker):
        with pytest.raises(SystemExit, match="No such provider installed: does-not-exist"):
            provider_command.get(
                self.parser.parse_args(["providers", "get", "does-not-exist"]),
                api_client=self._client(api_client_maker),
            )
