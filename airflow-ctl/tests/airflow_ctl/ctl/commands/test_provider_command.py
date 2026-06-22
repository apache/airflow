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
"""Tests for provider commands."""

from __future__ import annotations

import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import ProviderDetailsResponse
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import provider_command


class TestProviderCommands:
    parser = cli_parser.get_parser()
    provider_name = "apache-airflow-providers-standard"
    provider_info = {
        "package-name": provider_name,
        "name": "Standard",
        "description": "Standard provider",
        "versions": ["1.0.0"],
    }
    provider_response = ProviderDetailsResponse(
        package_name=provider_name,
        version="1.0.0",
        description="Standard provider",
        provider_info=provider_info,
    )

    @pytest.mark.parametrize(
        ("extra_args", "expected"),
        [
            ([], {"Provider": provider_name, "Version": "1.0.0"}),
            (["--full"], provider_info),
        ],
        ids=["summary", "full"],
    )
    def test_get_provider(self, api_client_maker, extra_args, expected):
        api_client = api_client_maker(
            path=f"/api/v2/providers/{self.provider_name}",
            response_json=self.provider_response.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        args = self.parser.parse_args(["providers", "get", self.provider_name, *extra_args])

        result = provider_command.get_provider(args, api_client=api_client)

        assert result == expected
