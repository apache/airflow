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

import contextlib
from io import StringIO

from airflowctl.api.client import ClientKind
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import config_command


class TestCliConfigGetValue:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    def test_should_display_value(self, api_client_maker):
        api_client = api_client_maker(
            path="/api/v2/config/section/core/option/test_key",
            response_json={
                "sections": [{"name": "core", "options": [{"key": "test_key", "value": "test_value"}]}]
            },
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        args = self.parser.parse_args(["config", "get-value", "--section", "core", "--option", "test_key"])

        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.get_value(args, api_client=api_client)

        assert temp_stdout.getvalue().strip() == "test_value"

    def test_should_handle_missing_section(self, api_client_maker):
        api_client = api_client_maker(
            path="/api/v2/config/section/missing_section/option/test_key",
            response_json={"detail": "Option [missing_section/dags_folder] not found."},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )

        args = self.parser.parse_args(
            ["config", "get-value", "--section", "missing_section", "--option", "test_key"]
        )

        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.get_value(args, api_client=api_client)

        output = temp_stdout.getvalue()
        assert "Server error" in output
        assert "Option [missing_section/dags_folder] not found." in output

    def test_should_handle_missing_option(self, api_client_maker):
        api_client = api_client_maker(
            path="/api/v2/config/section/core/option/missing_option",
            response_json={"detail": "Option [core/missing_option] not found."},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )

        args = self.parser.parse_args(
            ["config", "get-value", "--section", "core", "--option", "missing_option"]
        )

        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.get_value(args, api_client=api_client)

        output = temp_stdout.getvalue()
        assert "Server error" in output
        assert "Option [core/missing_option] not found." in output
