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

import os
from unittest.mock import patch

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import Config, ConfigOption, ConfigSection
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import config_command
from airflowctl.ctl.commands.config_command import ConfigChange, ConfigParameter


class TestCliConfigLint:
    parser = cli_parser.get_parser()

    @patch("rich.print")
    def test_lint_no_issues(self, mock_rich_print, api_client_maker):
        response_config = Config(
            sections=[
                ConfigSection(
                    name="test_section",
                    options=[
                        ConfigOption(
                            key="test_key",
                            value="test_value",
                        )
                    ],
                )
            ]
        )

        api_client = api_client_maker(
            path="/api/v2/config",
            response_json=response_config.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        config_command.lint(
            self.parser.parse_args(["config", "lint"]),
            api_client=api_client,
        )

        calls = [call[0][0] for call in mock_rich_print.call_args_list]
        assert "[green]No issues found in your airflow.cfg. It is ready for Airflow 3![/green]" in calls[0]

    @patch("airflowctl.api.client.Credentials.load")
    @patch("rich.print")
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_CONFIG"})
    @patch(
        "airflowctl.ctl.commands.config_command.CONFIGS_CHANGES",
        [
            ConfigChange(
                config=ConfigParameter("test_section", "test_option"),
                default_change=True,
                old_default="old_default",
                new_default="new_default",
            ),
        ],
    )
    def test_lint_detects_default_changed_configs(self, mock_rich_print, api_client_maker):
        test_section_key = "test_section"
        test_option_key = "test_option"
        old_default_value = "old_default"
        new_default_value = "new_default"

        response_config = Config(
            sections=[
                ConfigSection(
                    name=test_section_key,
                    options=[
                        ConfigOption(
                            key=test_option_key,
                            value=old_default_value,
                        )
                    ],
                )
            ]
        )

        api_client = api_client_maker(
            path="/api/v2/config",
            response_json=response_config.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        api_client.configs.list.return_value = response_config

        config_command.lint(
            self.parser.parse_args(["config", "lint"]),
            api_client=api_client,
        )

        calls = [call[0][0] for call in mock_rich_print.call_args_list]
        assert "[red]Found issues in your airflow.cfg:[/red]" in calls[0]
        assert (
            f"  - [yellow]Changed default value of `{test_option_key}` in `{test_section_key}` from `{old_default_value}` to `{new_default_value}`.[/yellow]"
            in calls[1]
        )

    @patch("airflowctl.api.client.Credentials.load")
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_CONFIG"})
    @patch("rich.print")
    @patch(
        "airflowctl.ctl.commands.config_command.CONFIGS_CHANGES",
        [
            ConfigChange(
                config=ConfigParameter("test_section", "test_option"),
                was_removed=True,
            ),
        ],
    )
    def test_lint_detects_removed_configs(self, mock_rich_print, api_client_maker):
        response_config = Config(
            sections=[
                ConfigSection(
                    name="test_section",
                    options=[
                        ConfigOption(
                            key="test_option",
                            value="test_value",
                        )
                    ],
                )
            ]
        )

        api_client = api_client_maker(
            path="/api/v2/config",
            response_json=response_config.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        api_client.configs.list.return_value = response_config

        config_command.lint(
            self.parser.parse_args(["config", "lint"]),
            api_client=api_client,
        )

        calls = [call[0][0] for call in mock_rich_print.call_args_list]
        assert "[red]Found issues in your airflow.cfg:[/red]" in calls[0]
        assert (
            "- [yellow]Removed deprecated `test_option` configuration parameter from `test_section` section.[/yellow]"
            in calls[1]
        )

    @patch("airflowctl.api.client.Credentials.load")
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_CONFIG"})
    @patch("rich.print")
    @patch(
        "airflowctl.ctl.commands.config_command.CONFIGS_CHANGES",
        [
            ConfigChange(
                config=ConfigParameter("test_section_1", "test_option"),
                renamed_to=ConfigParameter("test_section_2", "test_option"),
            ),
        ],
    )
    def test_lint_detects_renamed_configs_different_section(self, mock_rich_print, api_client_maker):
        response_config = Config(
            sections=[
                ConfigSection(
                    name="test_section_1",
                    options=[
                        ConfigOption(
                            key="test_option",
                            value="test_value",
                        )
                    ],
                ),
                ConfigSection(
                    name="test_section_2",
                    options=[
                        ConfigOption(
                            key="test_option",
                            value="test_value",
                        )
                    ],
                ),
            ]
        )
        api_client = api_client_maker(
            path="/api/v2/config",
            response_json=response_config.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        api_client.configs.list.return_value = response_config

        config_command.lint(
            self.parser.parse_args(["config", "lint"]),
            api_client=api_client,
        )

        calls = [call[0][0] for call in mock_rich_print.call_args_list]
        assert "[red]Found issues in your airflow.cfg:[/red]" in calls[0]
        assert (
            "- [yellow]`test_option` configuration parameter moved from `test_section_1` section to `test_section_2` section as `test_option`.[/yellow]"
            in calls[1]
        )

    @patch("airflowctl.api.client.Credentials.load")
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_CONFIG"})
    @patch("rich.print")
    @patch(
        "airflowctl.ctl.commands.config_command.CONFIGS_CHANGES",
        [
            ConfigChange(
                config=ConfigParameter("test_section", "test_option_1"),
                renamed_to=ConfigParameter("test_section", "test_option_2"),
            ),
        ],
    )
    def test_lint_detects_renamed_configs_same_section(self, mock_rich_print, api_client_maker):
        response_config = Config(
            sections=[
                ConfigSection(
                    name="test_section",
                    options=[
                        ConfigOption(
                            key="test_option_1",
                            value="test_value_1",
                        ),
                        ConfigOption(
                            key="test_option_2",
                            value="test_value_2",
                        ),
                    ],
                )
            ]
        )

        api_client = api_client_maker(
            path="/api/v2/config",
            response_json=response_config.model_dump(),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        api_client.configs.list.return_value = response_config

        config_command.lint(
            self.parser.parse_args(["config", "lint"]),
            api_client=api_client,
        )

        calls = [call[0][0] for call in mock_rich_print.call_args_list]
        assert "[red]Found issues in your airflow.cfg:[/red]" in calls[0]
        assert (
            "- [yellow]`test_option_1` configuration parameter renamed to `test_option_2` in the `test_section` section.[/yellow]"
            in calls[1]
        )
