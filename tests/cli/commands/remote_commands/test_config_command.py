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
import os
import re
from io import StringIO
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands.remote_commands import config_command
from airflow.cli.commands.remote_commands.config_command import ConfigChange, ConfigParameter

from tests_common.test_utils.config import conf_vars

STATSD_CONFIG_BEGIN_WITH = "# `StatsD <https://github.com/statsd/statsd>`"


class TestCliConfigList:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.remote_commands.config_command.StringIO")
    @mock.patch("airflow.cli.commands.remote_commands.config_command.conf")
    def test_cli_show_config_should_write_data(self, mock_conf, mock_stringio):
        config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        mock_conf.write.assert_called_once_with(
            mock_stringio.return_value.__enter__.return_value,
            section=None,
            include_examples=False,
            include_descriptions=False,
            include_sources=False,
            include_env_vars=False,
            include_providers=True,
            comment_out_everything=False,
            only_defaults=False,
        )

    @mock.patch("airflow.cli.commands.remote_commands.config_command.StringIO")
    @mock.patch("airflow.cli.commands.remote_commands.config_command.conf")
    def test_cli_show_config_should_write_data_specific_section(self, mock_conf, mock_stringio):
        config_command.show_config(
            self.parser.parse_args(["config", "list", "--section", "core", "--color", "off"])
        )
        mock_conf.write.assert_called_once_with(
            mock_stringio.return_value.__enter__.return_value,
            section="core",
            include_examples=False,
            include_descriptions=False,
            include_sources=False,
            include_env_vars=False,
            include_providers=True,
            comment_out_everything=False,
            only_defaults=False,
        )

    @conf_vars({("core", "testkey"): "test_value"})
    def test_cli_show_config_should_display_key(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        output = temp_stdout.getvalue()
        assert "[core]" in output
        assert "testkey = test_value" in temp_stdout.getvalue()

    def test_cli_show_config_should_only_show_comments_when_no_defaults(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert all(not line.startswith("#") or line.endswith("= ") for line in lines if line)

    def test_cli_show_config_shows_descriptions(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--include-descriptions"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        # comes from metrics description
        assert all(not line.startswith("# Source: ") for line in lines if line)
        assert any(line.startswith(STATSD_CONFIG_BEGIN_WITH) for line in lines if line)
        assert all(not line.startswith("# Example:") for line in lines if line)
        assert all(not line.startswith("# Variable:") for line in lines if line)

    def test_cli_show_config_shows_examples(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--include-examples"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert all(not line.startswith("# Source: ") for line in lines if line)
        assert all(not line.startswith(STATSD_CONFIG_BEGIN_WITH) for line in lines if line)
        assert any(line.startswith("# Example:") for line in lines if line)
        assert all(not line.startswith("# Variable:") for line in lines if line)

    def test_cli_show_config_shows_variables(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--include-env-vars"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert all(not line.startswith("# Source: ") for line in lines if line)
        assert all(not line.startswith(STATSD_CONFIG_BEGIN_WITH) for line in lines if line)
        assert all(not line.startswith("# Example:") for line in lines if line)
        assert any(line.startswith("# Variable:") for line in lines if line)

    def test_cli_show_config_shows_sources(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--include-sources"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("# Source: ") for line in lines if line)
        assert all(not line.startswith(STATSD_CONFIG_BEGIN_WITH) for line in lines if line)
        assert all(not line.startswith("# Example:") for line in lines if line)
        assert all(not line.startswith("# Variable:") for line in lines if line)

    def test_cli_show_config_defaults(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--defaults"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert all(not line.startswith("# Source: ") for line in lines if line)
        assert any(line.startswith(STATSD_CONFIG_BEGIN_WITH) for line in lines if line)
        assert any(not line.startswith("# Example:") for line in lines if line)
        assert any(not line.startswith("# Example:") for line in lines if line)
        assert any(line.startswith("# Variable:") for line in lines if line)
        assert any(
            line.startswith("# hostname_callable = airflow.utils.net.getfqdn") for line in lines if line
        )

    @conf_vars({("core", "hostname_callable"): "testfn"})
    def test_cli_show_config_defaults_not_show_conf_changes(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--defaults"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(
            line.startswith("# hostname_callable = airflow.utils.net.getfqdn") for line in lines if line
        )

    @mock.patch("os.environ", {"AIRFLOW__CORE__HOSTNAME_CALLABLE": "test_env"})
    def test_cli_show_config_defaults_do_not_show_env_changes(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--defaults"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(
            line.startswith("# hostname_callable = airflow.utils.net.getfqdn") for line in lines if line
        )

    @conf_vars({("core", "hostname_callable"): "testfn"})
    def test_cli_show_changed_defaults_when_overridden_in_conf(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("hostname_callable = testfn") for line in lines if line)

    @mock.patch("os.environ", {"AIRFLOW__CORE__HOSTNAME_CALLABLE": "test_env"})
    def test_cli_show_changed_defaults_when_overridden_in_env(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("hostname_callable = test_env") for line in lines if line)

    def test_cli_has_providers(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("celery_config_options") for line in lines if line)

    def test_cli_comment_out_everything(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--comment-out-everything"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert all(not line.strip() or line.startswith(("#", "[")) for line in lines if line)


class TestCliConfigGetValue:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @conf_vars({("core", "test_key"): "test_value"})
    def test_should_display_value(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.get_value(self.parser.parse_args(["config", "get-value", "core", "test_key"]))

        assert temp_stdout.getvalue().strip() == "test_value"

    @mock.patch("airflow.cli.commands.remote_commands.config_command.conf")
    def test_should_not_raise_exception_when_section_for_config_with_value_defined_elsewhere_is_missing(
        self, mock_conf
    ):
        # no section in config
        mock_conf.has_section.return_value = False
        # pretend that the option is defined by other means
        mock_conf.has_option.return_value = True

        config_command.get_value(self.parser.parse_args(["config", "get-value", "some_section", "value"]))

    def test_should_raise_exception_when_option_is_missing(self, caplog):
        config_command.get_value(
            self.parser.parse_args(["config", "get-value", "missing-section", "dags_folder"])
        )
        assert "section/key [missing-section/dags_folder] not found in config" in caplog.text


class TestConfigLint:
    @pytest.mark.parametrize("removed_config", config_command.CONFIGS_CHANGES)
    def test_lint_detects_removed_configs(self, removed_config):
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())
        normalized_message = re.sub(r"\s+", " ", removed_config.message.strip())

        assert normalized_message in normalized_output

    @pytest.mark.parametrize(
        "section, option, suggestion",
        [
            (
                "core",
                "check_slas",
                "The SLA feature is removed in Airflow 3.0, to be replaced with Airflow Alerts in future",
            ),
            (
                "core",
                "strict_asset_uri_validation",
                "Asset URI with a defined scheme will now always be validated strictly, raising a hard error on validation failure.",
            ),
            (
                "logging",
                "enable_task_context_logger",
                "Remove TaskContextLogger: Replaced by the Log table for better handling of task log messages outside the execution context.",
            ),
        ],
    )
    def test_lint_with_specific_removed_configs(self, section, option, suggestion):
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())

        expected_message = f"Removed deprecated `{option}` configuration parameter from `{section}` section."
        assert expected_message in normalized_output

        assert suggestion in normalized_output

    def test_lint_specific_section_option(self):
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(
                    cli_parser.get_parser().parse_args(
                        ["config", "lint", "--section", "core", "--option", "check_slas"]
                    )
                )

            output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())

        assert (
            "Removed deprecated `check_slas` configuration parameter from `core` section."
            in normalized_output
        )

    def test_lint_with_invalid_section_option(self):
        with mock.patch("airflow.configuration.conf.has_option", return_value=False):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(
                    cli_parser.get_parser().parse_args(
                        ["config", "lint", "--section", "invalid_section", "--option", "invalid_option"]
                    )
                )

            output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())

        assert "No issues found in your airflow.cfg." in normalized_output

    def test_lint_detects_multiple_issues(self):
        with mock.patch(
            "airflow.configuration.conf.has_option",
            side_effect=lambda s, o: o in ["check_slas", "strict_asset_uri_validation"],
        ):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())

        assert (
            "Removed deprecated `check_slas` configuration parameter from `core` section."
            in normalized_output
        )
        assert (
            "Removed deprecated `strict_asset_uri_validation` configuration parameter from `core` section."
            in normalized_output
        )

    @pytest.mark.parametrize(
        "removed_configs",
        [
            [
                (
                    "core",
                    "check_slas",
                    "The SLA feature is removed in Airflow 3.0, to be replaced with Airflow Alerts in future",
                ),
                (
                    "core",
                    "strict_asset_uri_validation",
                    "Asset URI with a defined scheme will now always be validated strictly, raising a hard error on validation failure.",
                ),
                (
                    "logging",
                    "enable_task_context_logger",
                    "Remove TaskContextLogger: Replaced by the Log table for better handling of task log messages outside the execution context.",
                ),
            ],
        ],
    )
    def test_lint_detects_multiple_removed_configs(self, removed_configs):
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())

        for section, option, suggestion in removed_configs:
            expected_message = (
                f"Removed deprecated `{option}` configuration parameter from `{section}` section."
            )
            assert expected_message in normalized_output

            if suggestion:
                assert suggestion in normalized_output

    @pytest.mark.parametrize(
        "renamed_configs",
        [
            # Case 1: Renamed configurations within the same section
            [
                ("core", "non_pooled_task_slot_count", "core", "default_pool_task_slot_count"),
                ("scheduler", "processor_poll_interval", "scheduler", "scheduler_idle_sleep_time"),
            ],
            # Case 2: Renamed configurations across sections
            [
                ("admin", "hide_sensitive_variable_fields", "core", "hide_sensitive_var_conn_fields"),
                ("core", "worker_precheck", "celery", "worker_precheck"),
            ],
        ],
    )
    def test_lint_detects_renamed_configs(self, renamed_configs):
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())

        for old_section, old_option, new_section, new_option in renamed_configs:
            if old_section == new_section:
                expected_message = f"`{old_option}` configuration parameter renamed to `{new_option}` in the `{old_section}` section."
            else:
                expected_message = f"`{old_option}` configuration parameter moved from `{old_section}` section to `{new_section}` section as `{new_option}`."
            assert expected_message in normalized_output

    @pytest.mark.parametrize(
        "env_var, config_change, expected_message",
        [
            (
                "AIRFLOW__CORE__CHECK_SLAS",
                ConfigChange(
                    config=ConfigParameter("core", "check_slas"),
                    suggestion="The SLA feature is removed in Airflow 3.0, to be replaced with Airflow Alerts in future",
                ),
                "Removed deprecated `check_slas` configuration parameter from `core` section.",
            ),
            (
                "AIRFLOW__CORE__STRICT_ASSET_URI_VALIDATION",
                ConfigChange(
                    config=ConfigParameter("core", "strict_asset_uri_validation"),
                    suggestion="Asset URI with a defined scheme will now always be validated strictly, raising a hard error on validation failure.",
                ),
                "Removed deprecated `strict_asset_uri_validation` configuration parameter from `core` section.",
            ),
        ],
    )
    def test_lint_detects_configs_with_env_vars(self, env_var, config_change, expected_message):
        with mock.patch.dict(os.environ, {env_var: "some_value"}):
            with mock.patch("airflow.configuration.conf.has_option", return_value=True):
                with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                    config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

                output = temp_stdout.getvalue()

        normalized_output = re.sub(r"\s+", " ", output.strip())

        assert expected_message in normalized_output
        assert config_change.suggestion in normalized_output
