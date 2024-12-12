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
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands.remote_commands import config_command

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
    from airflow.cli.commands.remote_commands.config_command import CONFIGS_CHANGES

    @pytest.mark.parametrize("removed_config", CONFIGS_CHANGES)
    def test_lint_detects_removed_configs(self, removed_config):
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()
            assert (
                f"Removed deprecated `{removed_config.option}` configuration parameter from `{removed_config.section}` section."
                in output
            )

            if removed_config.suggestion:
                assert removed_config.suggestion in output

    @pytest.mark.parametrize(
        "section, option, suggestion",
        [
            (
                "core",
                "check_slas",
                "The SLA feature is removed in Airflow 3.0, to be replaced with Airflow Alerts in Airflow 3.1",
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
            assert (
                f"Removed deprecated `{option}` configuration parameter from `{section}` section." in output
            )

            if suggestion:
                assert suggestion in output

    def test_lint_specific_section_option(self):
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(
                    cli_parser.get_parser().parse_args(
                        ["config", "lint", "--section", "core", "--option", "check_slas"]
                    )
                )

            output = temp_stdout.getvalue()
            assert "Removed deprecated `check_slas` configuration parameter from `core` section." in output

    def test_lint_with_invalid_section_option(self):
        with mock.patch("airflow.configuration.conf.has_option", return_value=False):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(
                    cli_parser.get_parser().parse_args(
                        ["config", "lint", "--section", "invalid_section", "--option", "invalid_option"]
                    )
                )

            output = temp_stdout.getvalue()
            assert "No issues found in your airflow.cfg." in output

    def test_lint_detects_multiple_issues(self):
        with mock.patch(
            "airflow.configuration.conf.has_option",
            side_effect=lambda s, o: o in ["check_slas", "strict_asset_uri_validation"],
        ):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()
            assert "Removed deprecated `check_slas` configuration parameter from `core` section." in output
            assert (
                "Removed deprecated `strict_asset_uri_validation` configuration parameter from `core` section."
                in output
            )

    def test_lint_detects_renamed_configs(self):
        renamed_config = config_command.CONFIGS_CHANGES[0]
        with mock.patch("airflow.configuration.conf.has_option", return_value=True):
            with contextlib.redirect_stdout(StringIO()) as temp_stdout:
                config_command.lint_config(cli_parser.get_parser().parse_args(["config", "lint"]))

            output = temp_stdout.getvalue()
            if renamed_config.renamed_to:
                new_section, new_option = renamed_config.renamed_to
                assert (
                    f"Removed deprecated `{renamed_config.option}` configuration parameter from `{renamed_config.section}` section."
                    in output
                )
                assert f"Please use `{new_option}` from section `{new_section}` instead." in output
