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

from airflow.cli import cli_parser
from airflow.cli.commands import config_command

from dev.tests_common.test_utils.config import conf_vars

STATSD_CONFIG_BEGIN_WITH = "# `StatsD <https://github.com/statsd/statsd>`"


class TestCliConfigList:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.config_command.StringIO")
    @mock.patch("airflow.cli.commands.config_command.conf")
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

    @mock.patch("airflow.cli.commands.config_command.StringIO")
    @mock.patch("airflow.cli.commands.config_command.conf")
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
        assert any(line.startswith("# task_runner = StandardTaskRunner") for line in lines if line)

    @conf_vars({("core", "task_runner"): "test-runner"})
    def test_cli_show_config_defaults_not_show_conf_changes(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--defaults"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("# task_runner = StandardTaskRunner") for line in lines if line)

    @mock.patch("os.environ", {"AIRFLOW__CORE__TASK_RUNNER": "test-env-runner"})
    def test_cli_show_config_defaults_do_not_show_env_changes(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(
                self.parser.parse_args(["config", "list", "--color", "off", "--defaults"])
            )
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("# task_runner = StandardTaskRunner") for line in lines if line)

    @conf_vars({("core", "task_runner"): "test-runner"})
    def test_cli_show_changed_defaults_when_overridden_in_conf(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("task_runner = test-runner") for line in lines if line)

    @mock.patch("os.environ", {"AIRFLOW__CORE__TASK_RUNNER": "test-env-runner"})
    def test_cli_show_changed_defaults_when_overridden_in_env(self):
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        output = temp_stdout.getvalue()
        lines = output.splitlines()
        assert any(line.startswith("task_runner = test-env-runner") for line in lines if line)

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

        assert "test_value" == temp_stdout.getvalue().strip()

    @mock.patch("airflow.cli.commands.config_command.conf")
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
