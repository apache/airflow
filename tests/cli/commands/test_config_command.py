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
import io
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import config_command
from tests.test_utils.config import conf_vars


class TestCliConfigList:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.config_command.io.StringIO")
    @mock.patch("airflow.cli.commands.config_command.conf")
    def test_cli_show_config_should_write_data(self, mock_conf, mock_stringio):
        config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        mock_conf.write.assert_called_once_with(
            mock_stringio.return_value.__enter__.return_value, section=None
        )

    @mock.patch("airflow.cli.commands.config_command.io.StringIO")
    @mock.patch("airflow.cli.commands.config_command.conf")
    def test_cli_show_config_should_write_data_specific_section(self, mock_conf, mock_stringio):
        config_command.show_config(
            self.parser.parse_args(["config", "list", "--section", "core", "--color", "off"])
        )
        mock_conf.write.assert_called_once_with(
            mock_stringio.return_value.__enter__.return_value, section="core"
        )

    @conf_vars({("core", "testkey"): "test_value"})
    def test_cli_show_config_should_display_key(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(["config", "list", "--color", "off"]))
        assert "[core]" in temp_stdout.getvalue()
        assert "testkey = test_value" in temp_stdout.getvalue()


class TestCliConfigGetValue:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @conf_vars({("core", "test_key"): "test_value"})
    def test_should_display_value(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            config_command.get_value(self.parser.parse_args(["config", "get-value", "core", "test_key"]))

        assert "test_value" == temp_stdout.getvalue().strip()

    @mock.patch("airflow.cli.commands.config_command.conf")
    def test_should_raise_exception_when_section_is_missing(self, mock_conf):
        mock_conf.has_section.return_value = False
        mock_conf.has_option.return_value = True

        with pytest.raises(SystemExit) as ctx:
            config_command.get_value(
                self.parser.parse_args(["config", "get-value", "missing-section", "dags_folder"])
            )
        assert "The section [missing-section] is not found in config." == str(ctx.value)

    @mock.patch("airflow.cli.commands.config_command.conf")
    def test_should_raise_exception_when_option_is_missing(self, mock_conf):
        mock_conf.has_section.return_value = True
        mock_conf.has_option.return_value = False

        with pytest.raises(SystemExit) as ctx:
            config_command.get_value(
                self.parser.parse_args(["config", "get-value", "missing-section", "dags_folder"])
            )
        assert "The option [missing-section/dags_folder] is not found in config." == str(ctx.value)
