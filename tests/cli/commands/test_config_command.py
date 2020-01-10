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
import contextlib
import io
import unittest
from configparser import ConfigParser
from contextlib import contextmanager
from shutil import copy
from tempfile import NamedTemporaryFile
from unittest import mock

from airflow.bin import cli
from airflow.cli.commands import config_command
from airflow.configuration import TEST_CONFIG_FILE, conf
from tests.test_utils.config import conf_vars


@contextmanager
def temporary_config():
    with NamedTemporaryFile() as f:
        copy(TEST_CONFIG_FILE, f.name)
        try:
            yield f.name
        finally:
            copy(f.name, TEST_CONFIG_FILE)


class TestCliConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch("airflow.cli.commands.config_command.io.StringIO")
    @mock.patch("airflow.cli.commands.config_command.conf")
    def test_cli_show_config_should_write_data(self, mock_conf, mock_stringio):
        config_command.show_config(self.parser.parse_args(['config', 'show']))
        mock_conf.write.assert_called_once_with(mock_stringio.return_value.__enter__.return_value)

    @conf_vars({
        ('core', 'testkey'): 'test_value'
    })
    def test_cli_show_config_should_display_key(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(['config', 'show']))
        self.assertIn('[core]', temp_stdout.getvalue())
        self.assertIn('testkey = test_value', temp_stdout.getvalue())

    @conf_vars({
        ('core', 'testkey'): 'test_value',
        ('logging', 'testkey'): 'test_value'
    })
    def test_cli_show_config_should_display_only_selected_section(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(['config', 'show', 'logging']))
        self.assertIn('[logging]', temp_stdout.getvalue())
        self.assertIn('testkey = test_value', temp_stdout.getvalue())

    @mock.patch("airflow.configuration.resolve_actual_config")
    @mock.patch("airflow.cli.commands.config_command.ask_yesno")
    def test_cli_set_config_should_override(self, mock_ask_yes_no, mock_resolve_config):
        with temporary_config() as tmp_cfg:
            mock_resolve_config.return_value = tmp_cfg

            config_command.set_config_option(
                self.parser.parse_args(['config', 'set', 'core', 'testkey', 'other_test_value'])
            )
            # Reload config from file
            conf.read(TEST_CONFIG_FILE)

            mock_ask_yes_no.assert_called_once_with(mock.ANY, assume="Y")
            value = conf.get("core", "testkey")
            self.assertEqual("other_test_value", value)

    @mock.patch("airflow.configuration.resolve_actual_config")
    @mock.patch("airflow.cli.commands.config_command.ask_yesno")
    def test_cli_set_config_should_create_new_section(self, mock_ask_yes_no, mock_resolve_config):
        with temporary_config() as tmp_cfg:
            mock_resolve_config.return_value = tmp_cfg

            config_command.set_config_option(
                self.parser.parse_args(['config', 'set', 'new', 'testkey', 'other_test_value'])
            )
            # Reload config from file
            conf.read(TEST_CONFIG_FILE)

            value = conf.get("new", "testkey")
            self.assertEqual("other_test_value", value)

    @mock.patch("airflow.configuration.resolve_actual_config")
    @mock.patch("airflow.cli.commands.config_command.ask_yesno")
    def test_cli_set_config_should_skip_on_no(self, mock_ask_yes_no, mock_resolve_config):
        mock_ask_yes_no.return_value = False

        with temporary_config() as tmp_cfg:
            mock_resolve_config.return_value = tmp_cfg
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                config_command.set_config_option(
                    self.parser.parse_args(['config', 'set', 'new', 'testkey', 'other_value'])
                )
            self.assertIn("Update skipped", temp_stdout.getvalue())

            current_conf = ConfigParser()
            current_conf.read(TEST_CONFIG_FILE)

            # Nothing should change
            self.assertFalse(current_conf.has_section("new"))
