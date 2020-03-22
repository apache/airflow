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
from unittest import mock

from airflow.bin import cli
from airflow.cli.commands import config_command
from tests.test_utils.config import conf_vars


class TestCliConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.get_parser()

    @mock.patch("airflow.cli.commands.config_command.io.StringIO")
    @mock.patch("airflow.cli.commands.config_command.conf")
    def test_cli_show_config_should_write_data(self, mock_conf, mock_stringio):
        config_command.show_config(self.parser.parse_args(['config']))
        mock_conf.write.assert_called_once_with(mock_stringio.return_value.__enter__.return_value)

    @conf_vars({
        ('core', 'testkey'): 'test_value'
    })
    def test_cli_show_config_should_display_key(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            config_command.show_config(self.parser.parse_args(['config']))
        self.assertIn('[core]', temp_stdout.getvalue())
        self.assertIn('testkey = test_value', temp_stdout.getvalue())
