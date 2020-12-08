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

import io
import unittest
from contextlib import redirect_stdout

from airflow.cli import cli_parser
from airflow.cli.commands import plugins_command
from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
from tests.test_utils.mock_plugins import mock_plugin_manager


class PluginHook(BaseHook):
    pass


class TestPlugin(AirflowPlugin):
    name = "test-plugin-cli"
    hooks = [PluginHook]


class TestPluginsCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock_plugin_manager(plugins=[])
    def test_should_display_no_plugins(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(['plugins']))
            stdout = temp_stdout.getvalue()
        self.assertIn('No plugins loaded', stdout)

    @mock_plugin_manager(plugins=[TestPlugin])
    def test_should_display_one_plugins(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(['plugins']))
            stdout = temp_stdout.getvalue()
        print(stdout)
        self.assertIn('Plugins directory:', stdout)
        self.assertIn("Loaded plugins: 1", stdout)
        self.assertIn('test-plugin-cli', stdout)
        self.assertIn('PluginHook', stdout)
