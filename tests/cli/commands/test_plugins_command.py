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
from contextlib import ExitStack, contextmanager, redirect_stdout
from unittest import mock

from airflow.cli import cli_parser
from airflow.cli.commands import plugins_command
from airflow.models.baseoperator import BaseOperator
from airflow.plugins_manager import AirflowPlugin


class PluginOperator(BaseOperator):
    pass


class TestPlugin(AirflowPlugin):
    name = "test-plugin-cli"

    operators = [PluginOperator]


PLUGINS_MANAGER_NULLABLE_ATTRIBUTES = [
    "plugins",
    "operators_modules",
    "sensors_modules",
    "hooks_modules",
    "macros_modules",
    "executors_modules",
    "admin_views",
    "flask_blueprints",
    "menu_links",
    "flask_appbuilder_views",
    "flask_appbuilder_menu_links",
    "global_operator_extra_links",
    "operator_extra_links",
    "registered_operator_link_classes",
]


@contextmanager
def keep_plugin_manager_state():
    """
    Protects the initial state and sets the default state for the airflow.plugins module.

    airflow.plugins_manager uses many global variables. To avoid side effects, this decorator performs
    the following operations:

    1. saves variables state,
    2. set variables to default value,
    3. executes context code,
    4. restores the state of variables to the state from point 1.

    Use this context if you want your test to not have side effects in airflow.plugins_manager, and
    other tests do not affect the results of this test.
    """
    with ExitStack() as exit_stack:
        for attr in PLUGINS_MANAGER_NULLABLE_ATTRIBUTES:
            exit_stack.enter_context(  # pylint: disable=no-member
                mock.patch(f"airflow.plugins_manager.{attr}", None)
            )
        exit_stack.enter_context(  # pylint: disable=no-member
            mock.patch("airflow.plugins_manager.import_errors", {})
        )

        yield


class TestPluginsCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @keep_plugin_manager_state()
    @mock.patch('airflow.plugins_manager.plugins', [])
    def test_should_display_no_plugins(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(['plugins']))
            stdout = temp_stdout.getvalue()

        self.assertIn('plugins = []', stdout)
        self.assertIn('No plugins loaded', stdout)
        self.assertIn("PLUGINS MANGER:", stdout)
        self.assertIn("PLUGINS:", stdout)

    @keep_plugin_manager_state()
    @mock.patch('airflow.plugins_manager.plugins', [TestPlugin])
    def test_should_display_one_plugins(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(['plugins']))
            stdout = temp_stdout.getvalue()
        self.assertIn('plugins = [<class ', stdout)
        self.assertIn('test-plugin-cli', stdout)
        self.assertIn('test_plugins_command.PluginOperator', stdout)
