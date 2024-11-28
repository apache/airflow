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

import json
import textwrap
from contextlib import redirect_stdout
from io import StringIO

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import plugins_command
from airflow.listeners.listener import get_listener_manager
from airflow.models.baseoperatorlink import BaseOperatorLink
from airflow.plugins_manager import AirflowPlugin

from tests.plugins.test_plugin import AirflowTestPlugin as ComplexAirflowPlugin
from tests_common.test_utils.mock_plugins import mock_plugin_manager

pytestmark = pytest.mark.db_test


class AirflowNewLink(BaseOperatorLink):
    """Operator Link for Apache Airflow Website."""

    name = "airflowtestlink"

    def get_link(self, operator, *, ti_key):
        return "https://airflow.apache.org"


class TestPlugin(AirflowPlugin):
    name = "test-plugin-cli"
    global_operator_extra_links = [AirflowNewLink()]


class TestPluginsCommand:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock_plugin_manager(plugins=[])
    def test_should_display_no_plugins(self):
        with redirect_stdout(StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(["plugins", "--output=json"]))
            stdout = temp_stdout.getvalue()
        assert "No plugins loaded" in stdout

    @mock_plugin_manager(plugins=[ComplexAirflowPlugin])
    def test_should_display_one_plugin(self):
        with redirect_stdout(StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(["plugins", "--output=json"]))
            stdout = temp_stdout.getvalue()
        print(stdout)
        info = json.loads(stdout)
        assert info == [
            {
                "name": "test_plugin",
                "admin_views": [],
                "macros": ["tests.plugins.test_plugin.plugin_macro"],
                "menu_links": [],
                "flask_blueprints": [
                    "<flask.blueprints.Blueprint: name='test_plugin' import_name='tests.plugins.test_plugin'>"
                ],
                "fastapi_apps": [
                    {
                        "app": "fastapi.applications.FastAPI",
                        "url_prefix": "/some_prefix",
                        "name": "Name of the App",
                    }
                ],
                "appbuilder_views": [
                    {
                        "name": "Test View",
                        "category": "Test Plugin",
                        "label": "Test Label",
                        "view": "tests.plugins.test_plugin.PluginTestAppBuilderBaseView",
                    }
                ],
                "global_operator_extra_links": [
                    "<tests_common.test_utils.mock_operators.AirflowLink object>",
                    "<tests_common.test_utils.mock_operators.GithubLink object>",
                ],
                "timetables": ["tests.plugins.test_plugin.CustomCronDataIntervalTimetable"],
                "operator_extra_links": [
                    "<tests_common.test_utils.mock_operators.GoogleLink object>",
                    "<tests_common.test_utils.mock_operators.AirflowLink2 object>",
                    "<tests_common.test_utils.mock_operators.CustomOpLink object>",
                    "<tests_common.test_utils.mock_operators.CustomBaseIndexOpLink object>",
                ],
                "listeners": [
                    "tests.listeners.empty_listener",
                    "tests.listeners.class_listener.ClassBasedListener",
                ],
                "source": None,
                "appbuilder_menu_items": [
                    {"name": "Google", "href": "https://www.google.com", "category": "Search"},
                    {
                        "name": "apache",
                        "href": "https://www.apache.org/",
                        "label": "The Apache Software Foundation",
                    },
                ],
                "ti_deps": ["<TIDep(CustomTestTriggerRule)>"],
                "priority_weight_strategies": ["tests.plugins.test_plugin.CustomPriorityWeightStrategy"],
            }
        ]
        get_listener_manager().clear()

    @mock_plugin_manager(plugins=[TestPlugin])
    def test_should_display_one_plugins_as_table(self):
        with redirect_stdout(StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(["plugins", "--output=table"]))
            stdout = temp_stdout.getvalue()

        # Remove leading spaces
        stdout = "\n".join(line.rstrip(" ") for line in stdout.splitlines())
        # Assert that only columns with values are displayed
        expected_output = textwrap.dedent(
            """\
            name            | global_operator_extra_links
            ================+================================================================
            test-plugin-cli | <tests.cli.commands.test_plugins_command.AirflowNewLink object>
            """
        )
        assert stdout == expected_output
