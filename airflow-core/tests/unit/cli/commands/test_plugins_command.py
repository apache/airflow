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

import importlib
import io
import json
import textwrap
from contextlib import redirect_stdout

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import plugins_command
from airflow.listeners import get_listener_manager
from airflow.plugins_manager import AirflowPlugin
from airflow.sdk import BaseOperatorLink

from tests_common.test_utils.mock_plugins import mock_plugin_manager

if importlib.util.find_spec("flask_appbuilder"):
    flask_appbuilder_installed = True
    from unit.plugins.test_plugin import AirflowTestPlugin as ComplexAirflowPlugin
else:
    ComplexAirflowPlugin = None  # type: ignore [misc, assignment]
    flask_appbuilder_installed = False

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
        with redirect_stdout(io.StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(["plugins", "--output=json"]))
            stdout = temp_stdout.getvalue()
        assert "No plugins loaded" in stdout

    @pytest.mark.skipif(not flask_appbuilder_installed, reason="Flask AppBuilder is not installed")
    @mock_plugin_manager(plugins=[ComplexAirflowPlugin])
    def test_should_display_one_plugin(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(["plugins", "--output=json"]))
            stdout = temp_stdout.getvalue()
        print(stdout)
        info = json.loads(stdout)
        assert info == [
            {
                "name": "test_plugin",
                "admin_views": [],
                "macros": ["unit.plugins.test_plugin.plugin_macro"],
                "menu_links": [],
                "flask_blueprints": [
                    "<flask.blueprints.Blueprint: name='test_plugin' import_name='unit.plugins.test_plugin'>"
                ],
                "fastapi_apps": [
                    {
                        "app": "fastapi.applications.FastAPI",
                        "url_prefix": "/some_prefix",
                        "name": "Name of the App",
                    }
                ],
                "fastapi_root_middlewares": [
                    {
                        "middleware": "unit.plugins.test_plugin.DummyMiddleware",
                        "name": "Name of the Middleware",
                    }
                ],
                "external_views": [
                    {
                        "destination": "nav",
                        "icon": "https://raw.githubusercontent.com/lucide-icons/lucide/refs/heads/main/icons/plug.svg",
                        "name": "Test IFrame Airflow Docs",
                        "href": "https://airflow.apache.org/",
                        "url_route": "test_iframe_plugin",
                        "category": "browse",
                    },
                ],
                "react_apps": [
                    {
                        "name": "Test React App",
                        "bundle_url": "https://example.com/test-plugin-bundle.js",
                        "icon": "https://raw.githubusercontent.com/lucide-icons/lucide/refs/heads/main/icons/plug.svg",
                        "url_route": "test_react_app",
                        "destination": "nav",
                        "category": "browse",
                    }
                ],
                "appbuilder_views": [
                    {
                        "name": "Test View",
                        "category": "Test Plugin",
                        "label": "Test Label",
                        "view": "unit.plugins.test_plugin.PluginTestAppBuilderBaseView",
                    }
                ],
                "global_operator_extra_links": [
                    "<tests_common.test_utils.mock_operators.AirflowLink object>",
                    "<tests_common.test_utils.mock_operators.GithubLink object>",
                ],
                "timetables": ["unit.plugins.test_plugin.CustomCronDataIntervalTimetable"],
                "operator_extra_links": [
                    "<tests_common.test_utils.mock_operators.GoogleLink object>",
                    "<tests_common.test_utils.mock_operators.AirflowLink2 object>",
                    "<tests_common.test_utils.mock_operators.CustomOpLink object>",
                    "<tests_common.test_utils.mock_operators.CustomBaseIndexOpLink object>",
                ],
                "listeners": [
                    "unit.listeners.empty_listener",
                    "unit.listeners.class_listener.ClassBasedListener",
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
                "priority_weight_strategies": ["unit.plugins.test_plugin.CustomPriorityWeightStrategy"],
            }
        ]
        get_listener_manager().clear()

    @mock_plugin_manager(plugins=[TestPlugin])
    def test_should_display_one_plugins_as_table(self):
        with redirect_stdout(io.StringIO()) as temp_stdout:
            plugins_command.dump_plugins(self.parser.parse_args(["plugins", "--output=table"]))
            stdout = temp_stdout.getvalue()

        # Remove leading spaces
        stdout = "\n".join(line.rstrip(" ") for line in stdout.splitlines())
        # Assert that only columns with values are displayed
        expected_output = textwrap.dedent(
            """\
            name            | global_operator_extra_links
            ================+===============================================================
            test-plugin-cli | <unit.cli.commands.test_plugins_command.AirflowNewLink object>
            """
        )
        assert stdout == expected_output
