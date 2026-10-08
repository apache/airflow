#
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
import inspect
import json
import logging
import os
import sys
from email.message import Message
from importlib.metadata import EntryPoint
from types import SimpleNamespace
from unittest import mock

import pytest

import airflow._shared.module_loading as module_loading
import airflow._shared.plugins_manager.plugins_manager as plugin_loader_module
import airflow.plugins_manager as plugins_manager
from airflow._shared.module_loading import qualname
from airflow.configuration import conf
from airflow.listeners.listener import get_listener_manager
from airflow.partition_mappers.window import Window
from airflow.plugins_manager import AirflowPlugin
from airflow.providers_manager import provider_incompatibility_reason

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker
from tests_common.test_utils.mock_plugins import mock_plugin_manager

pytestmark = pytest.mark.db_test

ON_LOAD_EXCEPTION_PLUGIN = """
from airflow.plugins_manager import AirflowPlugin

class AirflowTestOnLoadExceptionPlugin(AirflowPlugin):
    name = 'preload'

    def on_load(self, *args, **kwargs):
        raise Exception("oops")
"""


@pytest.fixture(autouse=True, scope="module")
def _clean_listeners():
    get_listener_manager().clear()
    yield
    get_listener_manager().clear()


class TestPluginsManager:
    @pytest.fixture(autouse=True)
    def clean_plugins(self):
        from airflow import plugins_manager

        plugins_manager._get_plugins.cache_clear()

    def test_no_log_when_no_plugins(self, caplog):
        with mock_plugin_manager(plugins=[]):
            from airflow import plugins_manager

            plugins_manager.ensure_plugins_loaded()

        assert [r for r in caplog.record_tuples if not r[0].startswith("opentelemetry.")] == []

    def test_loads_filesystem_plugins(self, caplog):
        from airflow import plugins_manager

        plugins, import_errors = plugins_manager._load_plugins_from_plugin_directory(
            plugins_folder=conf.get("core", "plugins_folder"),
            load_examples=conf.getboolean("core", "load_examples"),
            example_plugins_module="airflow.example_dags.plugins",
        )

        assert len(plugins) == 13
        assert not import_errors
        for plugin in plugins:
            if "AirflowTestOnLoadPlugin" in str(plugin):
                assert plugin.name == "preload"  # on_init() is not called here
                break
        else:
            pytest.fail("Wasn't able to find a registered `AirflowTestOnLoadPlugin`")

        assert [r for r in caplog.record_tuples if not r[0].startswith("opentelemetry.")] == []

    def test_empty_plugins_folder_logs_no_failure(self, caplog, tmp_path):
        from airflow import plugins_manager

        with (
            caplog.at_level(logging.DEBUG, logger="airflow.plugins_manager"),
            conf_vars(
                {
                    ("core", "plugins_folder"): os.fspath(tmp_path),
                    ("core", "load_examples"): "False",
                }
            ),
            mock.patch("airflow.plugins_manager._load_entrypoint_plugins", return_value=([], [])),
            mock.patch("airflow.plugins_manager._load_providers_plugins", return_value=([], [])),
        ):
            plugins, import_errors = plugins_manager._get_plugins()

        assert plugins == []
        assert import_errors == {}
        received_logs = caplog.text
        assert "Failed to load" not in received_logs
        assert "No plugins loaded" in received_logs

    def test_loads_filesystem_plugins_exception(self, caplog, tmp_path):
        from airflow import plugins_manager

        (tmp_path / "testplugin.py").write_text(ON_LOAD_EXCEPTION_PLUGIN)

        with (
            conf_vars({("core", "plugins_folder"): os.fspath(tmp_path)}),
            mock.patch("airflow.plugins_manager._load_entrypoint_plugins", return_value=([], [])),
            mock.patch("airflow.plugins_manager._load_providers_plugins", return_value=([], [])),
        ):
            plugins, import_errors = plugins_manager._get_plugins()

        assert len(plugins) == 6  # four are loaded from examples
        assert len(import_errors) == 1

        received_logs = caplog.text
        assert "Failed to load plugin" in received_logs
        assert "Failed to load 1 plugin file(s)" in received_logs
        assert "testplugin.py" in received_logs

    def test_duplicate_plugin_name_does_not_prevent_loading_subsequent_plugins(self):
        from airflow import plugins_manager

        class PluginA(AirflowPlugin):
            name = "plugin_a"

        class PluginB(AirflowPlugin):
            name = "plugin_b"

        class PluginC(AirflowPlugin):
            name = "plugin_c"

        plugin_a = PluginA()
        plugin_b = PluginB()
        plugin_b_dup = PluginB()
        plugin_c = PluginC()

        with (
            mock.patch(
                "airflow.plugins_manager._load_plugins_from_plugin_directory",
                return_value=([plugin_a, plugin_b], {}),
            ),
            mock.patch(
                "airflow.plugins_manager._load_entrypoint_plugins",
                return_value=([plugin_b_dup, plugin_c], {}),
            ),
            mock.patch("airflow.plugins_manager._load_providers_plugins", return_value=([], {})),
        ):
            plugins, import_errors = plugins_manager._get_plugins()

        plugin_names = [p.name for p in plugins]
        assert "plugin_a" in plugin_names
        assert "plugin_b" in plugin_names
        assert "plugin_c" in plugin_names
        assert len(plugins) == 3

    def test_duplicate_plugin_name_is_reported_as_import_error(self):
        from airflow import plugins_manager

        class PluginA(AirflowPlugin):
            name = "plugin_a"

        class PluginADuplicateName(AirflowPlugin):
            name = "plugin_a"

        plugin_a = PluginA()
        plugin_a_dup = PluginADuplicateName()

        with (
            mock.patch(
                "airflow.plugins_manager._load_plugins_from_plugin_directory",
                return_value=([plugin_a], {}),
            ),
            mock.patch(
                "airflow.plugins_manager._load_entrypoint_plugins",
                return_value=([plugin_a_dup], {}),
            ),
            mock.patch("airflow.plugins_manager._load_providers_plugins", return_value=([], {})),
        ):
            plugins, import_errors = plugins_manager._get_plugins()

        assert [p.name for p in plugins] == ["plugin_a"]
        assert len(import_errors) == 1

    def test_should_warning_about_incompatible_plugins(self, caplog):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            admin_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            menu_links = [mock.MagicMock()]

        with (
            mock_plugin_manager(plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            plugins_manager.get_flask_plugins()

        assert caplog.record_tuples == [
            (
                "airflow.plugins_manager",
                logging.WARNING,
                "Plugin 'test_admin_views_plugin' may not be compatible with the current Airflow version. "
                "Please contact the author of the plugin.",
            ),
            (
                "airflow.plugins_manager",
                logging.WARNING,
                "Plugin 'test_menu_links_plugin' may not be compatible with the current Airflow version. "
                "Please contact the author of the plugin.",
            ),
        ]

    def test_should_warning_about_conflicting_url_route(self, caplog):
        class TestPluginA(AirflowPlugin):
            name = "test_plugin_a"

            # Malformed on purpose to trigger the warning path; mypy ignores below.
            external_views = [{"url_route": "/test_route"}, {"wrong_view": "/no_url_route"}]  # type: ignore[typeddict-item, typeddict-unknown-key]

        class TestPluginB(AirflowPlugin):
            name = "test_plugin_b"

            # Malformed on purpose to trigger the warning path; mypy ignores below.
            external_views = [{"url_route": "/test_route"}]  # type: ignore[typeddict-item]
            react_apps = [{"url_route": "/test_route"}]  # type: ignore[typeddict-item]

        with (
            mock_plugin_manager(plugins=[TestPluginA(), TestPluginB()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            external_views, react_apps = plugins_manager._get_ui_plugins()

            # Verify that the conflicting external view and react app are not loaded
            plugin_b = next(
                plugin for plugin in plugins_manager._get_plugins()[0] if plugin.name == "test_plugin_b"
            )
            assert plugin_b.external_views == []
            assert plugin_b.react_apps == []
            assert len(external_views) == 1
            assert len(react_apps) == 0

    def test_should_warning_about_external_views_or_react_app_wrong_object(self, caplog):
        class TestPluginA(AirflowPlugin):
            name = "test_plugin_a"

            # Malformed on purpose to trigger the warning path; mypy ignores below.
            external_views = [[{"nested_list": "/test_route"}], {"url_route": "/test_route"}]  # type: ignore[list-item, typeddict-item]
            react_apps = [[{"nested_list": "/test_route"}], {"url_route": "/test_route_react_app"}]  # type: ignore[list-item, typeddict-item]

        with (
            mock_plugin_manager(plugins=[TestPluginA()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            external_views, react_apps = plugins_manager._get_ui_plugins()

            # Verify that the conflicting external view and react app are not loaded
            plugin_a = next(
                plugin for plugin in plugins_manager._get_plugins()[0] if plugin.name == "test_plugin_a"
            )
            assert plugin_a.external_views == [{"url_route": "/test_route"}]
            assert plugin_a.react_apps == [{"url_route": "/test_route_react_app"}]
            assert len(external_views) == 1
            assert len(react_apps) == 1

        assert caplog.record_tuples == [
            (
                "airflow.plugins_manager",
                logging.WARNING,
                "Plugin 'test_plugin_a' has an external view that is not a dictionary. "
                "The view will not be loaded.",
            ),
            (
                "airflow.plugins_manager",
                logging.WARNING,
                "Plugin 'test_plugin_a' has a React App that is not a dictionary. "
                "The React App will not be loaded.",
            ),
        ]

    def test_loads_typed_external_views_and_react_apps(self):
        class TypedPlugin(AirflowPlugin):
            name = "typed_plugin"

            # Recommended `ExternalViewDict` / `ReactAppDict` shapes — no `# type: ignore` needed here.
            external_views = [{"name": "typed-view", "href": "/typed", "url_route": "/typed"}]
            react_apps = [{"name": "typed-react", "bundle_url": "/typed.js", "url_route": "/typed_react"}]

        with mock_plugin_manager(plugins=[TypedPlugin()]):
            from airflow import plugins_manager

            external_views, react_apps = plugins_manager._get_ui_plugins()

            assert external_views == [{"name": "typed-view", "href": "/typed", "url_route": "/typed"}]
            assert react_apps == [
                {"name": "typed-react", "bundle_url": "/typed.js", "url_route": "/typed_react"}
            ]

    @pytest.mark.parametrize(
        ("applies_to", "error"),
        [
            pytest.param(
                ["ml"],
                "expected a dictionary, got list",
                id="not-a-dict",
            ),
            pytest.param(
                {1: ["ml"]},
                "field paths must be strings, got [1]",
                id="non-string-key",
            ),
            pytest.param(
                {"": ["ml"]},
                "field paths must not be empty, got ['']",
                id="empty-key",
            ),
            pytest.param(
                {"dag_ids": "my_dag"},
                "'dag_ids' must be a list of strings, got 'my_dag'",
                id="scalar-instead-of-list",
            ),
            pytest.param(
                {"dag_ids": ["my_dag", 3]},
                "'dag_ids' must be a list of strings, got ['my_dag', 3]",
                id="list-with-non-string",
            ),
        ],
    )
    def test_strips_and_warns_about_malformed_applies_to(self, applies_to, error, caplog):
        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            external_views = [
                {
                    "name": "Scoped",
                    "href": "/scoped",
                    "url_route": "/scoped",
                    "destination": "dag",
                    "applies_to": applies_to,
                }
            ]

        with (
            mock_plugin_manager(plugins=[TestPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            external_views, _ = plugins_manager._get_ui_plugins()

            assert external_views == [
                {"name": "Scoped", "href": "/scoped", "url_route": "/scoped", "destination": "dag"}
            ]

        assert caplog.record_tuples == [
            (
                "airflow.plugins_manager",
                logging.WARNING,
                f"Plugin 'test_plugin' has an external view 'Scoped' with an invalid 'applies_to': {error}. "
                "The scoping will be ignored.",
            ),
        ]

    def test_warns_about_criteria_a_destination_cannot_evaluate(self, caplog):
        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            react_apps = [
                {
                    "name": "Scoped",
                    "bundle_url": "/scoped.js",
                    "url_route": "/scoped",
                    "destination": "dag_run",
                    "applies_to": {
                        "dag.tags.name": ["ml"],
                        "task.class_ref.class_name": ["Op"],
                        "task_instance.operator": ["Op"],
                    },
                }
            ]

        with (
            mock_plugin_manager(plugins=[TestPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            _, react_apps = plugins_manager._get_ui_plugins()

            # The block is only warned about, never stripped: a path whose root the page lacks
            # is skipped by design, so one block can be shared across destinations.
            assert react_apps == [
                {
                    "name": "Scoped",
                    "bundle_url": "/scoped.js",
                    "url_route": "/scoped",
                    "destination": "dag_run",
                    "applies_to": {
                        "dag.tags.name": ["ml"],
                        "task.class_ref.class_name": ["Op"],
                        "task_instance.operator": ["Op"],
                    },
                }
            ]

        assert caplog.record_tuples == [
            (
                "airflow.plugins_manager",
                logging.WARNING,
                "Plugin 'test_plugin' has a React App 'Scoped' with destination 'dag_run', which cannot "
                "evaluate ['task.class_ref.class_name', 'task_instance.operator']. Those paths will be "
                "ignored.",
            ),
        ]

    @pytest.mark.parametrize(
        ("path", "error"),
        [
            pytest.param(
                "dag.tags.nme",
                "'dag.tags.nme' names no field 'nme' on DagTagResponse (did you mean 'name'?)",
                id="misspelled-nested-leaf",
            ),
            pytest.param(
                "stat",
                "'stat' names no field 'stat' on DAGRunResponse (did you mean 'state'?)",
                id="misspelled-own-field",
            ),
            pytest.param(
                "state.length",
                "'state.length' reads 'length' from DagRunState, which has no fields",
                id="path-past-a-scalar",
            ),
        ],
    )
    def test_warns_about_a_path_matching_no_field(self, path, error, caplog):
        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            external_views = [
                {
                    "name": "Scoped",
                    "href": "/scoped",
                    "url_route": "/scoped",
                    "destination": "dag_run",
                    "applies_to": {path: ["x"]},
                }
            ]

        with (
            mock_plugin_manager(plugins=[TestPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            external_views, _ = plugins_manager._get_ui_plugins()

            # Warned about, not stripped: the path is inert either way, and dropping it would
            # change the block the UI receives.
            assert external_views[0]["applies_to"] == {path: ["x"]}

        assert caplog.record_tuples == [
            (
                "airflow.plugins_manager",
                logging.WARNING,
                f"Plugin 'test_plugin' has an external view 'Scoped' with an 'applies_to' path that "
                f"matches no field: {error}. That path will be ignored, so the view will appear in "
                f"more places than intended.",
            ),
        ]

    @pytest.mark.parametrize(
        ("destination", "path"),
        [
            # `TaskInstanceResponse.run_id` is serialized as `dag_run_id`; the browser only ever
            # sees the alias.
            pytest.param("task_instance", "dag_run_id", id="serialization-alias"),
            pytest.param("task_instance", "queued_when", id="serialization-alias-datetime"),
            # A computed field has no `model_fields` entry but is in the response.
            pytest.param("dag", "is_backfillable", id="computed-field"),
        ],
    )
    def test_accepts_fields_as_the_api_serializes_them(self, destination, path, caplog):
        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            external_views = [
                {
                    "name": "Scoped",
                    "href": "/scoped",
                    "url_route": "/scoped",
                    "destination": destination,
                    "applies_to": {path: ["x"]},
                }
            ]

        with (
            mock_plugin_manager(plugins=[TestPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            plugins_manager._get_ui_plugins()

        assert caplog.record_tuples == []

    def test_rejects_a_python_attribute_name_that_is_not_in_the_response(self, caplog):
        """`run_id` is the Python attribute; the response carries `dag_run_id`."""

        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            external_views = [
                {
                    "name": "Scoped",
                    "href": "/scoped",
                    "url_route": "/scoped",
                    "destination": "task_instance",
                    "applies_to": {"run_id": ["manual__1"]},
                }
            ]

        with (
            mock_plugin_manager(plugins=[TestPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            plugins_manager._get_ui_plugins()

        assert len(caplog.record_tuples) == 1
        assert "names no field 'run_id' on TaskInstanceResponse" in caplog.record_tuples[0][2]

    def test_drops_a_null_valued_path_keeping_the_rest_of_the_block(self):
        """A null value is not a ``list[str]``; leaving it in drops the whole plugin."""

        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            external_views = [
                {
                    "name": "Scoped",
                    "href": "/scoped",
                    "url_route": "/scoped",
                    "destination": "dag_run",
                    "applies_to": {"state": None, "dag.tags.name": ["ml"]},
                }
            ]

        with mock_plugin_manager(plugins=[TestPlugin()]):
            from airflow import plugins_manager

            external_views, _ = plugins_manager._get_ui_plugins()

            assert external_views[0]["applies_to"] == {"dag.tags.name": ["ml"]}

            # The point of dropping it: the block still serializes, so the view survives.
            from airflow.api_fastapi.core_api.datamodels.plugins import ExternalViewResponse

            assert ExternalViewResponse(**external_views[0]).applies_to.root == {"dag.tags.name": ["ml"]}

    def test_accepts_a_path_through_a_field_the_models_do_not_describe(self, caplog):
        """A path cannot be checked past a bare ``dict``, so everything below it is accepted."""

        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            external_views = [
                {
                    "name": "Scoped",
                    "href": "/scoped",
                    "url_route": "/scoped",
                    "destination": "task_instance",
                    # `TaskResponse.class_ref` is a bare dict, and `conf` is dict[str, Any].
                    "applies_to": {
                        "task.class_ref.class_name": ["KubernetesPodOperator"],
                        "dag_run.conf.environment": ["prod"],
                    },
                }
            ]

        with (
            mock_plugin_manager(plugins=[TestPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            plugins_manager._get_ui_plugins()

        assert caplog.record_tuples == []

    def test_does_not_warn_about_valid_applies_to(self, caplog):
        class TestPlugin(AirflowPlugin):
            name = "test_plugin"

            external_views = [
                {
                    "name": "Scoped",
                    "href": "/scoped",
                    "url_route": "/scoped",
                    "destination": "task",
                    "applies_to": {
                        "dag.tags.name": ["ml"],
                        "operator_name": ["@task.bash"],
                        "task_id": ["train"],
                    },
                }
            ]

        with (
            mock_plugin_manager(plugins=[TestPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            external_views, _ = plugins_manager._get_ui_plugins()

            assert external_views[0]["applies_to"] == {
                "dag.tags.name": ["ml"],
                "operator_name": ["@task.bash"],
                "task_id": ["train"],
            }

        assert caplog.record_tuples == []

    def test_should_not_warning_about_fab_plugins(self, caplog):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            appbuilder_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            appbuilder_menu_items = [mock.MagicMock()]

        with (
            mock_plugin_manager(plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            plugins_manager.get_flask_plugins()

        assert caplog.record_tuples == []

    def test_should_not_warning_about_fab_and_flask_admin_plugins(self, caplog):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            admin_views = [mock.MagicMock()]
            appbuilder_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            menu_links = [mock.MagicMock()]
            appbuilder_menu_items = [mock.MagicMock()]

        with (
            mock_plugin_manager(plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]),
            caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"),
        ):
            from airflow import plugins_manager

            plugins_manager.get_flask_plugins()

        assert caplog.record_tuples == []

    def test_registering_plugin_macros(self, request):
        """
        Tests whether macros that originate from plugins are being registered correctly.
        """
        from airflow.plugins_manager import integrate_macros_plugins
        from airflow.sdk.execution_time import macros

        def cleanup_macros():
            """Reloads the macros module such that the symbol table is reset after the test."""
            # We're explicitly deleting the module from sys.modules and importing it again
            # using import_module() as opposed to using importlib.reload() because the latter
            # does not undo the changes to the airflow.sdk.execution_time.macros module that are being caused by
            # invoking integrate_macros_plugins()

            del sys.modules["airflow.sdk.execution_time.macros"]
            importlib.import_module("airflow.sdk.execution_time.macros")

        request.addfinalizer(cleanup_macros)

        def custom_macro():
            return "foo"

        class MacroPlugin(AirflowPlugin):
            name = "macro_plugin"
            macros = [custom_macro]

        with mock_plugin_manager(plugins=[MacroPlugin()]):
            # Ensure the macros for the plugin have been integrated.
            integrate_macros_plugins()
            # Test whether the modules have been created as expected.
            plugin_macros = importlib.import_module(f"airflow.sdk.execution_time.macros.{MacroPlugin.name}")
            for macro in MacroPlugin.macros:
                # Verify that the macros added by the plugin are being set correctly
                # on the plugin's macro module.
                assert hasattr(plugin_macros, macro.__name__)
            # Verify that the symbol table in airflow.sdk.execution_time.macros has been updated with an entry for
            # this plugin, this is necessary in order to allow the plugin's macros to be used when
            # rendering templates.
            assert hasattr(macros, MacroPlugin.name or "")

    @skip_if_force_lowest_dependencies_marker
    def test_registering_plugin_listeners(self):
        from airflow import plugins_manager

        assert not get_listener_manager().has_listeners
        with mock_plugin_manager(
            plugins=plugins_manager._load_plugins_from_plugin_directory(
                plugins_folder=conf.get("core", "plugins_folder"),
                load_examples=conf.getboolean("core", "load_examples"),
                example_plugins_module="airflow.example_dags.plugins",
            )[0]
        ):
            plugins_manager.integrate_listener_plugins(get_listener_manager())

            assert get_listener_manager().has_listeners
            listeners = get_listener_manager().pm.get_plugins()
            listener_names = [el.__name__ if inspect.ismodule(el) else qualname(el) for el in listeners]
            # sort names as order of listeners is not guaranteed
            assert sorted(listener_names) == [
                "airflow.example_dags.plugins.event_listener",
                "unit.listeners.class_listener.ClassBasedListener",
                "unit.listeners.empty_listener",
            ]

    @skip_if_force_lowest_dependencies_marker
    def test_should_import_plugin_from_providers(self):
        from airflow import plugins_manager

        plugins, import_errors = plugins_manager._load_providers_plugins()
        assert len(plugins) >= 2
        assert not import_errors

    @skip_if_force_lowest_dependencies_marker
    def test_does_not_double_import_entrypoint_provider_plugins(self):
        from airflow import plugins_manager

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = "test-entrypoint-plugin"
        mock_entrypoint.module = "module_name_plugin"

        mock_dist = mock.Mock()
        mock_dist.metadata = {"Name": "test-entrypoint-plugin"}
        mock_dist.version = "1.0.0"
        mock_dist.entry_points = [mock_entrypoint]

        # Mock/skip loading from plugin dir
        with mock.patch("airflow.plugins_manager._load_plugins_from_plugin_directory", return_value=([], [])):
            plugins = plugins_manager._get_plugins()[0]
        assert len(plugins) == 7


class TestWindowPluginRegistration:
    """``windows`` plugin attribute surfaces via ``get_windows_plugins()``."""

    def test_windows_attribute_surfaces_via_getter(self):
        from airflow import plugins_manager

        class MyCustomWindow(Window):
            name = "test_window_plugin"

            def to_upstream(self, decoded_downstream):
                return [decoded_downstream]

        class MyWindowPlugin(AirflowPlugin):
            name = "test_window_plugin"
            windows = [MyCustomWindow]

        with mock_plugin_manager(plugins=[MyWindowPlugin()]):
            plugins_manager.get_windows_plugins.cache_clear()
            registered = plugins_manager.get_windows_plugins()

        assert qualname(MyCustomWindow) in registered
        assert registered[qualname(MyCustomWindow)] is MyCustomWindow


class TestPluginTeamName:
    """``team_name`` exposure through ``get_plugin_info`` (attribute default is covered
    by the shared plugins_manager tests)."""

    def test_get_plugin_info_includes_team_name(self):
        from airflow import plugins_manager

        class GlobalPlugin(AirflowPlugin):
            name = "global_plugin"

        class TeamPlugin(AirflowPlugin):
            name = "team_plugin"
            team_name = "team_a"

        with mock_plugin_manager(plugins=[GlobalPlugin(), TeamPlugin()]):
            info_by_name = {info["name"]: info for info in plugins_manager.get_plugin_info()}

        assert info_by_name["global_plugin"]["team_name"] is None
        assert info_by_name["team_plugin"]["team_name"] == "team_a"


class TestGetSchedulingClassTeams:
    @staticmethod
    def _plugin(team_name, **registries):
        plugin = AirflowPlugin()
        plugin.name = f"plugin_{team_name}"
        plugin.team_name = team_name
        for registry, classes in registries.items():
            setattr(plugin, registry, classes)
        return plugin

    def test_maps_each_registry_by_qualname(self):
        from airflow.example_dags.plugins.business_day_window import BusinessDayWindow
        from airflow.example_dags.plugins.custom_partition_mapper import PrefixStripMapper
        from airflow.example_dags.plugins.decreasing_priority_weight_strategy import (
            DecreasingPriorityStrategy,
        )
        from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

        plugin = self._plugin(
            "team_a",
            timetables=[AfterWorkdayTimetable],
            partition_mappers=[PrefixStripMapper],
            windows=[BusinessDayWindow],
            priority_weight_strategies=[DecreasingPriorityStrategy],
        )
        with mock_plugin_manager(plugins=[plugin]):
            assert plugins_manager.get_scheduling_class_teams() == {
                qualname(cls): frozenset({"team_a"})
                for cls in (
                    AfterWorkdayTimetable,
                    PrefixStripMapper,
                    BusinessDayWindow,
                    DecreasingPriorityStrategy,
                )
            }

    def test_class_registered_by_several_plugins_maps_to_all_their_teams(self):
        from airflow.example_dags.plugins.workday import AfterWorkdayTimetable

        plugins = [self._plugin(team, timetables=[AfterWorkdayTimetable]) for team in ("team_a", None)]
        with mock_plugin_manager(plugins=plugins):
            assert plugins_manager.get_scheduling_class_teams() == {
                qualname(AfterWorkdayTimetable): frozenset({"team_a", None})
            }

    def test_airflow_classes_are_left_out(self):
        """The decoder imports these directly, so no plugin can own them."""
        from airflow.partition_mappers.temporal import StartOfDayMapper
        from airflow.partition_mappers.window import DayWindow
        from airflow.timetables.trigger import CronTriggerTimetable

        plugin = self._plugin(
            "team_a",
            timetables=[CronTriggerTimetable],
            partition_mappers=[StartOfDayMapper],
            windows=[DayWindow],
        )
        with mock_plugin_manager(plugins=[plugin]):
            assert plugins_manager.get_scheduling_class_teams() == {}


class TestValidatePluginTeams:
    """``validate_plugin_teams`` startup validation."""

    def test_no_op_when_multi_team_disabled(self):
        from airflow import plugins_manager

        class TeamPlugin(AirflowPlugin):
            name = "team_plugin"
            team_name = "nonexistent_team"

        # multi_team defaults to False; validation must return early without hitting
        # the database, even for a plugin pointing at a nonexistent team.
        with mock_plugin_manager(plugins=[TeamPlugin()]):
            with mock.patch("airflow.models.team.Team.get_all_team_names") as mock_get_all_team_names:
                plugins_manager.validate_plugin_teams()
        mock_get_all_team_names.assert_not_called()

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.models.team.Team.get_all_team_names", return_value={"team_a"})
    def test_passes_for_global_and_known_team_plugins(self, mock_get_all_team_names):
        from airflow import plugins_manager

        class GlobalPlugin(AirflowPlugin):
            name = "global_plugin"

        class TeamPlugin(AirflowPlugin):
            name = "team_plugin"
            team_name = "team_a"

        with mock_plugin_manager(plugins=[GlobalPlugin(), TeamPlugin()]):
            plugins_manager.validate_plugin_teams()

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.models.team.Team.get_all_team_names", return_value={"team_a"})
    def test_get_fastapi_plugins_records_unknown_team_import_error(self, mock_get_all_team_names, caplog):
        from airflow import plugins_manager

        class TeamPlugin(AirflowPlugin):
            name = "team_plugin"
            team_name = "unknown_team"

        # get_fastapi_plugins() is what init_plugins() calls, so validation runs
        # automatically: a plugin on a nonexistent team is recorded as an import error
        # and warned, not raised, so the API server and every other plugin still start.
        with mock_plugin_manager(plugins=[TeamPlugin()], import_errors={}):
            with caplog.at_level(logging.WARNING, logger="airflow.plugins_manager"):
                plugins_manager.get_fastapi_plugins()
            recorded = plugins_manager.get_import_errors()

        assert "unknown_team" in recorded["team_plugin"]
        warnings = [msg for _, level, msg in caplog.record_tuples if level == logging.WARNING]
        assert any("team_plugin" in msg and "unknown_team" in msg for msg in warnings)


class TestGetFastapiPluginsTeamName:
    """``get_fastapi_plugins`` must tell the API server which team each app belongs to,
    since that is what lets ``init_plugins`` authorize a team-scoped plugin's app."""

    @staticmethod
    def _plugins():
        class GlobalPlugin(AirflowPlugin):
            name = "global_plugin"

        class TeamPlugin(AirflowPlugin):
            name = "team_plugin"
            team_name = "team_a"

        global_plugin = GlobalPlugin()
        team_plugin = TeamPlugin()
        # Per-instance dicts so a mutation would be visible to the assertions below.
        global_plugin.fastapi_apps = [{"name": "global_app", "app": object(), "url_prefix": "/global"}]
        global_plugin.fastapi_root_middlewares = [{"name": "global_mw", "middleware": object()}]
        team_plugin.fastapi_apps = [{"name": "team_app", "app": object(), "url_prefix": "/team"}]
        team_plugin.fastapi_root_middlewares = [{"name": "team_mw", "middleware": object()}]
        return global_plugin, team_plugin

    def test_team_name_is_added_to_apps_and_middlewares(self):
        from airflow import plugins_manager

        global_plugin, team_plugin = self._plugins()
        with mock_plugin_manager(plugins=[global_plugin, team_plugin]):
            apps, middlewares = plugins_manager.get_fastapi_plugins()

        assert {app["name"]: app["team_name"] for app in apps} == {
            "global_app": None,
            "team_app": "team_a",
        }
        assert {mw["name"]: mw["team_name"] for mw in middlewares} == {
            "global_mw": None,
            "team_mw": "team_a",
        }

    def test_plugin_dicts_are_not_mutated(self):
        """The plugin's own dicts must stay clean so ``get_plugin_info`` (and therefore
        the public API response) does not gain an unexpected ``team_name`` key."""
        from airflow import plugins_manager

        global_plugin, team_plugin = self._plugins()
        with mock_plugin_manager(plugins=[global_plugin, team_plugin]):
            plugins_manager.get_fastapi_plugins()

        for plugin in (global_plugin, team_plugin):
            assert "team_name" not in plugin.fastapi_apps[0]
            assert "team_name" not in plugin.fastapi_root_middlewares[0]


class TestMergeTranslations:
    def test_override_wins_and_preserves_siblings(self):
        base = {"a": "base", "group": {"x": "bx", "y": "by"}}
        override = {"a": "override", "group": {"y": "oy", "z": "oz"}, "added": "new"}

        result = plugins_manager.merge_translations(base, override)

        assert result == {"a": "override", "group": {"x": "bx", "y": "oy", "z": "oz"}, "added": "new"}

    def test_overrides_a_deeply_nested_key_without_dropping_siblings(self):
        base = {"dagRun": {"durationStats": {"mean": "Mean", "mode": "Mode"}}}
        override = {"dagRun": {"durationStats": {"mean": "Moyenne"}}}

        result = plugins_manager.merge_translations(base, override)

        assert result == {"dagRun": {"durationStats": {"mean": "Moyenne", "mode": "Mode"}}}

    def test_inputs_are_not_mutated(self):
        base = {"group": {"x": "bx"}}
        override = {"group": {"y": "oy"}}

        plugins_manager.merge_translations(base, override)

        assert base == {"group": {"x": "bx"}}
        assert override == {"group": {"y": "oy"}}


class TestGetUiTranslations:
    def test_returns_empty_without_translation_plugins(self):
        with mock_plugin_manager(plugins=[]):
            assert plugins_manager.get_ui_translations() == {}

    def test_collects_inline_mapping_source(self):
        class InlinePlugin(AirflowPlugin):
            name = "inline"
            ui_translations = [{"en": {"common": {"greeting": "Hi"}}}]

        with mock_plugin_manager(plugins=[InlinePlugin()]):
            assert plugins_manager.get_ui_translations() == {"en": {"common": {"greeting": "Hi"}}}

    def test_collects_directory_source_including_new_language(self, tmp_path):
        locales = tmp_path / "locales"
        (locales / "eo").mkdir(parents=True)
        (locales / "eo" / "common.json").write_text(json.dumps({"greeting": "Saluton"}), encoding="utf-8")

        class DirectoryPlugin(AirflowPlugin):
            name = "directory"
            ui_translations = [locales]

        with mock_plugin_manager(plugins=[DirectoryPlugin()]):
            assert plugins_manager.get_ui_translations() == {"eo": {"common": {"greeting": "Saluton"}}}

    def test_deep_merges_across_plugins(self):
        class PluginA(AirflowPlugin):
            name = "a"
            ui_translations = [{"en": {"common": {"a": "1", "shared": {"x": "ax"}}}}]

        class PluginB(AirflowPlugin):
            name = "b"
            ui_translations = [{"en": {"common": {"b": "2", "shared": {"y": "by"}}}}]

        with mock_plugin_manager(plugins=[PluginA(), PluginB()]):
            assert plugins_manager.get_ui_translations() == {
                "en": {"common": {"a": "1", "b": "2", "shared": {"x": "ax", "y": "by"}}}
            }

    def test_skips_malformed_inline_source_but_keeps_valid_one(self, caplog):
        class BadPlugin(AirflowPlugin):
            name = "bad"
            ui_translations = [{"en": {"common": "not-a-mapping"}}]

        class GoodPlugin(AirflowPlugin):
            name = "good"
            ui_translations = [{"fr": {"common": {"greeting": "Bonjour"}}}]

        with mock_plugin_manager(plugins=[BadPlugin(), GoodPlugin()]), caplog.at_level(logging.WARNING):
            plugin_translations = plugins_manager.get_ui_translations()

        assert plugin_translations == {"fr": {"common": {"greeting": "Bonjour"}}}
        assert any("bad" in record.getMessage() for record in caplog.records)

    def test_skips_source_that_is_neither_directory_nor_mapping(self, caplog):
        class WeirdPlugin(AirflowPlugin):
            name = "weird"
            ui_translations = ["/nonexistent/locales/path", 123]

        with mock_plugin_manager(plugins=[WeirdPlugin()]), caplog.at_level(logging.WARNING):
            assert plugins_manager.get_ui_translations() == {}

        assert any("weird" in record.getMessage() for record in caplog.records)

    def test_skips_unreadable_file_but_keeps_the_rest_of_the_tree(self, tmp_path, caplog):
        locales = tmp_path / "locales"
        (locales / "eo").mkdir(parents=True)
        (locales / "eo" / "common.json").write_text("{ not valid json", encoding="utf-8")
        (locales / "eo" / "dags.json").write_text(json.dumps({"title": "Fluoj"}), encoding="utf-8")

        class DirectoryPlugin(AirflowPlugin):
            name = "directory"
            ui_translations = [locales]

        with mock_plugin_manager(plugins=[DirectoryPlugin()]), caplog.at_level(logging.WARNING):
            plugin_translations = plugins_manager.get_ui_translations()

        assert plugin_translations == {"eo": {"dags": {"title": "Fluoj"}}}
        assert any("common.json" in record.getMessage() for record in caplog.records)

    def test_broken_ui_translations_attribute_does_not_break_other_plugins(self, caplog):
        class BrokenPlugin(AirflowPlugin):
            name = "broken"
            ui_translations = 123  # not even iterable

        class GoodPlugin(AirflowPlugin):
            name = "good"
            ui_translations = [{"fr": {"common": {"greeting": "Bonjour"}}}]

        with mock_plugin_manager(plugins=[BrokenPlugin(), GoodPlugin()]), caplog.at_level(logging.WARNING):
            plugin_translations = plugins_manager.get_ui_translations()

        assert plugin_translations == {"fr": {"common": {"greeting": "Bonjour"}}}
        assert any("broken" in record.getMessage() for record in caplog.records)


class TestWarnAboutUnknownTranslationKeys:
    @staticmethod
    def _english_reference(tmp_path):
        reference_dir = tmp_path / "en"
        reference_dir.mkdir()
        (reference_dir / "common.json").write_text(
            json.dumps({"greeting": "Hi", "group": {"known": "K"}}), encoding="utf-8"
        )
        return reference_dir

    def test_warns_only_for_keys_absent_from_english(self, tmp_path, caplog):
        reference_dir = self._english_reference(tmp_path)
        plugin_translations = {
            "eo": {
                "common": {
                    "greeting": "Saluton",
                    "group": {"known": "Konata", "unknown": "Nekonata"},
                    "stale": "Malaktuala",
                }
            }
        }

        with caplog.at_level(logging.WARNING):
            plugins_manager.warn_about_unknown_translation_keys(plugin_translations, reference_dir)

        messages = [record.getMessage() for record in caplog.records]
        assert not any("'greeting'" in message for message in messages)
        assert not any("'group.known'" in message for message in messages)
        assert any("'group.unknown'" in message for message in messages)
        assert any("'stale'" in message for message in messages)

    def test_missing_reference_file_warns_without_raising(self, tmp_path, caplog):
        plugin_translations = {"eo": {"absent_namespace": {"a": "b"}}}

        with caplog.at_level(logging.WARNING):
            plugins_manager.warn_about_unknown_translation_keys(plugin_translations, tmp_path / "en")

        assert any("'a'" in record.getMessage() for record in caplog.records)


class TestExtraLinkTeamVisibility:
    """``is_extra_link_visible_to_team`` decides whether a team-scoped plugin's operator link
    is rendered for a given Dag, so the API server can hide one team's links from another."""

    @staticmethod
    def _link_class():
        from tests_common.test_utils.compat import BaseOperatorLink

        class SomeLink(BaseOperatorLink):
            name = "Some Link"

            def get_link(self, operator, ti_key):
                return "https://example.com"

        return SomeLink

    def test_operator_defined_link_is_visible_to_every_team(self):
        """A link no plugin registered belongs to the operator, so no team owns it."""
        from airflow import plugins_manager

        link = self._link_class()()
        with mock_plugin_manager(plugins=[]):
            assert plugins_manager.is_extra_link_visible_to_team(link, "team_a") is True
            assert plugins_manager.is_extra_link_visible_to_team(link, None) is True

    @pytest.mark.parametrize(
        ("dag_team", "expected"),
        [
            pytest.param("team_a", True, id="owning-team"),
            pytest.param("team_b", False, id="other-team"),
            pytest.param(None, False, id="teamless-dag"),
        ],
    )
    def test_team_scoped_link_is_visible_only_to_its_team(self, dag_team, expected):
        from airflow import plugins_manager

        link_class = self._link_class()

        class TeamPlugin(AirflowPlugin):
            name = "team_a_link_plugin"
            team_name = "team_a"
            global_operator_extra_links = [link_class()]

        with mock_plugin_manager(plugins=[TeamPlugin]):
            assert plugins_manager.is_extra_link_visible_to_team(link_class(), dag_team) is expected

    def test_link_registered_by_a_global_plugin_too_stays_global(self):
        """Ownership resolves least restrictively: one global registration keeps the link
        visible everywhere, rather than the team registration narrowing it."""
        from airflow import plugins_manager

        link_class = self._link_class()

        class TeamPlugin(AirflowPlugin):
            name = "team_a_link_plugin"
            team_name = "team_a"
            global_operator_extra_links = [link_class()]

        class GlobalPlugin(AirflowPlugin):
            name = "global_link_plugin"
            global_operator_extra_links = [link_class()]

        with mock_plugin_manager(plugins=[TeamPlugin, GlobalPlugin]):
            assert plugins_manager.is_extra_link_visible_to_team(link_class(), "team_b") is True
            assert plugins_manager.is_extra_link_visible_to_team(link_class(), None) is True

    def test_operator_scoped_links_are_tracked_alongside_global_ones(self):
        """``operator_extra_links`` are team-owned on the same terms as ``global_operator_extra_links``."""
        from airflow import plugins_manager

        link_class = self._link_class()

        class TeamPlugin(AirflowPlugin):
            name = "team_a_link_plugin"
            team_name = "team_a"
            operator_extra_links = [link_class()]

        with mock_plugin_manager(plugins=[TeamPlugin]):
            assert plugins_manager.is_extra_link_visible_to_team(link_class(), "team_a") is True
            assert plugins_manager.is_extra_link_visible_to_team(link_class(), "team_b") is False


@pytest.mark.parametrize(
    ("version", "direct_url", "allowed"),
    [
        pytest.param("0.10.0", None, False, id="released-0.10.0"),
        pytest.param("1.0.0rc1", None, True, id="release-candidate-1.0.0rc1"),
        pytest.param("1.0.0", None, True, id="released-1.0.0"),
        pytest.param("1.1.0", None, True, id="released-1.1.0"),
        pytest.param("0.10.0", {"dir_info": {"editable": True}}, True, id="editable-source-install"),
        pytest.param("0.10.0", {"dir_info": {}}, True, id="directory-source-install"),
        pytest.param("0.10.0", {"archive_info": {}}, False, id="local-archive-install"),
    ],
)
def test_common_ai_plugin_entrypoint_checks_installed_version_before_import(
    monkeypatch, caplog, version, direct_url, allowed
):
    metadata = Message()
    metadata["Name"] = "apache-airflow-providers-common-ai"
    distribution = SimpleNamespace(
        metadata=metadata,
        version=version,
        read_text=lambda filename: json.dumps(direct_url) if direct_url else None,
    )

    # Defined here, not at module level: this module sits in the plugins folder and would be loaded as a plugin.
    class CompatiblePlugin(AirflowPlugin):
        name = "compatible_test_plugin"

    entry_point = mock.Mock(spec=EntryPoint)
    entry_point.name = "hitl_review"
    entry_point.module = "airflow.providers.common.ai.plugins.hitl_review"
    entry_point.load.return_value = CompatiblePlugin
    monkeypatch.setattr(
        module_loading,
        "entry_points_with_dist",
        lambda group: [(entry_point, distribution)],
    )

    with caplog.at_level(logging.WARNING):
        plugins, import_errors = plugin_loader_module._load_entrypoint_plugins(
            provider_incompatibility_reason
        )

    assert bool(plugins) is allowed
    assert entry_point.load.called is allowed
    if not allowed:
        assert import_errors[entry_point.module].startswith(
            f"Skipping incompatible provider apache-airflow-providers-common-ai {version}"
        )
        assert "apache-airflow-providers-common-ai>=1.0.0" in caplog.text
    else:
        assert import_errors == {}


def test_core_plugin_manager_passes_compatibility_policy(monkeypatch, tmp_path):
    monkeypatch.setattr(plugins_manager.settings, "PLUGINS_FOLDER", str(tmp_path))
    with (
        mock.patch.object(
            plugins_manager, "_load_entrypoint_plugins", autospec=True, return_value=([], {})
        ) as load,
        mock.patch.object(
            plugins_manager, "_load_plugins_from_plugin_directory", autospec=True, return_value=([], {})
        ),
        mock.patch.object(plugins_manager, "_load_providers_plugins", autospec=True, return_value=([], {})),
    ):
        plugins_manager._get_plugins.__wrapped__()

    load.assert_called_once_with(provider_incompatibility_reason)
