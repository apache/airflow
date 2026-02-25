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

import contextlib
import importlib
import inspect
import logging
import os
import sys
from unittest import mock

import pytest

from airflow._shared.module_loading import qualname
from airflow.configuration import conf
from airflow.listeners.listener import get_listener_manager
from airflow.plugins_manager import AirflowPlugin

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


@pytest.fixture
def mock_metadata_distribution(mocker):
    @contextlib.contextmanager
    def wrapper(*args, **kwargs):
        if sys.version_info < (3, 12):
            patch_fq = "importlib_metadata.distributions"
        else:
            patch_fq = "importlib.metadata.distributions"

        with mock.patch(patch_fq, *args, **kwargs) as m:
            yield m

    return wrapper


class TestPluginsManager:
    @pytest.fixture(autouse=True)
    def clean_plugins(self):
        from airflow import plugins_manager

        plugins_manager._get_plugins.cache_clear()

    def test_no_log_when_no_plugins(self, caplog):
        with mock_plugin_manager(plugins=[]):
            from airflow import plugins_manager

            plugins_manager.ensure_plugins_loaded()

        assert caplog.record_tuples == []

    def test_loads_filesystem_plugins(self, caplog):
        from airflow import plugins_manager

        plugins, import_errors = plugins_manager._load_plugins_from_plugin_directory(
            plugins_folder=conf.get("core", "plugins_folder"),
            load_examples=conf.getboolean("core", "load_examples"),
            example_plugins_module="airflow.example_dags.plugins",
        )

        assert len(plugins) == 10
        assert not import_errors
        for plugin in plugins:
            if "AirflowTestOnLoadPlugin" in str(plugin):
                assert plugin.name == "preload"  # on_init() is not called here
                break
        else:
            pytest.fail("Wasn't able to find a registered `AirflowTestOnLoadPlugin`")

        assert caplog.record_tuples == []

    def test_loads_filesystem_plugins_exception(self, caplog, tmp_path):
        from airflow import plugins_manager

        (tmp_path / "testplugin.py").write_text(ON_LOAD_EXCEPTION_PLUGIN)

        with (
            conf_vars({("core", "plugins_folder"): os.fspath(tmp_path)}),
            mock.patch("airflow.plugins_manager._load_entrypoint_plugins", return_value=([], [])),
            mock.patch("airflow.plugins_manager._load_providers_plugins", return_value=([], [])),
        ):
            plugins, import_errors = plugins_manager._get_plugins()

        assert len(plugins) == 3  # three are loaded from examples
        assert len(import_errors) == 1

        received_logs = caplog.text
        assert "Failed to load plugin" in received_logs
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
        assert not import_errors

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

            external_views = [{"url_route": "/test_route"}, {"wrong_view": "/no_url_route"}]

        class TestPluginB(AirflowPlugin):
            name = "test_plugin_b"

            external_views = [{"url_route": "/test_route"}]
            react_apps = [{"url_route": "/test_route"}]

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

            external_views = [[{"nested_list": "/test_route"}], {"url_route": "/test_route"}]
            react_apps = [[{"nested_list": "/test_route"}], {"url_route": "/test_route_react_app"}]

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
        assert len(plugins) == 5


# ---------------------------------------------------------------------------
# Tests for Pydantic config models & validation helpers
# ---------------------------------------------------------------------------


class TestPluginConfigModels:
    """Tests for the strongly-typed Pydantic config models."""

    # --- ExternalViewConfig ---

    def test_external_view_config_valid_full(self):
        from airflow.plugins_manager import ExternalViewConfig

        cfg = ExternalViewConfig(
            name="My View",
            href="https://example.com",
            url_route="my_view",
            icon="https://example.com/icon.svg",
            icon_dark_mode="https://example.com/icon_dark.svg",
            category="browse",
            destination="nav",
        )
        assert cfg.name == "My View"
        assert cfg.href == "https://example.com"
        assert cfg.destination == "nav"

    def test_external_view_config_minimal(self):
        from airflow.plugins_manager import ExternalViewConfig

        cfg = ExternalViewConfig(name="Min View")
        assert cfg.name == "Min View"
        assert cfg.href is None
        assert cfg.destination == "nav"

    def test_external_view_config_custom_destination(self):
        from airflow.plugins_manager import ExternalViewConfig

        # Custom destination strings are accepted (validated at API response layer instead)
        cfg = ExternalViewConfig(name="Custom", destination="custom_place")
        assert cfg.destination == "custom_place"

    def test_external_view_config_missing_name(self):
        from pydantic import ValidationError

        from airflow.plugins_manager import ExternalViewConfig

        with pytest.raises(ValidationError, match="name"):
            ExternalViewConfig()

    def test_external_view_config_extra_fields(self):
        from airflow.plugins_manager import ExternalViewConfig

        cfg = ExternalViewConfig(name="Extra", custom_key="custom_value")
        assert cfg.name == "Extra"
        assert cfg.custom_key == "custom_value"

    # --- FastAPIAppConfig ---

    def test_fastapi_app_config_valid(self):
        from airflow.plugins_manager import FastAPIAppConfig

        fake_app = object()
        cfg = FastAPIAppConfig(app=fake_app, url_prefix="/api", name="My API")
        assert cfg.app is fake_app
        assert cfg.url_prefix == "/api"
        assert cfg.name == "My API"

    def test_fastapi_app_config_empty_prefix(self):
        from pydantic import ValidationError

        from airflow.plugins_manager import FastAPIAppConfig

        with pytest.raises(ValidationError, match="url_prefix"):
            FastAPIAppConfig(app=object(), url_prefix="", name="Bad")

    def test_fastapi_app_config_missing_fields(self):
        from pydantic import ValidationError

        from airflow.plugins_manager import FastAPIAppConfig

        with pytest.raises(ValidationError):
            FastAPIAppConfig()

    # --- AppBuilderViewConfig ---

    def test_appbuilder_view_config_all_optional(self):
        from airflow.plugins_manager import AppBuilderViewConfig

        cfg = AppBuilderViewConfig()
        assert cfg.name is None
        assert cfg.view is None

    def test_appbuilder_view_config_with_view(self):
        from airflow.plugins_manager import AppBuilderViewConfig

        fake_view = object()
        cfg = AppBuilderViewConfig(name="V", category="C", view=fake_view, label="L")
        assert cfg.view is fake_view
        assert cfg.label == "L"

    # --- AppBuilderMenuItemConfig ---

    def test_appbuilder_menu_item_config_valid(self):
        from airflow.plugins_manager import AppBuilderMenuItemConfig

        cfg = AppBuilderMenuItemConfig(name="Google", href="https://google.com")
        assert cfg.name == "Google"
        assert cfg.category is None

    def test_appbuilder_menu_item_config_missing_required(self):
        from pydantic import ValidationError

        from airflow.plugins_manager import AppBuilderMenuItemConfig

        with pytest.raises(ValidationError, match="href"):
            AppBuilderMenuItemConfig(name="No href")

    # --- FastAPIRootMiddlewareConfig ---

    def test_fastapi_middleware_config_valid(self):
        from airflow.plugins_manager import FastAPIRootMiddlewareConfig

        cfg = FastAPIRootMiddlewareConfig(middleware=lambda: None, name="MW")
        assert cfg.name == "MW"
        assert cfg.args == []
        assert cfg.kwargs == {}

    # --- ReactAppConfig ---

    def test_react_app_config_valid(self):
        from airflow.plugins_manager import ReactAppConfig

        cfg = ReactAppConfig(name="App", bundle_url="https://example.com/bundle.js")
        assert cfg.destination == "nav"

    def test_react_app_config_dashboard_destination(self):
        from airflow.plugins_manager import ReactAppConfig

        cfg = ReactAppConfig(name="App", destination="dashboard")
        assert cfg.destination == "dashboard"

    # --- Dict-to-model conversion via model_validate ---

    def test_external_view_from_dict(self):
        from airflow.plugins_manager import ExternalViewConfig

        d = {"name": "Dict View", "href": "https://example.com", "url_route": "test"}
        cfg = ExternalViewConfig.model_validate(d)
        assert cfg.name == "Dict View"
        assert isinstance(cfg, ExternalViewConfig)

    def test_fastapi_app_from_dict(self):
        from airflow.plugins_manager import FastAPIAppConfig

        app = object()
        d = {"app": app, "url_prefix": "/p", "name": "N"}
        cfg = FastAPIAppConfig.model_validate(d)
        assert cfg.app is app


class TestValidatePluginAttributes:
    """Tests for ``validate_plugin_attributes``."""

    def _make_plugin(self, **attrs):
        """Helper to create a minimal plugin-like object."""

        class FakePlugin:
            name = attrs.pop("name", "test_plugin")

        for k, v in attrs.items():
            setattr(FakePlugin, k, v)
        return FakePlugin

    def test_valid_dicts_converted(self):
        from airflow.plugins_manager import ExternalViewConfig, validate_plugin_attributes

        plugin = self._make_plugin(
            external_views=[{"name": "V", "href": "https://ex.com", "url_route": "v"}]
        )
        errors = validate_plugin_attributes(plugin)
        assert errors == []
        assert isinstance(plugin.external_views[0], ExternalViewConfig)

    def test_model_passthrough(self):
        from airflow.plugins_manager import ExternalViewConfig, validate_plugin_attributes

        model = ExternalViewConfig(name="V")
        plugin = self._make_plugin(external_views=[model])
        errors = validate_plugin_attributes(plugin)
        assert errors == []
        assert plugin.external_views[0] is model  # same object

    def test_invalid_dict_collected_as_error(self):
        from airflow.plugins_manager import validate_plugin_attributes

        # FastAPIAppConfig requires app, url_prefix, name
        plugin = self._make_plugin(fastapi_apps=[{"name": "only name"}])
        errors = validate_plugin_attributes(plugin)
        assert len(errors) == 1
        assert "fastapi_apps[0]" in errors[0]
        # The invalid dict should be kept as-is
        assert isinstance(plugin.fastapi_apps[0], dict)

    def test_unexpected_type_warning(self):
        from airflow.plugins_manager import validate_plugin_attributes

        plugin = self._make_plugin(external_views=["not a dict"])
        errors = validate_plugin_attributes(plugin)
        assert len(errors) == 1
        assert "unexpected type" in errors[0]
        assert "str" in errors[0]

    def test_mixed_valid_and_invalid(self):
        from airflow.plugins_manager import ExternalViewConfig, validate_plugin_attributes

        plugin = self._make_plugin(
            external_views=[
                {"name": "Good"},
                12345,  # bad type
            ]
        )
        errors = validate_plugin_attributes(plugin)
        assert len(errors) == 1
        assert isinstance(plugin.external_views[0], ExternalViewConfig)
        assert plugin.external_views[1] == 12345

    def test_empty_lists_no_error(self):
        from airflow.plugins_manager import validate_plugin_attributes

        plugin = self._make_plugin(
            external_views=[],
            fastapi_apps=[],
            appbuilder_views=[],
        )
        errors = validate_plugin_attributes(plugin)
        assert errors == []

    def test_none_attribute_skipped(self):
        from airflow.plugins_manager import validate_plugin_attributes

        plugin = self._make_plugin()
        plugin.external_views = None
        errors = validate_plugin_attributes(plugin)
        assert errors == []

    def test_extra_fields_preserved(self):
        from airflow.plugins_manager import ExternalViewConfig, validate_plugin_attributes

        plugin = self._make_plugin(
            external_views=[{"name": "V", "my_custom_field": 42}]
        )
        errors = validate_plugin_attributes(plugin)
        assert errors == []
        assert isinstance(plugin.external_views[0], ExternalViewConfig)
        assert plugin.external_views[0].my_custom_field == 42

    def test_backward_compat_real_world_data(self):
        """Validate against the fixture data used in test_should_display_one_plugin."""
        from airflow.plugins_manager import validate_plugin_attributes

        plugin = self._make_plugin(
            external_views=[
                {
                    "destination": "nav",
                    "icon": "https://example.com/icon.svg",
                    "name": "Test IFrame",
                    "href": "https://airflow.apache.org/",
                    "url_route": "test_iframe",
                    "category": "browse",
                }
            ],
            fastapi_apps=[
                {
                    "app": object(),  # stand-in for FastAPI()
                    "url_prefix": "/some_prefix",
                    "name": "Name of the App",
                }
            ],
            appbuilder_views=[
                {
                    "name": "Test View",
                    "category": "Test Plugin",
                    "label": "Test Label",
                    "view": object(),  # stand-in for BaseView
                }
            ],
            appbuilder_menu_items=[
                {
                    "name": "Google",
                    "href": "https://www.google.com",
                    "category": "Search",
                }
            ],
            react_apps=[
                {
                    "name": "Test React App",
                    "bundle_url": "https://example.com/bundle.js",
                    "icon": "https://example.com/icon.svg",
                    "url_route": "test_react",
                    "destination": "nav",
                    "category": "browse",
                }
            ],
        )
        errors = validate_plugin_attributes(plugin)
        assert errors == [], f"Backward compatibility failure: {errors}"


class TestGetAttrAndToDict:
    """Tests for ``_get_attr`` and ``_to_dict`` helpers."""

    def test_get_attr_from_dict(self):
        from airflow.plugins_manager import _get_attr

        assert _get_attr({"key": "val"}, "key") == "val"
        assert _get_attr({"key": "val"}, "missing", "default") == "default"

    def test_get_attr_from_model(self):
        from airflow.plugins_manager import ExternalViewConfig, _get_attr

        cfg = ExternalViewConfig(name="V")
        assert _get_attr(cfg, "name") == "V"
        assert _get_attr(cfg, "nonexistent", "fallback") == "fallback"

    def test_to_dict_from_dict(self):
        from airflow.plugins_manager import _to_dict

        d = {"a": 1, "b": 2}
        assert _to_dict(d) is d

    def test_to_dict_from_model(self):
        from airflow.plugins_manager import ExternalViewConfig, _to_dict

        cfg = ExternalViewConfig(name="V", href="https://test.com")
        result = _to_dict(cfg)
        assert isinstance(result, dict)
        assert result["name"] == "V"
        assert result["href"] == "https://test.com"

    def test_to_dict_fallback(self):
        from airflow.plugins_manager import _to_dict

        assert _to_dict(42) == {}
