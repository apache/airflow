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

from unittest import mock

import pytest

from airflow.sdk import plugins_manager
from airflow.sdk.plugins_manager import AirflowPlugin

# ``_disable_ol_plugin`` in ``task-sdk/tests/conftest.py`` is a session-scoped autouse fixture that
# replaces ``_get_plugins`` with a stub returning no plugins. Capture the real, ``@cache``-wrapped
# function here at collection time — before any fixture runs — so these tests can put it back.
_REAL_GET_PLUGINS = plugins_manager._get_plugins


class TestGetPlugins:
    """Mirrors ``TestPluginsManager`` in ``airflow-core/tests/unit/plugins/test_plugins_manager.py``."""

    @pytest.fixture(autouse=True)
    def _use_real_get_plugins(self, monkeypatch):
        monkeypatch.setattr(plugins_manager, "_get_plugins", _REAL_GET_PLUGINS)
        _REAL_GET_PLUGINS.cache_clear()
        yield
        _REAL_GET_PLUGINS.cache_clear()

    def test_duplicate_plugin_name_does_not_prevent_loading_subsequent_plugins(self):
        """
        A duplicate name must skip that one plugin, not abandon the rest of the batch.

        ``__register_plugins`` used to ``return`` on the first duplicate, so every plugin
        enumerated after it was dropped without an error. Here ``plugin_b`` is loaded from
        the plugins directory and again from an entry point; ``plugin_c`` follows the
        duplicate in that second batch and is the one that used to disappear.
        """

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
            mock.patch.object(plugins_manager.settings, "PLUGINS_FOLDER", "/dev/null"),
            mock.patch.object(plugins_manager.settings, "LAZY_LOAD_PROVIDERS", False),
            mock.patch.object(
                plugins_manager,
                "_load_plugins_from_plugin_directory",
                return_value=([plugin_a, plugin_b], {}),
            ),
            mock.patch.object(
                plugins_manager, "_load_entrypoint_plugins", return_value=([plugin_b_dup, plugin_c], {})
            ),
            mock.patch.object(plugins_manager, "_load_providers_plugins", return_value=([], {})),
        ):
            plugins, _ = plugins_manager._get_plugins()

        plugin_names = [plugin.name for plugin in plugins]
        assert "plugin_a" in plugin_names
        assert "plugin_b" in plugin_names
        assert "plugin_c" in plugin_names
        assert len(plugins) == 3

    def test_duplicate_plugin_name_is_reported_as_import_error(self):
        """The skipped duplicate is surfaced to the caller rather than dropped silently."""

        class PluginA(AirflowPlugin):
            name = "plugin_a"

        class PluginADuplicateName(AirflowPlugin):
            name = "plugin_a"

        plugin_a = PluginA()
        plugin_a_dup = PluginADuplicateName()

        with (
            mock.patch.object(plugins_manager.settings, "PLUGINS_FOLDER", "/dev/null"),
            mock.patch.object(plugins_manager.settings, "LAZY_LOAD_PROVIDERS", False),
            mock.patch.object(
                plugins_manager, "_load_plugins_from_plugin_directory", return_value=([plugin_a], {})
            ),
            mock.patch.object(plugins_manager, "_load_entrypoint_plugins", return_value=([plugin_a_dup], {})),
            mock.patch.object(plugins_manager, "_load_providers_plugins", return_value=([], {})),
        ):
            plugins, import_errors = plugins_manager._get_plugins()

        assert [plugin.name for plugin in plugins] == ["plugin_a"]
        assert len(import_errors) == 1
