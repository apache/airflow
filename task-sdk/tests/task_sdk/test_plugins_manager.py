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

from airflow.listeners import hookimpl
from airflow.sdk._shared.listeners.listener import ListenerManager
from airflow.sdk.plugins_manager import AirflowPlugin, integrate_listener_plugins

from tests_common.test_utils.mock_plugins import mock_plugin_manager


class _RecordingListener:
    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance):
        pass


def _make_plugin(name: str, team_name: str | None, listener: object) -> AirflowPlugin:
    plugin = AirflowPlugin()
    plugin.name = name
    plugin.team_name = team_name
    plugin.listeners = [listener]
    return plugin


class TestIntegrateListenerPluginsTeamFiltering:
    """The worker always scopes by the server-provided team, independent of any
    local ``core.multi_team`` configuration."""

    def test_team_scoped_manager_excludes_other_teams(self):
        global_listener = _RecordingListener()
        team_a_listener = _RecordingListener()
        team_b_listener = _RecordingListener()
        plugins = [
            _make_plugin("global_plugin", None, global_listener),
            _make_plugin("team_a_plugin", "team_a", team_a_listener),
            _make_plugin("team_b_plugin", "team_b", team_b_listener),
        ]
        manager = ListenerManager()
        with mock_plugin_manager(plugins=plugins):
            integrate_listener_plugins(manager, team_name="team_a")

        registered = set(manager.pm.get_plugins())
        assert global_listener in registered
        assert team_a_listener in registered
        assert team_b_listener not in registered

    def test_teamless_manager_gets_only_global(self):
        global_listener = _RecordingListener()
        team_a_listener = _RecordingListener()
        plugins = [
            _make_plugin("global_plugin", None, global_listener),
            _make_plugin("team_a_plugin", "team_a", team_a_listener),
        ]
        manager = ListenerManager()
        with mock_plugin_manager(plugins=plugins):
            integrate_listener_plugins(manager, team_name=None)

        registered = set(manager.pm.get_plugins())
        assert global_listener in registered
        assert team_a_listener not in registered
