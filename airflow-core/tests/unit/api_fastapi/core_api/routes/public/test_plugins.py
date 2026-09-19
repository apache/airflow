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
from unittest.mock import patch

import pytest

from airflow.plugins_manager import AirflowPlugin

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker
from tests_common.test_utils.mock_plugins import mock_plugin_manager

pytestmark = pytest.mark.db_test


@skip_if_force_lowest_dependencies_marker
class TestGetPlugins:
    @pytest.mark.parametrize(
        ("query_params", "expected_total_entries", "expected_names"),
        [
            # Filters
            (
                {},
                19,
                [
                    "InformaticaProviderPlugin",
                    "MetadataCollectionPlugin",
                    "OpenLineageProviderPlugin",
                    "business_day_window_plugin",
                    "databricks_workflow",
                    "decreasing_priority_weight_strategy_plugin",
                    "edge_executor",
                    "hitl_review",
                    "hive",
                    "kafka_event_producer",
                    "plugin-a",
                    "plugin-b",
                    "plugin-c",
                    "postload",
                    "prefix_strip_mapper_plugin",
                    "priority_weight_strategy_plugin",
                    "scheduled_runtime_partition_timetable_plugin",
                    "test_plugin",
                    "workday_timetable_plugin",
                ],
            ),
            (
                {"limit": 3, "offset": 3},
                19,
                [
                    "business_day_window_plugin",
                    "databricks_workflow",
                    "decreasing_priority_weight_strategy_plugin",
                ],
            ),
            ({"limit": 1}, 19, ["InformaticaProviderPlugin"]),
        ],
    )
    def test_should_respond_200(
        self, test_client, session, query_params, expected_total_entries, expected_names
    ):
        with assert_queries_count(2):
            response = test_client.get("/plugins", params=query_params)
        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [plugin["name"] for plugin in body["plugins"]] == expected_names

    def test_external_views_model_validator(self, test_client):
        with assert_queries_count(2):
            response = test_client.get("plugins")
        body = response.json()

        test_plugin = next((plugin for plugin in body["plugins"] if plugin["name"] == "test_plugin"), None)
        assert test_plugin is not None

        # Base external_view that is always present
        expected_views = [
            {
                "name": "Test IFrame Airflow Docs",
                "href": "https://airflow.apache.org/",
                "icon": "https://raw.githubusercontent.com/lucide-icons/lucide/refs/heads/main/icons/plug.svg",
                "icon_dark_mode": None,
                "url_route": "test_iframe_plugin",
                "destination": "dag",
                "category": "browse",
                "nav_top_level": False,
                "applies_to": {
                    "dag_tags": ["ml", "production"],
                    "dag_ids": ["example_dag"],
                    "task_ids": None,
                    "operators": None,
                    "operator_names": None,
                },
            },
        ]

        # The test plugin conditionally defines appbuilder_menu_items based on flask_appbuilder availability
        try:
            import flask_appbuilder  # noqa: F401

            expected_views.extend(
                [
                    {
                        "category": "Search",
                        "destination": "nav",
                        "href": "https://www.google.com",
                        "icon": None,
                        "icon_dark_mode": None,
                        "name": "Google",
                        "nav_top_level": False,
                        "url_route": None,
                        "applies_to": None,
                    },
                    {
                        "category": None,
                        "destination": "nav",
                        "href": "https://www.apache.org/",
                        "icon": None,
                        "icon_dark_mode": None,
                        "label": "The Apache Software Foundation",
                        "name": "apache",
                        "nav_top_level": False,
                        "url_route": None,
                        "applies_to": None,
                    },
                ]
            )
        except ImportError:
            pass

        assert test_plugin["external_views"] == expected_views

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/plugins")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/plugins")
        assert response.status_code == 403

    def test_applies_to_defaults_to_none_when_omitted(self):
        from airflow.api_fastapi.core_api.datamodels.plugins import ExternalViewResponse

        view = ExternalViewResponse(name="No Filter", href="https://example.com/")

        assert view.applies_to is None

    def test_applies_to_parses_nested_criteria(self):
        from airflow.api_fastapi.core_api.datamodels.plugins import ExternalViewResponse

        view = ExternalViewResponse(
            name="Scoped",
            href="https://example.com/",
            applies_to={
                "dag_tags": ["ml"],
                "operators": ["KubernetesPodOperator"],
                "operator_names": ["@task.bash"],
            },
        )

        assert view.applies_to is not None
        assert view.applies_to.dag_tags == ["ml"]
        assert view.applies_to.operators == ["KubernetesPodOperator"]
        assert view.applies_to.operator_names == ["@task.bash"]
        assert view.applies_to.dag_ids is None
        assert view.applies_to.task_ids is None

    def test_applies_to_rejects_unknown_criteria(self):
        from pydantic import ValidationError

        from airflow.api_fastapi.core_api.datamodels.plugins import ExternalViewResponse

        with pytest.raises(ValidationError):
            ExternalViewResponse(
                name="Scoped",
                href="https://example.com/",
                applies_to={"dag_tag": ["ml"]},
            )

    def test_invalid_external_view_destination_should_log_warning_and_continue(self, test_client, caplog):
        caplog.set_level("WARNING", "airflow.api_fastapi.core_api.routes.public.plugins")

        response = test_client.get("/plugins")
        assert response.status_code == 200

        body = response.json()
        plugin_names = [plugin["name"] for plugin in body["plugins"]]

        # Ensure our invalid plugin is skipped from the valid list
        assert "test_plugin_invalid" not in plugin_names

        # Verify warning was logged
        assert any("Skipping invalid plugin due to error" in rec.message for rec in caplog.records)

        response = test_client.get("/plugins", params={"limit": 8, "offset": 9})
        assert response.status_code == 200

        body = response.json()
        plugins_page = body["plugins"]

        # Invalid plugins are filtered before pagination, so the page is full.
        assert len(plugins_page) == 8
        assert "test_plugin_invalid" not in [p["name"] for p in plugins_page]

        assert body["total_entries"] == 19


@skip_if_force_lowest_dependencies_marker
class TestGetPluginImportErrors:
    @patch(
        "airflow.plugins_manager.get_import_errors",
        new=lambda: {"plugins/test_plugin.py": "something went wrong"},
    )
    def test_should_respond_200(self, test_client, session):
        with assert_queries_count(2):
            response = test_client.get("/plugins/importErrors")
        assert response.status_code == 200

        body = response.json()
        assert body == {
            "import_errors": [
                {
                    "source": "plugins/test_plugin.py",
                    "error": "something went wrong",
                }
            ],
            "total_entries": 1,
        }

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/plugins/importErrors")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/plugins/importErrors")
        assert response.status_code == 403


class _GlobalPlugin(AirflowPlugin):
    name = "global_plugin"


class _TeamAPlugin(AirflowPlugin):
    name = "team_a_plugin"
    team_name = "team_a"


class _TeamBPlugin(AirflowPlugin):
    name = "team_b_plugin"
    team_name = "team_b"


@skip_if_force_lowest_dependencies_marker
class TestGetPluginsTeamFiltering:
    _plugins = [_GlobalPlugin(), _TeamAPlugin(), _TeamBPlugin()]

    def test_no_filtering_when_multi_team_disabled(self, test_client):
        """With multi_team off, all plugins are returned regardless of team_name."""
        with mock_plugin_manager(plugins=self._plugins):
            response = test_client.get("/plugins")

        assert response.status_code == 200
        names = {plugin["name"] for plugin in response.json()["plugins"]}
        assert names == {"global_plugin", "team_a_plugin", "team_b_plugin"}

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.api_fastapi.core_api.routes.public.plugins.get_auth_manager")
    def test_filters_to_authorized_teams_and_global(self, mock_get_auth_manager, test_client):
        """A user authorized for team_a sees global + team_a plugins, but not team_b."""
        mock_get_auth_manager.return_value.get_authorized_teams.return_value = {"team_a"}

        with mock_plugin_manager(plugins=self._plugins):
            response = test_client.get("/plugins")

        assert response.status_code == 200
        body = response.json()
        names = {plugin["name"] for plugin in body["plugins"]}
        assert names == {"global_plugin", "team_a_plugin"}
        assert body["total_entries"] == 2

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.api_fastapi.core_api.routes.public.plugins.get_auth_manager")
    def test_user_with_no_teams_sees_only_global(self, mock_get_auth_manager, test_client):
        mock_get_auth_manager.return_value.get_authorized_teams.return_value = set()

        with mock_plugin_manager(plugins=self._plugins):
            response = test_client.get("/plugins")

        assert response.status_code == 200
        body = response.json()
        names = {plugin["name"] for plugin in body["plugins"]}
        assert names == {"global_plugin"}
        assert body["total_entries"] == 1

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.api_fastapi.core_api.routes.public.plugins.get_auth_manager")
    def test_team_name_present_in_response(self, mock_get_auth_manager, test_client):
        mock_get_auth_manager.return_value.get_authorized_teams.return_value = {"team_a"}

        with mock_plugin_manager(plugins=self._plugins):
            response = test_client.get("/plugins")

        team_name_by_plugin = {p["name"]: p["team_name"] for p in response.json()["plugins"]}
        assert team_name_by_plugin == {"global_plugin": None, "team_a_plugin": "team_a"}


def _make_view_plugin(name: str, team_name: str | None) -> AirflowPlugin:
    """Build a plugin instance carrying one external view and one react app.

    Uses per-instance lists (not class attributes) so the dedup/validation pass in
    ``_get_ui_plugins`` cannot mutate shared state across tests.
    """
    plugin = AirflowPlugin()
    plugin.name = name
    plugin.team_name = team_name
    plugin.external_views = [
        {
            "name": f"{name} view",
            "href": f"https://example.com/{name}",
            "url_route": f"{name}_view",
            "destination": "nav",
        }
    ]
    plugin.react_apps = [
        {
            "name": f"{name} app",
            "bundle_url": f"https://example.com/{name}.js",
            "url_route": f"{name}_app",
            "destination": "nav",
        }
    ]
    return plugin


@skip_if_force_lowest_dependencies_marker
class TestGetPluginsUIViewTeamFiltering:
    """The React UI builds its nav items, external-view tabs, and react apps from the
    ``external_views``/``react_apps`` nested in the /api/v2/plugins response. Because a
    team-scoped plugin's whole entry is filtered out for non-team users, its UI views
    and apps must be absent from the response as well.
    """

    def _plugins(self) -> list[AirflowPlugin]:
        return [
            _make_view_plugin("global_view_plugin", None),
            _make_view_plugin("team_a_view_plugin", "team_a"),
            _make_view_plugin("team_b_view_plugin", "team_b"),
        ]

    @staticmethod
    def _view_routes(plugins: list[dict]) -> set[str]:
        return {view["url_route"] for plugin in plugins for view in plugin["external_views"]}

    @staticmethod
    def _app_routes(plugins: list[dict]) -> set[str]:
        return {app["url_route"] for plugin in plugins for app in plugin["react_apps"]}

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.api_fastapi.core_api.routes.public.plugins.get_auth_manager")
    def test_views_and_apps_scoped_to_authorized_teams(self, mock_get_auth_manager, test_client):
        """A team_a user sees global + team_a views/apps, but never team_b's."""
        mock_get_auth_manager.return_value.get_authorized_teams.return_value = {"team_a"}

        with mock_plugin_manager(plugins=self._plugins()):
            response = test_client.get("/plugins")

        assert response.status_code == 200
        plugins = response.json()["plugins"]

        view_routes = self._view_routes(plugins)
        assert "global_view_plugin_view" in view_routes
        assert "team_a_view_plugin_view" in view_routes
        assert "team_b_view_plugin_view" not in view_routes

        app_routes = self._app_routes(plugins)
        assert "global_view_plugin_app" in app_routes
        assert "team_a_view_plugin_app" in app_routes
        assert "team_b_view_plugin_app" not in app_routes

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch("airflow.api_fastapi.core_api.routes.public.plugins.get_auth_manager")
    def test_user_with_no_teams_sees_only_global_views_and_apps(self, mock_get_auth_manager, test_client):
        mock_get_auth_manager.return_value.get_authorized_teams.return_value = set()

        with mock_plugin_manager(plugins=self._plugins()):
            response = test_client.get("/plugins")

        assert response.status_code == 200
        plugins = response.json()["plugins"]
        assert self._view_routes(plugins) == {"global_view_plugin_view"}
        assert self._app_routes(plugins) == {"global_view_plugin_app"}

    def test_no_view_filtering_when_multi_team_disabled(self, test_client):
        """With multi_team off, every plugin's views/apps are returned."""
        with mock_plugin_manager(plugins=self._plugins()):
            response = test_client.get("/plugins")

        assert response.status_code == 200
        plugins = response.json()["plugins"]
        assert {
            "global_view_plugin_view",
            "team_a_view_plugin_view",
            "team_b_view_plugin_view",
        } <= self._view_routes(plugins)
        assert {
            "global_view_plugin_app",
            "team_a_view_plugin_app",
            "team_b_view_plugin_app",
        } <= self._app_routes(plugins)
