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

from unittest.mock import patch

import pytest

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = pytest.mark.db_test


@skip_if_force_lowest_dependencies_marker
class TestGetPlugins:
    @pytest.mark.parametrize(
        ("query_params", "expected_total_entries", "expected_names"),
        [
            # Filters
            (
                {},
                13,
                [
                    "MetadataCollectionPlugin",
                    "OpenLineageProviderPlugin",
                    "databricks_workflow",
                    "decreasing_priority_weight_strategy_plugin",
                    "edge_executor",
                    "hive",
                    "plugin-a",
                    "plugin-b",
                    "plugin-c",
                    "postload",
                    "priority_weight_strategy_plugin",
                    "test_plugin",
                    "workday_timetable_plugin",
                ],
            ),
            (
                {"limit": 3, "offset": 2},
                13,
                ["databricks_workflow", "decreasing_priority_weight_strategy_plugin", "edge_executor"],
            ),
            ({"limit": 1}, 13, ["MetadataCollectionPlugin"]),
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
                "destination": "nav",
                "category": "browse",
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
                        "url_route": None,
                    },
                    {
                        "category": None,
                        "destination": "nav",
                        "href": "https://www.apache.org/",
                        "icon": None,
                        "icon_dark_mode": None,
                        "label": "The Apache Software Foundation",
                        "name": "apache",
                        "url_route": None,
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

        response = test_client.get("/plugins", params={"limit": 5, "offset": 9})
        assert response.status_code == 200

        body = response.json()
        plugins_page = body["plugins"]

        # Even though limit=5, only 4 valid plugins should come back
        assert len(plugins_page) == 4
        assert "test_plugin_invalid" not in [p["name"] for p in plugins_page]

        assert body["total_entries"] == 13


@skip_if_force_lowest_dependencies_marker
class TestGetPluginImportErrors:
    @patch(
        "airflow.plugins_manager.import_errors",
        new={"plugins/test_plugin.py": "something went wrong"},
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
