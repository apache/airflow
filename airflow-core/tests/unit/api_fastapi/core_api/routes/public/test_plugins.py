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

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = pytest.mark.db_test


@skip_if_force_lowest_dependencies_marker
class TestGetPlugins:
    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_names",
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
        response = test_client.get("/plugins", params=query_params)
        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [plugin["name"] for plugin in body["plugins"]] == expected_names

    def test_external_views_model_validator(self, test_client):
        response = test_client.get("plugins")
        body = response.json()

        test_plugin = next((plugin for plugin in body["plugins"] if plugin["name"] == "test_plugin"), None)
        assert test_plugin is not None
        assert test_plugin["external_views"] == [
            # external_views
            {
                "name": "Test IFrame Airflow Docs",
                "href": "https://airflow.apache.org/",
                "icon": "https://raw.githubusercontent.com/lucide-icons/lucide/refs/heads/main/icons/plug.svg",
                "icon_dark_mode": None,
                "url_route": "test_iframe_plugin",
                "destination": "nav",
                "category": "browse",
            },
            # appbuilder_menu_items
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

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/plugins")
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/plugins")
        assert response.status_code == 403


@skip_if_force_lowest_dependencies_marker
class TestGetPluginImportErrors:
    @patch(
        "airflow.plugins_manager.import_errors",
        new={"plugins/test_plugin.py": "something went wrong"},
    )
    def test_should_respond_200(self, test_client, session):
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
