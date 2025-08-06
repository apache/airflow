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

import pytest

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

mock_config_response = {
    "page_size": 100,
    "auto_refresh_interval": 3,
    "hide_paused_dags_by_default": True,
    "instance_name": "Airflow",
    "enable_swagger_ui": True,
    "require_confirmation_dag_change": False,
    "default_wrap": False,
    "test_connection": "Disabled",
    "dashboard_alert": [],
    "show_external_log_redirect": False,
    "external_log_name": None,
    "plugins_extra_menu_items": [],
    "plugin_import_errors": [],
}


@pytest.fixture
def mock_config_data():
    """
    Mock configuration settings used in the endpoint.
    """
    with conf_vars(
        {
            ("api", "instance_name"): "Airflow",
            ("api", "enable_swagger_ui"): "true",
            ("api", "hide_paused_dags_by_default"): "true",
            ("api", "page_size"): "100",
            ("api", "default_wrap"): "false",
            ("api", "auto_refresh_interval"): "3",
            ("api", "require_confirmation_dag_change"): "false",
        }
    ):
        yield


class TestGetConfig:
    def test_should_response_200(self, mock_config_data, test_client):
        """
        Test the /config endpoint to verify response matches mock data.
        """
        response = test_client.get("/config")

        assert response.status_code == 200
        response_json = response.json()

        # Check that all expected fields are present
        for key in mock_config_response:
            assert key in response_json, f"Missing key: {key}"

        # Check core configuration values
        assert response_json["page_size"] == 100
        assert response_json["auto_refresh_interval"] == 3
        assert response_json["hide_paused_dags_by_default"] is True
        assert response_json["instance_name"] == "Airflow"
        assert response_json["enable_swagger_ui"] is True
        assert response_json["require_confirmation_dag_change"] is False
        assert response_json["default_wrap"] is False
        assert response_json["test_connection"] == "Disabled"
        assert response_json["dashboard_alert"] == []
        assert response_json["show_external_log_redirect"] is False
        assert response_json["external_log_name"] is None

        # Check plugin-related fields (these may vary)
        assert "plugins_extra_menu_items" in response_json
        assert "plugin_import_errors" in response_json
        assert isinstance(response_json["plugins_extra_menu_items"], list)
        assert isinstance(response_json["plugin_import_errors"], list)

    def test_get_config_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/config")
        assert response.status_code == 401

    def test_get_config_just_authenticated(self, mock_config_data, unauthorized_test_client):
        """Just being authenticated is enough to access the endpoint."""
        response = unauthorized_test_client.get("/config")
        assert response.status_code == 200
        response_json = response.json()

        # Verify core configuration fields are present and correct
        assert response_json["page_size"] == 100
        assert response_json["instance_name"] == "Airflow"
        assert "plugins_extra_menu_items" in response_json
        assert "plugin_import_errors" in response_json

    def test_config_response_structure(self, mock_config_data, test_client):
        """Test that the config response has the expected structure."""
        response = test_client.get("/config")
        assert response.status_code == 200

        response_json = response.json()

        # Verify all required fields from ConfigResponse model are present
        required_fields = [
            "page_size",
            "auto_refresh_interval",
            "hide_paused_dags_by_default",
            "instance_name",
            "enable_swagger_ui",
            "require_confirmation_dag_change",
            "default_wrap",
            "test_connection",
            "dashboard_alert",
            "show_external_log_redirect",
            "external_log_name",
            "plugins_extra_menu_items",
            "plugin_import_errors",
        ]

        for field in required_fields:
            assert field in response_json, f"Required field '{field}' missing from response"

        # Verify field types
        assert isinstance(response_json["page_size"], int)
        assert isinstance(response_json["auto_refresh_interval"], int)
        assert isinstance(response_json["hide_paused_dags_by_default"], bool)
        assert isinstance(response_json["instance_name"], str)
        assert isinstance(response_json["enable_swagger_ui"], bool)
        assert isinstance(response_json["require_confirmation_dag_change"], bool)
        assert isinstance(response_json["default_wrap"], bool)
        assert isinstance(response_json["test_connection"], str)
        assert isinstance(response_json["dashboard_alert"], list)
        assert isinstance(response_json["show_external_log_redirect"], bool)
        assert isinstance(response_json["plugins_extra_menu_items"], list)
        assert isinstance(response_json["plugin_import_errors"], list)

        # external_log_name can be None or str
        assert response_json["external_log_name"] is None or isinstance(
            response_json["external_log_name"], str
        )

    def test_config_with_custom_settings(self, test_client):
        """Test config endpoint with custom configuration values."""
        custom_config = {
            ("api", "instance_name"): "CustomAirflow",
            ("api", "enable_swagger_ui"): "false",
            ("api", "hide_paused_dags_by_default"): "false",
            ("api", "page_size"): "50",
            ("api", "default_wrap"): "true",
            ("api", "auto_refresh_interval"): "5",
            ("api", "require_confirmation_dag_change"): "true",
        }

        with conf_vars(custom_config):
            response = test_client.get("/config")

            assert response.status_code == 200
            response_json = response.json()

            # Verify custom configuration values
            assert response_json["instance_name"] == "CustomAirflow"
            assert response_json["enable_swagger_ui"] is False
            assert response_json["hide_paused_dags_by_default"] is False
            assert response_json["page_size"] == 50
            assert response_json["default_wrap"] is True
            assert response_json["auto_refresh_interval"] == 5
            assert response_json["require_confirmation_dag_change"] is True
