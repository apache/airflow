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
    "hide_paused_dags_by_default": False,
    "instance_name": "Airflow",
    "instance_name_has_markup": False,
    "enable_swagger_ui": True,
    "require_confirmation_dag_change": False,
    "default_wrap": False,
    "warn_deployment_exposure": False,
    "audit_view_excluded_events": "",
    "audit_view_included_events": "",
    "test_connection": "Disabled",
    "dashboard_alert": [],
}


@pytest.fixture
def mock_config_data():
    """
    Mock configuration settings used in the endpoint.
    """
    with conf_vars(
        {
            ("webserver", "page_size"): "100",
            ("webserver", "auto_refresh_interval"): "3",
            ("webserver", "hide_paused_dags_by_default"): "false",
            ("webserver", "instance_name"): "Airflow",
            ("webserver", "instance_name_has_markup"): "false",
            ("webserver", "enable_swagger_ui"): "true",
            ("webserver", "require_confirmation_dag_change"): "false",
            ("webserver", "default_wrap"): "false",
            ("webserver", "audit_view_excluded_events"): "",
            ("webserver", "audit_view_included_events"): "",
            ("webserver", "warn_deployment_exposure"): "false",
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
        assert response.json() == mock_config_response

    def test_get_config_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/config")
        assert response.status_code == 401

    def test_get_config_just_authenticated(self, mock_config_data, unauthorized_test_client):
        """Just being authenticated is enough to access the endpoint."""
        response = unauthorized_test_client.get("/config")
        assert response.status_code == 200
        assert response.json() == mock_config_response
