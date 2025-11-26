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

from tests_common.test_utils.asserts import assert_queries_count
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
        with assert_queries_count(0):
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
