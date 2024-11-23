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

mock_config_response = {
    "navbar_color": "#fff",
    "navbar_text_color": "#51504f",
    "navbar_hover_color": "#eee",
    "navbar_text_hover_color": "#51504f",
    "navbar_logo_text_color": "#51504f",
    "page_size": 100,
    "auto_refresh_interval": 3,
    "default_ui_timezone": "UTC",
    "hide_paused_dags_by_default": False,
    "instance_name": "Airflow",
    "instance_name_has_markup": False,
    "enable_swagger_ui": True,
    "require_confirmation_dag_change": False,
    "default_wrap": False,
    "warn_deployment_exposure": False,
    "audit_view_excluded_events": "",
    "audit_view_included_events": "",
    "is_k8s": False,
    "test_connection": "Disabled",
    "state_color_mapping": {
        "deferred": "mediumpurple",
        "failed": "red",
        "queued": "gray",
        "removed": "lightgrey",
        "restarting": "violet",
        "running": "lime",
        "scheduled": "tan",
        "skipped": "hotpink",
        "success": "green",
        "up_for_reschedule": "turquoise",
        "up_for_retry": "gold",
        "upstream_failed": "orange",
    },
}


@pytest.fixture
def mock_config_data():
    """
    Mock configuration settings used in the endpoint.
    """
    with patch("airflow.configuration.conf.as_dict") as mock_conf:
        mock_conf.return_value = {
            "webserver": {
                "navbar_color": "#fff",
                "navbar_text_color": "#51504f",
                "navbar_hover_color": "#eee",
                "navbar_text_hover_color": "#51504f",
                "navbar_logo_text_color": "#51504f",
                "page_size": "100",
                "auto_refresh_interval": "3",
                "default_ui_timezone": "UTC",
                "hide_paused_dags_by_default": "false",
                "instance_name": "Airflow",
                "instance_name_has_markup": "false",
                "enable_swagger_ui": "true",
                "require_confirmation_dag_change": "false",
                "default_wrap": "false",
                "warn_deployment_exposure": "false",
                "audit_view_excluded_events": "",
                "audit_view_included_events": "",
            }
        }
        with patch(
            "airflow.settings.STATE_COLORS",
            {
                "deferred": "mediumpurple",
                "failed": "red",
                "queued": "gray",
                "removed": "lightgrey",
                "restarting": "violet",
                "running": "lime",
                "scheduled": "tan",
                "skipped": "hotpink",
                "success": "green",
                "up_for_reschedule": "turquoise",
                "up_for_retry": "gold",
                "upstream_failed": "orange",
            },
        ):
            yield mock_conf


def test_get_configs_basic(mock_config_data, test_client):
    """
    Test the /ui/config endpoint to verify response matches mock data.
    """

    response = test_client.get("/ui/config")

    assert response.status_code == 200
    assert response.json() == mock_config_response
