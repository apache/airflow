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

import json

import pytest

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

THEME = {
    "tokens": {
        "colors": {
            "brand": {
                "50": {"value": "oklch(0.98 0.006 248.717)"},
                "100": {"value": "oklch(0.962 0.012 249.46)"},
                "200": {"value": "oklch(0.923 0.023 255.082)"},
                "300": {"value": "oklch(0.865 0.039 252.42)"},
                "400": {"value": "oklch(0.705 0.066 256.378)"},
                "500": {"value": "oklch(0.575 0.08 257.759)"},
                "600": {"value": "oklch(0.469 0.084 257.657)"},
                "700": {"value": "oklch(0.399 0.084 257.85)"},
                "800": {"value": "oklch(0.324 0.072 260.329)"},
                "900": {"value": "oklch(0.259 0.062 265.566)"},
                "950": {"value": "oklch(0.179 0.05 265.487)"},
            }
        }
    },
    "globalCss": {
        "button": {
            "text-transform": "uppercase",
        },
        "a": {
            "text-transform": "uppercase",
        },
    },
    "icon": "https://somehost.com/static/custom-logo.svg",
    "icon_dark_mode": "/static/custom-logo-dark.svg",
}

expected_config_response = {
    "fallback_page_limit": 100,
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
    "theme": THEME,
    "multi_team": False,
}


def _theme_conf_vars(theme: dict) -> dict:
    return {
        ("api", "instance_name"): "Airflow",
        ("api", "enable_swagger_ui"): "true",
        ("api", "hide_paused_dags_by_default"): "true",
        ("api", "fallback_page_limit"): "100",
        ("api", "default_wrap"): "false",
        ("api", "auto_refresh_interval"): "3",
        ("api", "require_confirmation_dag_change"): "false",
        ("api", "theme"): json.dumps(theme),
    }


@pytest.fixture
def mock_config_data():
    """
    Mock configuration settings used in the endpoint.
    """
    with conf_vars(_theme_conf_vars(THEME)):
        yield


THEME_WITH_ALL_COLORS = {
    "tokens": {
        "colors": {
            "brand": {
                "50": {"value": "oklch(0.975 0.008 298.0)"},
                "100": {"value": "oklch(0.950 0.020 298.0)"},
                "200": {"value": "oklch(0.900 0.045 298.0)"},
                "300": {"value": "oklch(0.800 0.080 298.0)"},
                "400": {"value": "oklch(0.680 0.120 298.0)"},
                "500": {"value": "oklch(0.560 0.160 298.0)"},
                "600": {"value": "oklch(0.460 0.190 298.0)"},
                "700": {"value": "oklch(0.390 0.160 298.0)"},
                "800": {"value": "oklch(0.328 0.080 298.0)"},
                "900": {"value": "oklch(0.230 0.050 298.0)"},
                "950": {"value": "oklch(0.155 0.030 298.0)"},
            },
            "gray": {
                "50": {"value": "oklch(0.975 0.002 264.0)"},
                "100": {"value": "oklch(0.950 0.003 264.0)"},
                "200": {"value": "oklch(0.880 0.005 264.0)"},
                "300": {"value": "oklch(0.780 0.008 264.0)"},
                "400": {"value": "oklch(0.640 0.012 264.0)"},
                "500": {"value": "oklch(0.520 0.015 264.0)"},
                "600": {"value": "oklch(0.420 0.015 264.0)"},
                "700": {"value": "oklch(0.340 0.012 264.0)"},
                "800": {"value": "oklch(0.260 0.009 264.0)"},
                "900": {"value": "oklch(0.200 0.007 264.0)"},
                "950": {"value": "oklch(0.145 0.005 264.0)"},
            },
            "black": {"value": "oklch(0.220 0.025 288.6)"},
            "white": {"value": "oklch(0.985 0.002 264.0)"},
        }
    },
}


@pytest.fixture
def mock_config_data_all_colors():
    with conf_vars(_theme_conf_vars(THEME_WITH_ALL_COLORS)):
        yield


class TestGetConfig:
    def test_should_response_200(self, mock_config_data, test_client):
        """
        Test the /config endpoint to verify response matches the expected data.
        """
        with assert_queries_count(0):
            response = test_client.get("/config")

        assert response.status_code == 200
        assert response.json() == expected_config_response

    def test_get_config_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/config")
        assert response.status_code == 401

    def test_get_config_just_authenticated(self, mock_config_data, unauthorized_test_client):
        """Just being authenticated is enough to access the endpoint."""
        response = unauthorized_test_client.get("/config")
        assert response.status_code == 200
        assert response.json() == expected_config_response

    def test_should_response_200_with_all_color_tokens(self, mock_config_data_all_colors, test_client):
        """Theme with gray, black, and white tokens (in addition to brand) passes validation and round-trips."""
        response = test_client.get("/config")

        assert response.status_code == 200
        theme = response.json()["theme"]
        colors = theme["tokens"]["colors"]
        assert "brand" in colors
        assert "gray" in colors
        assert "black" in colors
        assert "white" in colors
        assert colors["black"] == {"value": "oklch(0.22 0.025 288.6)"}
        assert colors["white"] == {"value": "oklch(0.985 0.002 264.0)"}
