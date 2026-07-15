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

from airflow.providers.common.dataquality.api.app import dataquality_app
from airflow.providers.common.dataquality.plugins.dataquality_plugin import (
    DataQualityPlugin,
    _get_base_url_path,
    _get_bundle_url,
    _react_apps,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


class TestDataQualityPlugin:
    def test_name(self):
        assert DataQualityPlugin.name == "dataquality"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Plugin endpoints are not supported before Airflow 3.1"
    )
    def test_registers_the_query_api_under_dataquality_prefix(self):
        assert DataQualityPlugin.fastapi_apps == [
            {"name": "dataquality", "app": dataquality_app, "url_prefix": "/dataquality"}
        ]


@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Plugin React apps are not supported before Airflow 3.1")
class TestReactApps:
    def test_empty_when_bundle_not_built(self, tmp_path):
        # A plain source checkout has no dist/ until `pnpm build` runs (wired into the wheel
        # build via hatch_build.py); the entry must not advertise a bundle URL that 404s.
        assert _react_apps(tmp_path / "dist") == []

    def test_registers_task_and_task_instance_views_once_bundle_exists(self, tmp_path):
        dist_dir = tmp_path / "dist"
        dist_dir.mkdir()
        (dist_dir / "main.umd.cjs").write_text("")

        with conf_vars({("api", "base_url"): "/"}):
            apps = _react_apps(dist_dir)

        assert apps == [
            {
                "name": "Data Quality",
                "bundle_url": "/dataquality/static/main.umd.cjs",
                "destination": "task",
                "url_route": "dataquality-task",
            },
            {
                "name": "Data Quality",
                "bundle_url": "/dataquality/static/main.umd.cjs",
                "destination": "task_instance",
                "url_route": "dataquality-run",
            },
        ]


class TestGetBaseUrlPath:
    def test_root_base_url(self):
        with conf_vars({("api", "base_url"): "/"}):
            assert _get_base_url_path("/dataquality") == "/dataquality"

    def test_http_base_url_extracts_path(self):
        with conf_vars({("api", "base_url"): "http://example.com/airflow/"}):
            assert _get_base_url_path("/dataquality") == "/airflow/dataquality"


class TestGetBundleUrl:
    def test_root_base_url_returns_relative_path(self):
        with conf_vars({("api", "base_url"): "/"}):
            assert _get_bundle_url() == "/dataquality/static/main.umd.cjs"

    def test_http_base_url_returns_absolute_url(self):
        with conf_vars({("api", "base_url"): "http://example.com:28080/airflow/"}):
            assert _get_bundle_url() == "http://example.com:28080/airflow/dataquality/static/main.umd.cjs"
