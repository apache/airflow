#
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

import platform
from unittest import mock

import pytest

from airflow import __version__ as airflow_version, settings
from airflow.configuration import conf
from airflow.utils.scarf import scarf_analytics


@pytest.mark.parametrize("is_enabled, is_prerelease", [(False, True), (True, True)])
@mock.patch("httpx.get")
def test_scarf_analytics_disabled(mock_get, is_enabled, is_prerelease):
    with mock.patch("airflow.settings.is_scarf_analytics_enabled", return_value=is_enabled), mock.patch(
        "airflow.utils.scarf._version_is_prerelease", return_value=is_prerelease
    ):
        scarf_analytics()
    mock_get.assert_not_called()


@mock.patch("airflow.settings.is_scarf_analytics_enabled", return_value=True)
@mock.patch("airflow.utils.scarf._version_is_prerelease", return_value=False)
@mock.patch("httpx.get")
def test_scarf_analytics(mock_get, mock_is_scarf_analytics_enabled, mock_version_is_prerelease):
    platform_sys = platform.system()
    platform_machine = platform.machine()
    python_version = platform.python_version()
    version_info = settings.engine.dialect.server_version_info
    version_info = ".".join(map(str, version_info)) if version_info else ""
    database = settings.engine.dialect.name
    executor = conf.get("core", "EXECUTOR")
    scarf_endpoint = "https://apacheairflow.gateway.scarf.sh/scheduler"
    scarf_analytics()

    expected_scarf_url = (
        f"{scarf_endpoint}?version={airflow_version}"
        f"&python_version={python_version}"
        f"&platform={platform_sys}"
        f"&arch={platform_machine}"
        f"&database={database}"
        f"&db_version={version_info}"
        f"&executor={executor}"
    )

    mock_get.assert_called_once_with(expected_scarf_url, timeout=5.0)
