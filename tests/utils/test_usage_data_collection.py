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

from airflow import __version__ as airflow_version
from airflow.configuration import conf
from airflow.utils.usage_data_collection import (
    get_database_version,
    get_python_version,
    usage_data_collection,
)


@pytest.mark.parametrize("is_enabled, is_prerelease", [(False, True), (True, True)])
@mock.patch("httpx.get")
def test_scarf_analytics_disabled(mock_get, is_enabled, is_prerelease):
    with mock.patch("airflow.settings.is_usage_data_collection_enabled", return_value=is_enabled), mock.patch(
        "airflow.utils.usage_data_collection._version_is_prerelease", return_value=is_prerelease
    ):
        usage_data_collection()
    mock_get.assert_not_called()


@mock.patch("airflow.settings.is_usage_data_collection_enabled", return_value=True)
@mock.patch("airflow.utils.usage_data_collection._version_is_prerelease", return_value=False)
@mock.patch("airflow.utils.usage_data_collection.get_database_version", return_value="12.3")
@mock.patch("airflow.utils.usage_data_collection.get_database_name", return_value="postgres")
@mock.patch("httpx.get")
def test_scarf_analytics(
    mock_get,
    mock_is_usage_data_collection_enabled,
    mock_version_is_prerelease,
    get_database_version,
    get_database_name,
):
    platform_sys = platform.system()
    platform_machine = platform.machine()
    python_version = get_python_version()
    executor = conf.get("core", "EXECUTOR")
    scarf_endpoint = "https://apacheairflow.gateway.scarf.sh/scheduler"
    usage_data_collection()

    expected_scarf_url = (
        f"{scarf_endpoint}?version={airflow_version}"
        f"&python_version={python_version}"
        f"&platform={platform_sys}"
        f"&arch={platform_machine}"
        f"&database=postgres"
        f"&db_version=12.3"
        f"&executor={executor}"
    )

    mock_get.assert_called_once_with(expected_scarf_url, timeout=5.0)


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
@pytest.mark.parametrize(
    "version_info, expected_version",
    [
        ((1, 2, 3), "1.2"),  # Normal version tuple
        (None, "None"),  # No version info available
        ((1,), "1"),  # Single element version tuple
        ((1, 2, 3, "beta", 4), "1.2"),  # Complex version tuple with strings
    ],
)
def test_get_database_version(version_info, expected_version):
    with mock.patch("airflow.settings.engine.dialect.server_version_info", new=version_info):
        assert get_database_version() == expected_version


@pytest.mark.parametrize(
    "version_info, expected_version",
    [
        ("1.2.3", "1.2"),  # Normal version
        ("4", "4"),  # Single element version
        ("1.2.3.beta4", "1.2"),  # Complex version tuple with strings
    ],
)
def test_get_python_version(version_info, expected_version):
    with mock.patch("platform.python_version", return_value=version_info):
        assert get_python_version() == expected_version
