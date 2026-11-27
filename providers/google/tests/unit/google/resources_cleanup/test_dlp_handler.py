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

from unittest import mock

import pytest
from airflow_google_provider_resource_cleanup.handlers import dlp

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("asset_type", "resource_path"),
    [
        ("dlp.googleapis.com/JobTrigger", "projects/test-project/locations/global/jobTriggers/trigger-1"),
        (
            "dlp.googleapis.com/DiscoveryConfig",
            "projects/test-project/locations/us-central1/discoveryConfigs/config-1",
        ),
        ("dlp.googleapis.com/DlpJob", "projects/test-project/locations/global/dlpJobs/i-job-1"),
        (
            "dlp.googleapis.com/InspectTemplate",
            "projects/test-project/locations/global/inspectTemplates/inspect-1",
        ),
        (
            "dlp.googleapis.com/DeidentifyTemplate",
            "projects/test-project/locations/global/deidentifyTemplates/deid-1",
        ),
        (
            "dlp.googleapis.com/StoredInfoType",
            "projects/test-project/locations/global/storedInfoTypes/info-type-1",
        ),
    ],
)
@mock.patch.object(dlp, "curl", autospec=True)
async def test_delete_dlp_resource(mock_curl, asset_type, resource_path):
    resource = {"name": f"//dlp.googleapis.com/{resource_path}", "assetType": asset_type}

    await dlp.DLPDeleteHandler.DELETERS[asset_type](resource, "[1/1] ")

    mock_curl.assert_awaited_once_with(
        f"https://dlp.googleapis.com/v2/{resource_path}",
        log_prefix="[1/1] ",
        check=True,
        ignore_not_found=True,
    )


@mock.patch.object(dlp, "curl", autospec=True)
async def test_dlp_handler_replaces_numeric_project_number_with_project_id(mock_curl):
    resources = [
        {
            "assetType": "dlp.googleapis.com/JobTrigger",
            "name": "//dlp.googleapis.com/projects/123456789012/locations/us-central1/jobTriggers/trigger-1",
        }
    ]

    await dlp.DLPDeleteHandler(project_id="configured-project").handle(resources)

    mock_curl.assert_awaited_once_with(
        "https://dlp.googleapis.com/v2/projects/configured-project/locations/us-central1/jobTriggers/trigger-1",
        log_prefix="[1/1] ",
        check=True,
        ignore_not_found=True,
    )


def test_dlp_deletion_order():
    handler = dlp.DLPDeleteHandler()
    assert set(handler.DELETERS.keys()) == set(handler.DELETION_ORDER)
    assert handler.DELETION_ORDER == [
        "dlp.googleapis.com/JobTrigger",
        "dlp.googleapis.com/DiscoveryConfig",
        "dlp.googleapis.com/DlpJob",
        "dlp.googleapis.com/InspectTemplate",
        "dlp.googleapis.com/DeidentifyTemplate",
        "dlp.googleapis.com/StoredInfoType",
    ]
