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

import subprocess
from unittest import mock

import pytest
from airflow_google_provider_resource_cleanup.handlers import dataproc_metastore

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("asset_type", "resource", "expected_command"),
    [
        (
            "metastore.googleapis.com/Service",
            {
                "name": (
                    "//metastore.googleapis.com/projects/test-project/locations/us-central1/"
                    "services/test-service"
                ),
                "location": "us-central1",
            },
            "gcloud metastore services delete test-service --location=us-central1 --project=test-project --quiet",
        ),
        (
            "metastore.googleapis.com/Backup",
            {
                "name": (
                    "//metastore.googleapis.com/projects/test-project/locations/europe-west3/"
                    "services/test-service/backups/test-backup"
                ),
            },
            (
                "gcloud metastore services backups delete test-backup "
                "--service=test-service --location=europe-west3 --project=test-project --quiet"
            ),
        ),
    ],
)
@mock.patch.object(dataproc_metastore, "run_command_async", autospec=True)
async def test_delete_dataproc_metastore_resource(
    mock_run_command_async, asset_type, resource, expected_command
):
    await dataproc_metastore.DataprocMetastoreDeleteHandler.DELETERS[asset_type](resource, "[1/1] ")

    mock_run_command_async.assert_awaited_once_with(
        expected_command,
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )


@mock.patch.object(dataproc_metastore, "run_command_async", autospec=True)
async def test_dataproc_metastore_handler_logs_failure_and_continues(mock_run_command_async, capsys):
    mock_run_command_async.side_effect = [subprocess.CalledProcessError(1, "delete"), False]
    resources = [
        {
            "assetType": "metastore.googleapis.com/Backup",
            "name": (
                "//metastore.googleapis.com/projects/test-project/locations/us-central1/"
                "services/test-service/backups/test-backup"
            ),
            "location": "us-central1",
        },
        {
            "assetType": "metastore.googleapis.com/Service",
            "name": (
                "//metastore.googleapis.com/projects/test-project/locations/us-central1/services/test-service"
            ),
            "location": "us-central1",
        },
    ]

    await dataproc_metastore.DataprocMetastoreDeleteHandler().handle(resources)

    assert mock_run_command_async.await_count == 2
    output = capsys.readouterr()
    assert "Found an error in the handle deletion results: " in output.out
    assert "Resource deleted!" not in output.out


def test_dataproc_metastore_deletion_order_removes_backups_before_services():
    assert dataproc_metastore.DataprocMetastoreDeleteHandler.DELETION_ORDER == [
        "metastore.googleapis.com/Backup",
        "metastore.googleapis.com/Service",
    ]


@mock.patch.object(dataproc_metastore, "run_command_async", autospec=True)
async def test_dataproc_metastore_handler_uses_configured_project_id_for_numeric_project(
    mock_run_command_async,
):
    resources = [
        {
            "assetType": "metastore.googleapis.com/Service",
            "name": (
                "//metastore.googleapis.com/projects/123456789012/locations/us-central1/services/test-service"
            ),
            "project": "projects/123456789012",
        }
    ]

    await dataproc_metastore.DataprocMetastoreDeleteHandler(project_id="configured-project").handle(resources)

    mock_run_command_async.assert_awaited_once_with(
        "gcloud metastore services delete test-service --location=us-central1 --project=configured-project --quiet",
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )
