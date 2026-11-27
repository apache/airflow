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
from airflow_google_provider_resource_cleanup.handlers import storage

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("asset_type", "resource", "expected_command"),
    [
        (
            "storage.googleapis.com/Bucket",
            {"name": "//storage.googleapis.com/test-bucket"},
            "gcloud storage rm --recursive gs://test-bucket --quiet",
        ),
        (
            "storage.googleapis.com/Bucket",
            {"name": "//storage.googleapis.com/projects/_/buckets/test-bucket"},
            "gcloud storage rm --recursive gs://test-bucket --quiet",
        ),
        (
            "storagetransfer.googleapis.com/TransferJob",
            {"name": "//storagetransfer.googleapis.com/projects/test-project/transferJobs/123456"},
            "gcloud transfer jobs delete 123456 --project=test-project --quiet",
        ),
        (
            "storagetransfer.googleapis.com/TransferJob",
            {
                "name": "//storagetransfer.googleapis.com/projects/123456789012/transferJobs/123456",
                "project": "projects/123456789012",
            },
            "gcloud transfer jobs delete 123456 --quiet",
        ),
        (
            "storagetransfer.googleapis.com/TransferJob",
            {
                "name": "//storagetransfer.googleapis.com/transferJobs/123456",
                "project": "projects/test-project",
            },
            "gcloud transfer jobs delete 123456 --project=test-project --quiet",
        ),
        (
            "storagetransfer.googleapis.com/TransferJob",
            {"name": "//storagetransfer.googleapis.com/transferJobs/123456"},
            "gcloud transfer jobs delete 123456 --quiet",
        ),
    ],
)
@mock.patch.object(storage, "run_command_async", autospec=True)
async def test_delete_storage_and_transfer_resources(
    mock_run_command_async, asset_type, resource, expected_command
):
    await storage.StorageDeleteHandler.DELETERS[asset_type](resource, "[1/1] ")

    mock_run_command_async.assert_awaited_once_with(
        expected_command,
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )


@pytest.mark.parametrize(
    ("resource", "expected_command"),
    [
        (
            {
                "assetType": "storagetransfer.googleapis.com/TransferJob",
                "name": "//storagetransfer.googleapis.com/projects/123456789012/transferJobs/123456",
                "project": "projects/123456789012",
            },
            "gcloud transfer jobs delete 123456 --project=configured-project --quiet",
        ),
        (
            {
                "assetType": "storage.googleapis.com/Bucket",
                "name": "//storage.googleapis.com/projects/_/buckets/test-bucket",
                "project": "projects/123456789012",
            },
            "gcloud storage rm --recursive gs://test-bucket --project=configured-project --quiet",
        ),
    ],
)
@mock.patch.object(storage, "run_command_async", autospec=True)
async def test_storage_handler_uses_configured_project_id(mock_run_command_async, resource, expected_command):
    await storage.StorageDeleteHandler(project_id="configured-project").handle([resource])

    mock_run_command_async.assert_awaited_once_with(
        expected_command,
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )


@mock.patch.object(storage, "run_command_async", autospec=True)
async def test_already_deleted_transfer_job_is_skipped(mock_run_command_async, capsys):
    resource = {
        "name": "//storagetransfer.googleapis.com/transferJobs/123456",
        "state": "DELETED",
    }

    result = await storage._delete_transfer_job(resource, "[1/1] ")

    assert result is False
    mock_run_command_async.assert_not_awaited()
    assert "Transfer job transferJobs/123456 is already deleted. Skipping." in capsys.readouterr().out


def test_storage_deletion_order_deletes_transfer_jobs_before_buckets():
    handler = storage.StorageDeleteHandler()
    assert set(handler.DELETERS.keys()) == set(handler.DELETION_ORDER)
    assert handler.DELETION_ORDER == [
        "storagetransfer.googleapis.com/TransferJob",
        "storage.googleapis.com/Bucket",
    ]
