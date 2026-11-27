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
from airflow_google_provider_resource_cleanup.handlers import sqladmin

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("asset_type", "resource_name", "expected_command"),
    [
        (
            "sqladmin.googleapis.com/Instance",
            "//sqladmin.googleapis.com/projects/my-project/instances/my-instance",
            "gcloud sql instances delete my-instance --project=my-project --quiet",
        ),
        (
            "sqladmin.googleapis.com/Backup",
            "//sqladmin.googleapis.com/projects/my-project/instances/my-instance/backups/123456",
            "gcloud sql backups delete 123456 --instance=my-instance --project=my-project --quiet",
        ),
        (
            "sqladmin.googleapis.com/Backup",
            "//sqladmin.googleapis.com/projects/my-project/backups/123456",
            "gcloud sql backups delete projects/my-project/backups/123456 --project=my-project --quiet",
        ),
        (
            "sqladmin.googleapis.com/BackupRun",
            "//sqladmin.googleapis.com/projects/my-project/instances/my-instance/backupRuns/789012",
            "gcloud sql backups delete 789012 --instance=my-instance --project=my-project --quiet",
        ),
    ],
)
@mock.patch.object(sqladmin, "run_command_async", autospec=True)
async def test_delete_sqladmin_resource(mock_run_command_async, asset_type, resource_name, expected_command):
    resource = {"name": resource_name, "assetType": asset_type}

    await sqladmin.CloudSQLDeleteHandler.DELETERS[asset_type](resource, "[1/1] ")

    mock_run_command_async.assert_awaited_once_with(
        expected_command,
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )


def test_sqladmin_handler_deleters_match_deletion_order():
    handler = sqladmin.CloudSQLDeleteHandler()
    assert set(handler.DELETERS.keys()) == set(handler.DELETION_ORDER)
    assert handler.DELETION_ORDER == [
        "sqladmin.googleapis.com/BackupRun",
        "sqladmin.googleapis.com/Backup",
        "sqladmin.googleapis.com/Instance",
    ]


@mock.patch.object(sqladmin, "run_command_async", autospec=True)
async def test_sqladmin_handler_uses_configured_project_id_for_numeric_project(mock_run_command_async):
    resources = [
        {
            "assetType": "sqladmin.googleapis.com/Backup",
            "name": "//cloudsql.googleapis.com/projects/123456789012/backups/123456",
            "project": "projects/123456789012",
        },
        {
            "assetType": "sqladmin.googleapis.com/Instance",
            "name": "//cloudsql.googleapis.com/projects/123456789012/instances/my-instance",
            "project": "projects/123456789012",
        },
    ]

    await sqladmin.CloudSQLDeleteHandler(project_id="configured-project").handle(resources)

    assert mock_run_command_async.await_args_list == [
        mock.call(
            "gcloud sql backups delete projects/configured-project/backups/123456 "
            "--project=configured-project --quiet",
            "[1/1] ",
            check=True,
            ignore_not_found=True,
        ),
        mock.call(
            "gcloud sql instances delete my-instance --project=configured-project --quiet",
            "[1/1] ",
            check=True,
            ignore_not_found=True,
        ),
    ]
