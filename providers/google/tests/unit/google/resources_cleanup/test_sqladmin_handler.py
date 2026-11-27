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

from unittest.mock import AsyncMock, patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import sqladmin

pytestmark = pytest.mark.anyio


def test_sqladmin_handler_deleters_match_deletion_order():
    handler = sqladmin.CloudSQLDeleteHandler()
    assert set(handler.DELETERS.keys()) == set(handler.DELETION_ORDER)
    assert handler.DELETION_ORDER[-1] == "sqladmin.googleapis.com/Instance"


async def test_delete_instance():
    with patch.object(sqladmin, "run_command_async", AsyncMock()) as mock_run_cmd:
        res = {
            "name": "//sqladmin.googleapis.com/projects/my-project/instances/my-instance",
            "assetType": "sqladmin.googleapis.com/Instance",
        }
        await sqladmin._delete_instance(res, "[1/1] ")

    mock_run_cmd.assert_awaited_once_with(
        "gcloud sql instances delete my-instance --project=my-project --quiet",
        "[1/1] ",
    )


async def test_delete_backup():
    with patch.object(sqladmin, "run_command_async", AsyncMock()) as mock_run_cmd:
        res = {
            "name": "//sqladmin.googleapis.com/projects/my-project/instances/my-instance/backups/123456",
            "assetType": "sqladmin.googleapis.com/Backup",
        }
        await sqladmin._delete_backup(res, "[1/1] ")

    mock_run_cmd.assert_awaited_once_with(
        "gcloud sql backups delete 123456 --instance=my-instance --project=my-project --quiet",
        "[1/1] ",
    )


async def test_delete_backup_run():
    with patch.object(sqladmin, "run_command_async", AsyncMock()) as mock_run_cmd:
        res = {
            "name": "//sqladmin.googleapis.com/projects/my-project/instances/my-instance/backupRuns/789012",
            "assetType": "sqladmin.googleapis.com/BackupRun",
        }
        await sqladmin._delete_backup_run(res, "[1/1] ")

    mock_run_cmd.assert_awaited_once_with(
        "gcloud sql backups delete 789012 --instance=my-instance --project=my-project --quiet",
        "[1/1] ",
    )
