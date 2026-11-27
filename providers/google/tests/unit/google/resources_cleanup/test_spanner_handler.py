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
from airflow_google_provider_resource_cleanup.handlers import spanner

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("asset_type", "resource_path", "expected_command"),
    [
        (
            "spanner.googleapis.com/BackupSchedule",
            "projects/test-project/instances/test-instance/databases/test-db/backupSchedules/test-sched",
            (
                "gcloud spanner backup-schedules delete test-sched "
                "--database=test-db --instance=test-instance --project=test-project --quiet"
            ),
        ),
        (
            "spanner.googleapis.com/Backup",
            "projects/test-project/instances/test-instance/backups/test-backup",
            "gcloud spanner backups delete test-backup --instance=test-instance --project=test-project --quiet",
        ),
        (
            "spanner.googleapis.com/Database",
            "projects/test-project/instances/test-instance/databases/test-db",
            "gcloud spanner databases delete test-db --instance=test-instance --project=test-project --quiet",
        ),
        (
            "spanner.googleapis.com/InstancePartition",
            "projects/test-project/instances/test-instance/instancePartitions/test-part",
            (
                "gcloud spanner instance-partitions delete test-part "
                "--instance=test-instance --project=test-project --quiet"
            ),
        ),
        (
            "spanner.googleapis.com/Instance",
            "projects/test-project/instances/test-instance",
            "gcloud spanner instances delete test-instance --project=test-project --quiet",
        ),
        (
            "spanner.googleapis.com/InstanceConfig",
            "projects/test-project/instanceConfigs/custom-test-config",
            "gcloud spanner instance-configs delete custom-test-config --project=test-project --quiet",
        ),
    ],
)
@mock.patch.object(spanner, "run_command_async", autospec=True)
async def test_delete_spanner_resource(mock_run_command_async, asset_type, resource_path, expected_command):
    resource = {"name": f"//spanner.googleapis.com/{resource_path}", "assetType": asset_type}

    await spanner.SpannerDeleteHandler.DELETERS[asset_type](resource, "[1/1] ")

    mock_run_command_async.assert_awaited_once_with(
        expected_command,
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )


@mock.patch.object(spanner, "run_command_async", autospec=True)
async def test_managed_spanner_instance_config_is_skipped(mock_run_command_async, capsys):
    resource = {
        "name": "//spanner.googleapis.com/projects/test-project/instanceConfigs/regional-us-central1",
        "assetType": "spanner.googleapis.com/InstanceConfig",
    }

    result = await spanner._delete_instance_config(resource, "[1/1] ")

    assert result is False
    mock_run_command_async.assert_not_awaited()
    assert (
        "Managed Spanner instance config regional-us-central1 cannot be deleted. Skipping."
        in capsys.readouterr().out
    )


def test_spanner_deletion_order_deletes_instance_configs_after_instances():
    handler = spanner.SpannerDeleteHandler()
    assert set(handler.DELETERS.keys()) == set(handler.DELETION_ORDER)
    assert handler.DELETION_ORDER == [
        "spanner.googleapis.com/BackupSchedule",
        "spanner.googleapis.com/Backup",
        "spanner.googleapis.com/Database",
        "spanner.googleapis.com/InstancePartition",
        "spanner.googleapis.com/Instance",
        "spanner.googleapis.com/InstanceConfig",
    ]


@mock.patch.object(spanner, "run_command_async", autospec=True)
async def test_spanner_handler_uses_configured_project_id_for_numeric_project(mock_run_command_async):
    resources = [
        {
            "assetType": "spanner.googleapis.com/Instance",
            "name": "//spanner.googleapis.com/projects/123456789012/instances/test-instance",
            "project": "projects/123456789012",
        }
    ]

    await spanner.SpannerDeleteHandler(project_id="configured-project").handle(resources)

    mock_run_command_async.assert_awaited_once_with(
        "gcloud spanner instances delete test-instance --project=configured-project --quiet",
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )
