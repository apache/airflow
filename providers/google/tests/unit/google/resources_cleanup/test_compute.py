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
from airflow_google_provider_resource_cleanup import helpers
from airflow_google_provider_resource_cleanup.handlers import compute

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("scope", "location"),
    [("regions", "europe-west2"), ("zones", "europe-west2-b")],
)
@mock.patch.object(compute, "run_command_async", new_callable=mock.AsyncMock)
async def test_instance_group_manager_uses_correct_scope(mock_run_command, scope, location):
    resource = {
        "name": (
            f"//compute.googleapis.com/projects/test/{scope}/{location}/instanceGroupManagers/manager-one"
        ),
        "location": location,
    }

    await compute.delete_instance_group_manager(resource, "[1/1] ")

    flag = "region" if scope == "regions" else "zone"
    mock_run_command.assert_awaited_once_with(
        f"gcloud compute instance-groups managed delete manager-one --{flag}={location} --quiet",
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )


@pytest.mark.parametrize(
    ("asset_type", "path", "location", "expected_command"),
    [
        (
            "compute.googleapis.com/Instance",
            "projects/test/zones/us-central1-a/instances/instance-one",
            "us-central1-a",
            "gcloud compute instances delete instance-one --zone=us-central1-a --quiet",
        ),
        (
            "compute.googleapis.com/InstanceTemplate",
            "projects/test/global/instanceTemplates/template-one",
            "global",
            "gcloud compute instance-templates delete template-one --global --quiet",
        ),
        (
            "compute.googleapis.com/InstanceTemplate",
            "projects/test/regions/us-central1/instanceTemplates/template-one",
            "us-central1",
            "gcloud compute instance-templates delete template-one --region=us-central1 --quiet",
        ),
        (
            "compute.googleapis.com/Snapshot",
            "projects/test/global/snapshots/snapshot-one",
            "global",
            "gcloud compute snapshots delete snapshot-one --quiet",
        ),
    ],
)
@mock.patch.object(compute, "run_command_async", new_callable=mock.AsyncMock)
async def test_deletes_instances_templates_and_snapshots(
    mock_run_command, asset_type, path, location, expected_command
):
    resource = {"name": f"//compute.googleapis.com/{path}", "location": location}

    await compute.ComputeDeleteHandler.DELETERS[asset_type](resource, "[1/1] ")

    mock_run_command.assert_awaited_once_with(expected_command, "[1/1] ", check=True, ignore_not_found=True)


@pytest.mark.parametrize(
    ("scope", "location"),
    [("regions", "us-central1"), ("zones", "us-central1-a")],
)
@mock.patch.object(compute, "run_command_async", new_callable=mock.AsyncMock)
async def test_disk_deletion_does_not_use_stale_instance_references(mock_run_command, scope, location):
    resource = {
        "name": f"//compute.googleapis.com/projects/test/{scope}/{location}/disks/disk-one",
        "location": location,
        "additionalAttributes": {
            "users": ["https://www.googleapis.com/compute/v1/projects/test/zones/us-central1-a/instances/old"]
        },
    }

    await compute.delete_disk(resource, "[1/1] ")

    flag = "region" if scope == "regions" else "zone"
    mock_run_command.assert_awaited_once_with(
        f"gcloud compute disks delete disk-one --{flag}={location} --quiet",
        "[1/1] ",
        check=True,
        ignore_not_found=True,
    )


@pytest.mark.parametrize("stdout", [b"", None])
@pytest.mark.parametrize("stderr", ["NOT_FOUND: resource is gone", "resource was not found"])
async def test_checked_command_accepts_resource_that_is_already_gone(stdout, stderr):
    process = mock.MagicMock(returncode=1)
    process.communicate = mock.AsyncMock(return_value=(stdout, stderr.encode()))

    with mock.patch.object(helpers.asyncio, "create_subprocess_shell", return_value=process):
        result = await helpers.run_command_async("delete command", check=True, ignore_not_found=True)

    assert result is False


@pytest.mark.parametrize(("returncode", "check"), [(0, True), (1, False)])
async def test_command_without_output_streams(returncode, check):
    process = mock.MagicMock(returncode=returncode)
    process.communicate = mock.AsyncMock(return_value=(None, None))

    with mock.patch.object(helpers.asyncio, "create_subprocess_shell", return_value=process):
        result = await helpers.run_command_async("delete command", check=check, ignore_not_found=True)

    assert result is None


async def test_checked_command_raises_for_real_delete_failure():
    process = mock.MagicMock(returncode=1)
    process.communicate = mock.AsyncMock(return_value=(b"", b"PERMISSION_DENIED"))

    with (
        mock.patch.object(helpers.asyncio, "create_subprocess_shell", return_value=process),
        pytest.raises(subprocess.CalledProcessError),
    ):
        await helpers.run_command_async("delete command", check=True, ignore_not_found=True)


@mock.patch.object(compute, "run_command_async", new_callable=mock.AsyncMock)
async def test_compute_handler_does_not_report_missing_resource_as_deleted(mock_run_command, capsys):
    mock_run_command.return_value = False
    resource = {
        "assetType": "compute.googleapis.com/Instance",
        "name": "//compute.googleapis.com/projects/test/zones/us-central1-a/instances/instance-one",
        "location": "us-central1-a",
    }

    await compute.ComputeDeleteHandler().handle([resource])

    assert "Resource deleted!" not in capsys.readouterr().out


@mock.patch.object(compute, "run_command_async", new_callable=mock.AsyncMock)
async def test_compute_handler_logs_failure_and_continues_with_remaining_resources(mock_run_command, capsys):
    mock_run_command.side_effect = [subprocess.CalledProcessError(1, "delete command"), None]
    failing_instance = {
        "assetType": "compute.googleapis.com/Instance",
        "name": "//compute.googleapis.com/projects/test/zones/us-central1-a/instances/instance-one",
        "location": "us-central1-a",
    }
    snapshot = {
        "assetType": "compute.googleapis.com/Snapshot",
        "name": "//compute.googleapis.com/projects/test/global/snapshots/snapshot-one",
        "location": "global",
    }

    await compute.ComputeDeleteHandler().handle([failing_instance, snapshot])

    assert mock_run_command.await_count == 2
    output = capsys.readouterr()
    assert "Failed to delete a resource of type compute.googleapis.com/Instance" in output.out
    assert "CalledProcessError" in output.err


def test_deletion_order_respects_resource_dependencies():
    assert compute.ComputeDeleteHandler.DELETION_ORDER == [
        "compute.googleapis.com/InstanceGroupManager",
        "compute.googleapis.com/Instance",
        "compute.googleapis.com/InstanceTemplate",
        "compute.googleapis.com/Disk",
        "compute.googleapis.com/Snapshot",
    ]
