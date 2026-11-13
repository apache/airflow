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
from unittest.mock import AsyncMock, patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import dataplex


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("deleter", "resource_name", "expected_command"),
    [
        (
            dataplex._delete_entry_group,
            "//dataplex.googleapis.com/projects/test-project/locations/us-central1/entryGroups/group",
            "gcloud dataplex entry-groups delete group --location=us-central1 --project=test-project --quiet",
        ),
        (
            dataplex._delete_asset,
            "//dataplex.googleapis.com/projects/test-project/locations/us-central1/lakes/lake/"
            "zones/zone/assets/asset",
            "gcloud dataplex assets delete asset --location=us-central1 --lake=lake --zone=zone "
            "--project=test-project --quiet",
        ),
        (
            dataplex._delete_task,
            "//dataplex.googleapis.com/projects/test-project/locations/us-central1/lakes/lake/tasks/task",
            "gcloud dataplex tasks delete task --location=us-central1 --lake=lake "
            "--project=test-project --quiet",
        ),
        (
            dataplex._delete_zone,
            "//dataplex.googleapis.com/projects/test-project/locations/us-central1/lakes/lake/zones/zone",
            "gcloud dataplex zones delete zone --location=us-central1 --lake=lake "
            "--project=test-project --quiet",
        ),
        (
            dataplex._delete_lake,
            "//dataplex.googleapis.com/projects/test-project/locations/us-central1/lakes/lake",
            "gcloud dataplex lakes delete lake --location=us-central1 --project=test-project --quiet",
        ),
    ],
)
async def test_delete_dataplex_resource(deleter, resource_name, expected_command):
    resource = {"name": resource_name, "location": "us-central1"}

    with patch.object(dataplex, "run_command_async", AsyncMock(return_value=0)) as mock_run:
        await deleter(resource, "[1/1] ")

    mock_run.assert_awaited_once_with(expected_command, "[1/1] ")


@pytest.mark.anyio
@pytest.mark.parametrize("entry_group", ["@bigquery", "@dataprocmetastore", "@spanner", "@storage"])
async def test_delete_system_entry_group_is_skipped(entry_group, capsys):
    resource = {
        "name": (
            f"//dataplex.googleapis.com/projects/test-project/locations/global/entryGroups/{entry_group}"
        ),
        "location": "global",
    }

    with patch.object(dataplex, "run_command_async", AsyncMock()) as mock_run:
        result = await dataplex._delete_entry_group(resource, "[1/1] ")

    assert result is False
    mock_run.assert_not_awaited()
    assert (
        f"System entry group {entry_group} is managed by Dataplex and cannot be deleted. Skipping."
        in capsys.readouterr().out
    )


@pytest.mark.anyio
async def test_delete_failure_is_not_reported_as_success():
    resource = {
        "name": "//dataplex.googleapis.com/projects/test-project/locations/us-central1/lakes/lake",
        "location": "us-central1",
    }

    with (
        patch.object(dataplex, "run_command_async", AsyncMock(return_value=1)),
        pytest.raises(subprocess.CalledProcessError),
    ):
        await dataplex._delete_lake(resource, "[1/1] ")


def test_dataplex_handler_deletes_nested_resources_first():
    assert dataplex.DataplexDeleteHandler.DELETION_ORDER == [
        "dataplex.googleapis.com/EntryGroup",
        "dataplex.googleapis.com/Asset",
        "dataplex.googleapis.com/Task",
        "dataplex.googleapis.com/Zone",
        "dataplex.googleapis.com/Lake",
    ]
