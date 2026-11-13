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
from airflow_google_provider_resource_cleanup.handlers import dataproc


@pytest.mark.anyio
async def test_delete_job():
    resource = {
        "name": "//dataproc.googleapis.com/projects/test-project/regions/us-central1/jobs/test-job",
        "location": "us-central1",
    }

    with patch.object(dataproc, "run_command_async_with_stderr", AsyncMock(return_value=(0, ""))) as mock_run:
        await dataproc._delete_job(resource, "[1/1] ")

    mock_run.assert_awaited_once_with(
        "gcloud dataproc jobs delete test-job --quiet --region=us-central1",
        log_prefix="[1/1] ",
    )


@pytest.mark.anyio
async def test_serverless_batch_job_record_is_skipped(capsys):
    resource = {
        "name": (
            "//dataproc.googleapis.com/projects/test-project/regions/us-central1/jobs/"
            "srvls-batch-50299d2e-4a2b-45ae-ab3a-249177aef048"
        ),
        "location": "us-central1",
    }

    with patch.object(dataproc, "run_command_async_with_stderr", AsyncMock()) as mock_run:
        result = await dataproc._delete_job(resource, "[1/1] ")

    assert result is False
    mock_run.assert_not_awaited()
    assert "is managed by its batch. Skipping." in capsys.readouterr().out


@pytest.mark.anyio
async def test_missing_dataproc_resource_is_skipped(capsys):
    resource = {
        "name": "//dataproc.googleapis.com/projects/test-project/locations/us-central1/batches/batch",
        "location": "us-central1",
    }

    with patch.object(
        dataproc,
        "run_command_async_with_stderr",
        AsyncMock(return_value=(1, "ERROR: NOT_FOUND: batch was not found")),
    ):
        result = await dataproc._delete_batch(resource, "[1/1] ")

    assert result is False
    assert "resource batch no longer exists. Skipping." in capsys.readouterr().out


@pytest.mark.anyio
async def test_dataproc_delete_failure_is_raised():
    resource = {
        "name": "//dataproc.googleapis.com/projects/test-project/regions/us-central1/jobs/test-job",
        "location": "us-central1",
    }

    with (
        patch.object(
            dataproc,
            "run_command_async_with_stderr",
            AsyncMock(return_value=(1, "ERROR: PERMISSION_DENIED")),
        ),
        pytest.raises(subprocess.CalledProcessError),
    ):
        await dataproc._delete_job(resource, "[1/1] ")
