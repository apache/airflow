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
from airflow_google_provider_resource_cleanup.handlers import dataflow

pytestmark = pytest.mark.anyio


def _dataflow_resource(state: str) -> dict:
    return {
        "name": "//dataflow.googleapis.com/projects/test/locations/europe-west3/jobs/job-one",
        "location": "europe-west3",
        "state": state,
    }


@pytest.mark.parametrize("state", dataflow.FINISHED_JOB_STATUSES)
@mock.patch.object(dataflow, "run_command_async", new_callable=mock.AsyncMock)
@mock.patch.object(dataflow, "curl", new_callable=mock.AsyncMock)
async def test_archives_finished_job_with_rest_api(mock_curl, mock_run_command, state):
    await dataflow._delete_dataflow_job(_dataflow_resource(state), "[1/1] ")

    mock_curl.assert_awaited_once_with(
        "https://dataflow.googleapis.com/v1b3/projects/test/locations/europe-west3/jobs/job-one/"
        "?updateMask=job_metadata.user_display_properties.archived",
        log_prefix="[1/1] ",
        method="PUT",
        data={"jobMetadata": {"userDisplayProperties": {"archived": "true"}}},
    )
    mock_run_command.assert_not_awaited()


@mock.patch.object(dataflow, "run_command_async", new_callable=mock.AsyncMock, return_value=0)
@mock.patch.object(dataflow, "curl", new_callable=mock.AsyncMock)
async def test_cancels_active_job_without_archiving_it(mock_curl, mock_run_command):
    await dataflow._delete_dataflow_job(_dataflow_resource("JOB_STATE_RUNNING"), "[1/1] ")

    mock_run_command.assert_awaited_once_with(
        "gcloud dataflow jobs cancel job-one --region=europe-west3 --quiet", "[1/1] "
    )
    mock_curl.assert_not_awaited()


@mock.patch.object(dataflow, "run_command_async", new_callable=mock.AsyncMock, return_value=1)
async def test_cancel_failure_is_not_reported_as_success(mock_run_command):
    with pytest.raises(subprocess.CalledProcessError):
        await dataflow._delete_dataflow_job(_dataflow_resource("JOB_STATE_RUNNING"), "[1/1] ")


def test_dataflow_handler_limits_api_requests():
    assert dataflow.DataflowDeleteHandler.SEMAPHORE_COUNT == 1
    assert dataflow.DataflowDeleteHandler.SLEEP_AFTER_EACH_REQUEST == 1


@mock.patch.object(helpers, "run_command_async", new_callable=mock.AsyncMock, return_value=0)
@mock.patch.object(helpers, "_get_access_token", return_value="test-token")
async def test_curl_sends_json_request(mock_get_access_token, mock_run_command):
    url = "https://dataflow.googleapis.com/v1b3/projects/test/locations/us-central1/jobs/one"
    data = {"jobMetadata": {"userDisplayProperties": {"archived": "true"}}}

    await helpers.curl(url, log_prefix="[1/1] ", method="PUT", data=data)

    command = mock_run_command.await_args.args[0]
    assert command.startswith("curl --fail --silent --show-error")
    assert "--retry 5 --retry-delay 5 --retry-max-time 120" in command
    assert "-X PUT" in command
    assert "Authorization: Bearer $GOOGLE_OAUTH_ACCESS_TOKEN" in command
    assert "Content-Type: application/json; charset=utf-8" in command
    assert "--data" in command
    assert '"archived": "true"' in command
    assert url in command
    assert "test-token" not in command
    assert mock_run_command.await_args.kwargs["env"]["GOOGLE_OAUTH_ACCESS_TOKEN"] == "test-token"
    mock_get_access_token.assert_called_once_with()
