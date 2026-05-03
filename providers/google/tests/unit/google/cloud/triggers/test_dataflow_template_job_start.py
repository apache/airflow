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

import sys
import types
from unittest import mock

import pytest
from google.api_core.exceptions import ServiceUnavailable
from google.cloud.dataflow_v1beta3 import JobState

from airflow.triggers.base import TriggerEvent

PROJECT_ID = "test-project-id"
JOB_ID = "test_job_id_2012-12-23-10:00"
LOCATION = "us-central1"
GCP_CONN_ID = "test_gcp_conn_id"
POLL_SLEEP = 20
IMPERSONATION_CHAIN = ["impersonate", "this"]
CANCEL_TIMEOUT = 10 * 420


def _beam_module_stubs() -> dict[str, types.ModuleType]:
    apache_module = types.ModuleType("airflow.providers.apache")
    apache_module.__path__ = []
    beam_package = types.ModuleType("airflow.providers.apache.beam")
    beam_package.__path__ = []
    hooks_package = types.ModuleType("airflow.providers.apache.beam.hooks")
    hooks_package.__path__ = []
    beam_module = types.ModuleType("airflow.providers.apache.beam.hooks.beam")

    class BeamRunnerType:
        DataflowRunner = "DataflowRunner"

    beam_module.BeamHook = mock.MagicMock()
    beam_module.BeamRunnerType = BeamRunnerType
    beam_module.beam_options_to_args = mock.MagicMock(return_value=[])

    return {
        "airflow.providers.apache": apache_module,
        "airflow.providers.apache.beam": beam_package,
        "airflow.providers.apache.beam.hooks": hooks_package,
        "airflow.providers.apache.beam.hooks.beam": beam_module,
    }


with mock.patch.dict(sys.modules, _beam_module_stubs()):
    from airflow.providers.google.cloud.triggers import dataflow as dataflow_triggers

TemplateJobStartTrigger = dataflow_triggers.TemplateJobStartTrigger


@pytest.fixture
def template_job_start_trigger():
    return TemplateJobStartTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        cancel_timeout=CANCEL_TIMEOUT,
    )


@pytest.mark.asyncio
async def test_run_continues_polling_after_retryable_service_unavailable(template_job_start_trigger):
    with (
        mock.patch.object(
            dataflow_triggers.AsyncDataflowHook,
            "get_job_status",
            new_callable=mock.AsyncMock,
        ) as mock_job_status,
        mock.patch.object(dataflow_triggers.asyncio, "sleep", new_callable=mock.AsyncMock) as mock_sleep,
    ):
        mock_job_status.side_effect = [
            ServiceUnavailable(
                "Visibility check was unavailable. Please retry the request and contact support if the problem persists"
            ),
            JobState.JOB_STATE_DONE,
        ]

        expected_event = TriggerEvent(
            {
                "job_id": JOB_ID,
                "status": "success",
                "message": "Job completed",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

    assert actual_event == expected_event
    assert mock_job_status.call_count == 2
    mock_sleep.assert_awaited_once_with(POLL_SLEEP)


@pytest.mark.asyncio
async def test_run_returns_error_event_after_unexpected_status_polling_exception(
    template_job_start_trigger,
):
    with mock.patch.object(
        dataflow_triggers.AsyncDataflowHook,
        "get_job_status",
        new_callable=mock.AsyncMock,
    ) as mock_job_status:
        mock_job_status.side_effect = Exception("Test exception")

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

    assert actual_event == expected_event
    mock_job_status.assert_awaited_once_with(project_id=PROJECT_ID, job_id=JOB_ID, location=LOCATION)
