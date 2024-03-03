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

import asyncio
import logging
from unittest import mock

import pytest
from google.cloud.dataflow_v1beta3 import JobState

from airflow.providers.google.cloud.triggers.dataflow import TemplateJobStartTrigger
from airflow.triggers.base import TriggerEvent

PROJECT_ID = "test-project-id"
JOB_ID = "test_job_id_2012-12-23-10:00"
LOCATION = "us-central1"
GCP_CONN_ID = "test_gcp_conn_id"
POLL_SLEEP = 20
IMPERSONATION_CHAIN = ["impersonate", "this"]
CANCEL_TIMEOUT = 10 * 420


@pytest.fixture
def trigger():
    return TemplateJobStartTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        cancel_timeout=CANCEL_TIMEOUT,
    )


class TestTemplateJobStartTrigger:
    def test_serialize(self, trigger):
        actual_data = trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.TemplateJobStartTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "cancel_timeout": CANCEL_TIMEOUT,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        "attr, expected",
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
            ("cancel_timeout", CANCEL_TIMEOUT),
        ],
    )
    def test_get_async_hook(self, trigger, attr, expected):
        hook = trigger._get_async_hook()
        actual = hook._hook_kwargs.get(attr)
        assert actual is not None
        assert actual == expected

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_success_event(self, mock_job_status, trigger):
        mock_job_status.return_value = JobState.JOB_STATE_DONE

        expected_event = TriggerEvent(
            {
                "job_id": JOB_ID,
                "status": "success",
                "message": "Job completed",
            }
        )
        actual_event = await trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_failed_event(self, mock_job_status, trigger):
        mock_job_status.return_value = JobState.JOB_STATE_FAILED

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Dataflow job with id {JOB_ID} has failed its execution",
            }
        )
        actual_event = await trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_stopped_event(self, mock_job_status, trigger):
        mock_job_status.return_value = JobState.JOB_STATE_STOPPED
        expected_event = TriggerEvent(
            {
                "status": "stopped",
                "message": f"Dataflow job with id {JOB_ID} was stopped",
            }
        )
        actual_event = await trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_is_still_running(self, mock_job_status, trigger, caplog):
        mock_job_status.return_value = JobState.JOB_STATE_RUNNING
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert f"Current job status is: {JobState.JOB_STATE_RUNNING}"
        assert f"Sleeping for {POLL_SLEEP} seconds."
