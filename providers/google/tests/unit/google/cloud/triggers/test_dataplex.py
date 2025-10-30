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
from google.cloud.dataplex_v1.types import DataScanJob

from airflow.providers.google.cloud.triggers.dataplex import DataplexDataQualityJobTrigger
from airflow.triggers.base import TriggerEvent

TEST_PROJECT_ID = "project-id"
TEST_REGION = "region"
TEST_POLL_INTERVAL = 5
TEST_GCP_CONN_ID = "test_conn"
TEST_JOB_ID = "test_job_id"
TEST_DATA_SCAN_ID = "test_data_scan_id"
HOOK_STR = "airflow.providers.google.cloud.hooks.dataplex.DataplexAsyncHook.{}"
TRIGGER_STR = "airflow.providers.google.cloud.triggers.dataplex.DataplexDataQualityJobTrigger.{}"


@pytest.fixture
def trigger():
    return DataplexDataQualityJobTrigger(
        job_id=TEST_JOB_ID,
        data_scan_id=TEST_DATA_SCAN_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=None,
        polling_interval_seconds=TEST_POLL_INTERVAL,
    )


@pytest.fixture
def async_get_data_scan_job():
    def func(**kwargs):
        m = mock.MagicMock()
        m.configure_mock(**kwargs)
        f = asyncio.Future()
        f.set_result(m)
        return f

    return func


class TestDataplexDataQualityJobTrigger:
    def test_async_dataplex_job_trigger_serialization_should_execute_successfully(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.dataplex.DataplexDataQualityJobTrigger"
        assert kwargs == {
            "job_id": TEST_JOB_ID,
            "data_scan_id": TEST_DATA_SCAN_ID,
            "project_id": TEST_PROJECT_ID,
            "region": TEST_REGION,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": None,
            "polling_interval_seconds": TEST_POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch(TRIGGER_STR.format("_convert_to_dict"))
    @mock.patch(HOOK_STR.format("get_data_scan_job"))
    async def test_async_dataplex_job_triggers_on_success_should_execute_successfully(
        self, mock_hook, mock_convert_to_dict, trigger, async_get_data_scan_job
    ):
        mock_hook.return_value = async_get_data_scan_job(
            state=DataScanJob.State.SUCCEEDED,
        )
        mock_convert_to_dict.return_value = {}

        generator = trigger.run()
        actual_event = await generator.asend(None)

        expected_event = TriggerEvent(
            {
                "job_id": TEST_JOB_ID,
                "job_state": DataScanJob.State.SUCCEEDED.name,
                "job": {},
            }
        )
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch(TRIGGER_STR.format("_convert_to_dict"))
    @mock.patch(HOOK_STR.format("get_data_scan_job"))
    async def test_async_dataplex_job_trigger_run_returns_error_event(
        self, mock_hook, mock_convert_to_dict, trigger, async_get_data_scan_job
    ):
        mock_hook.return_value = async_get_data_scan_job(
            state=DataScanJob.State.FAILED,
        )
        mock_convert_to_dict.return_value = {}

        actual_event = await trigger.run().asend(None)
        await asyncio.sleep(0.5)

        expected_event = TriggerEvent(
            {"job_id": TEST_JOB_ID, "job_state": DataScanJob.State.FAILED.name, "job": {}}
        )
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("get_data_scan_job"))
    async def test_async_dataplex_job_run_loop_is_still_running(
        self, mock_hook, trigger, caplog, async_get_data_scan_job
    ):
        mock_hook.return_value = async_get_data_scan_job(
            state=DataScanJob.State.RUNNING,
        )

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert f"Current state is: RUNNING, sleeping for {TEST_POLL_INTERVAL} seconds." in caplog.text
