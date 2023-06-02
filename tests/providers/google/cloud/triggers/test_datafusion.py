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

from airflow.providers.google.cloud.triggers.datafusion import DataFusionStartPipelineTrigger
from airflow.triggers.base import TriggerEvent

HOOK_STATUS_STR = "airflow.providers.google.cloud.hooks.datafusion.DataFusionAsyncHook.get_pipeline_status"
CLASSPATH = "airflow.providers.google.cloud.triggers.datafusion.DataFusionStartPipelineTrigger"

TASK_ID = "test_task"
LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}
PROJECT_ID = "test_project_id"
PIPELINE_NAME = "shrubberyPipeline"
PIPELINE = {"test": "pipeline"}
PIPELINE_ID = "test_pipeline_id"
INSTANCE_URL = "http://datafusion.instance.com"
NAMESPACE = "TEST_NAMESPACE"
RUNTIME_ARGS = {"arg1": "a", "arg2": "b"}
TEST_POLL_INTERVAL = 4.0
TEST_GCP_PROJECT_ID = "test-project"


@pytest.fixture
def trigger():
    return DataFusionStartPipelineTrigger(
        instance_url=INSTANCE_URL,
        namespace=NAMESPACE,
        pipeline_name=PIPELINE_NAME,
        pipeline_id=PIPELINE_ID,
        poll_interval=TEST_POLL_INTERVAL,
        gcp_conn_id=TEST_GCP_PROJECT_ID,
    )


class TestDataFusionStartPipelineTrigger:
    def test_start_pipeline_trigger_serialization_should_execute_successfully(self, trigger):
        """
        Asserts that the DataFusionStartPipelineTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = trigger.serialize()
        assert classpath == CLASSPATH
        assert kwargs == {
            "instance_url": INSTANCE_URL,
            "namespace": NAMESPACE,
            "pipeline_name": PIPELINE_NAME,
            "pipeline_id": PIPELINE_ID,
            "gcp_conn_id": TEST_GCP_PROJECT_ID,
            "success_states": None,
        }

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR)
    async def test_start_pipeline_trigger_on_success_should_execute_successfully(
        self, mock_pipeline_status, trigger
    ):
        """
        Tests the DataFusionStartPipelineTrigger only fires once the job execution reaches a successful state.
        """
        mock_pipeline_status.return_value = "success"
        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "success", "message": "Pipeline is running", "pipeline_id": PIPELINE_ID})
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR)
    async def test_start_pipeline_trigger_running_should_execute_successfully(
        self, mock_pipeline_status, trigger, caplog
    ):
        """
        Test that DataFusionStartPipelineTrigger does not fire while a job is still running.
        """

        mock_pipeline_status.return_value = "pending"
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False

        assert "Pipeline is not still in running state..." in caplog.text
        assert f"Sleeping for {TEST_POLL_INTERVAL} seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR)
    async def test_start_pipeline_trigger_error_should_execute_successfully(
        self, mock_pipeline_status, trigger
    ):
        """
        Test that DataFusionStartPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.return_value = "error"

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "error"}) == actual

    @pytest.mark.asyncio
    @mock.patch(HOOK_STATUS_STR)
    async def test_start_pipeline_trigger_exception_should_execute_successfully(
        self, mock_pipeline_status, trigger
    ):
        """
        Test that DataFusionStartPipelineTrigger fires the correct event in case of an error.
        """
        mock_pipeline_status.side_effect = Exception("Test exception")

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual
