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
import importlib.util

import pytest

from airflow.providers.amazon.aws.triggers.batch import (
    BatchOperatorTrigger,
)
from airflow.triggers.base import TriggerEvent
from tests.providers.amazon.aws.utils.compat import async_mock

JOB_NAME = "51455483-c62c-48ac-9b88-53a6a725baa3"
JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
MAX_RETRIES = 2
STATUS_RETRIES = 3
POKE_INTERVAL = 5
AWS_CONN_ID = "airflow_test"
REGION_NAME = "eu-west-1"


@pytest.mark.skipif(not bool(importlib.util.find_spec("aiobotocore")), reason="aiobotocore require")
class TestBatchOperatorTrigger:
    TRIGGER = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )

    def test_batch_trigger_serialization(self):
        """
        Asserts that the BatchOperatorTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.batch.BatchOperatorTrigger"
        assert kwargs == {
            "job_id": JOB_ID,
            "job_name": JOB_NAME,
            "job_definition": "hello-world",
            "job_queue": "queue",
            "waiters": None,
            "tags": {},
            "max_retries": MAX_RETRIES,
            "status_retries": STATUS_RETRIES,
            "parameters": {},
            "overrides": {},
            "array_properties": {},
            "region_name": "eu-west-1",
            "aws_conn_id": "airflow_test",
        }

    @pytest.mark.asyncio
    async def test_batch_trigger_run(self):
        """Test that the task is not done when event is not returned from trigger."""

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)
        # TriggerEvent was not returned
        assert task.done() is False

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.monitor_job")
    async def test_batch_trigger_completed(self, mock_response):
        """Test if the success event is  returned from trigger."""
        mock_response.return_value = {"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"}

        generator = self.TRIGGER.run()
        actual_response = await generator.asend(None)
        assert (
            TriggerEvent({"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"})
            == actual_response
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.monitor_job")
    async def test_batch_trigger_failure(self, mock_response):
        """Test if the failure event is returned from trigger."""
        mock_response.return_value = {"status": "error", "message": f"{JOB_ID} failed"}

        generator = self.TRIGGER.run()
        actual_response = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": f"{JOB_ID} failed"}) == actual_response

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.monitor_job")
    async def test_batch_trigger_none(self, mock_response):
        """Test if the failure event is returned when there is no response from hook."""
        mock_response.return_value = None

        generator = self.TRIGGER.run()
        actual_response = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": f"{JOB_ID} failed"}) == actual_response

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.monitor_job")
    async def test_batch_trigger_exception(self, mock_response):
        """Test if the exception is raised from trigger."""
        mock_response.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
