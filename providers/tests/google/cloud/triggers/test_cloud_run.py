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
from google.protobuf.any_pb2 import Any
from google.rpc.status_pb2 import Status

from airflow.providers.google.cloud.triggers.cloud_run import (
    CloudRunJobFinishedTrigger,
    RunJobStatus,
)
from airflow.triggers.base import TriggerEvent

OPERATION_NAME = "operation"
JOB_NAME = "jobName"
ERROR_CODE = 13
ERROR_MESSAGE = "Some message"
PROJECT_ID = "projectId"
LOCATION = "us-central1"
GCP_CONNECTION_ID = "gcp_connection_id"
POLL_SLEEP = 0.01
TIMEOUT = 0.02
IMPERSONATION_CHAIN = "impersonation_chain"


@pytest.fixture
def trigger():
    return CloudRunJobFinishedTrigger(
        operation_name=OPERATION_NAME,
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONNECTION_ID,
        polling_period_seconds=POLL_SLEEP,
        timeout=TIMEOUT,
        impersonation_chain=IMPERSONATION_CHAIN,
    )


class TestCloudBatchJobFinishedTrigger:
    def test_serialization(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.cloud.triggers.cloud_run.CloudRunJobFinishedTrigger"
        )
        assert kwargs == {
            "project_id": PROJECT_ID,
            "operation_name": OPERATION_NAME,
            "job_name": JOB_NAME,
            "location": LOCATION,
            "gcp_conn_id": GCP_CONNECTION_ID,
            "polling_period_seconds": POLL_SLEEP,
            "timeout": TIMEOUT,
            "impersonation_chain": IMPERSONATION_CHAIN,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_run.CloudRunAsyncHook")
    async def test_trigger_on_operation_completed_yield_successfully(
        self, mock_hook, trigger: CloudRunJobFinishedTrigger
    ):
        """
        Tests the CloudRunJobFinishedTrigger fires once the job execution reaches a successful state.
        """

        async def _mock_operation(name):
            operation = mock.MagicMock()
            operation.done = True
            operation.name = "name"
            operation.error = Any()
            operation.error.ParseFromString(b"")
            return operation

        mock_hook.return_value.get_operation = _mock_operation
        generator = trigger.run()
        actual = await generator.asend(None)  # type:ignore[attr-defined]
        assert (
            TriggerEvent(
                {
                    "status": RunJobStatus.SUCCESS.value,
                    "job_name": JOB_NAME,
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_run.CloudRunAsyncHook")
    async def test_trigger_on_operation_failed_yield_error(
        self, mock_hook, trigger: CloudRunJobFinishedTrigger
    ):
        """
        Tests the CloudRunJobFinishedTrigger raises an exception once the job execution fails.
        """

        async def _mock_operation(name):
            operation = mock.MagicMock()
            operation.done = True
            operation.name = "name"
            operation.error = Status(code=13, message="Some message")
            return operation

        mock_hook.return_value.get_operation = _mock_operation
        generator = trigger.run()

        actual = await generator.asend(None)  # type:ignore[attr-defined]
        assert (
            TriggerEvent(
                {
                    "status": RunJobStatus.FAIL.value,
                    "operation_error_code": ERROR_CODE,
                    "operation_error_message": ERROR_MESSAGE,
                    "job_name": JOB_NAME,
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_run.CloudRunAsyncHook")
    async def test_trigger_timeout(self, mock_hook, trigger: CloudRunJobFinishedTrigger):
        """
        Tests the CloudRunJobFinishedTrigger fires once the job execution times out with an error message.
        """

        async def _mock_operation(name):
            operation = mock.MagicMock()
            operation.done = False
            operation.error = mock.MagicMock()
            operation.error.message = None
            operation.error.code = None
            return operation

        mock_hook.return_value.get_operation = _mock_operation

        generator = trigger.run()
        actual = await generator.asend(None)  # type:ignore[attr-defined]

        assert (
            TriggerEvent(
                {
                    "status": RunJobStatus.TIMEOUT,
                    "job_name": JOB_NAME,
                }
            )
            == actual
        )
