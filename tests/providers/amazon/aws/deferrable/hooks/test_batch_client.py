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

import sys

import botocore
import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientAsyncHook

if sys.version_info < (3, 8):
    # For compatibility with Python 3.7
    from asynctest import mock as async_mock
else:
    from unittest import mock as async_mock

pytest.importorskip("aiobotocore")


class TestBatchClientAsyncHook:
    JOB_ID = "e2a459c5-381b-494d-b6e8-d6ee334db4e2"
    BATCH_API_SUCCESS_RESPONSE = {"jobs": [{"jobId": JOB_ID, "status": "SUCCEEDED"}]}

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.poll_job_status")
    async def test_monitor_job_with_success(self, mock_poll_job_status, mock_client):
        """Tests that the  monitor_job method returns expected event once successful"""
        mock_poll_job_status.return_value = True
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = (
            self.BATCH_API_SUCCESS_RESPONSE
        )
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None)
        result = await hook.monitor_job()
        assert result == {"status": "success", "message": f"AWS Batch job ({self.JOB_ID}) succeeded"}

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.poll_job_status")
    async def test_monitor_job_with_no_job_id(self, mock_poll_job_status, mock_client):
        """Tests that the monitor_job method raises expected exception when incorrect job id is passed"""
        mock_poll_job_status.return_value = True
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = (
            self.BATCH_API_SUCCESS_RESPONSE
        )

        with pytest.raises(AirflowException) as exc_info:
            hook = BatchClientAsyncHook(job_id=False, waiters=None)
            await hook.monitor_job()
        assert str(exc_info.value) == "AWS Batch job - job_id was not found"

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.poll_job_status")
    async def test_hit_api_throttle(self, mock_poll_job_status, mock_client):
        """
        Tests that the get_job_description method raises  correct exception when retries
        exceed the threshold
        """
        mock_poll_job_status.return_value = True
        mock_client.return_value.__aenter__.return_value.describe_jobs.side_effect = (
            botocore.exceptions.ClientError(
                error_response={
                    "Error": {
                        "Code": "TooManyRequestsException",
                    }
                },
                operation_name="get job description",
            )
        )
        """status_retries = 2 ensures that exponential_delay block is covered in batch_client.py
        otherwise the code coverage will drop"""
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None, status_retries=2)
        with pytest.raises(AirflowException) as exc_info:
            await hook.get_job_description(job_id=self.JOB_ID)
        assert (
            str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) description error: exceeded "
            "status_retries (2)"
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.poll_job_status")
    async def test_client_error(self, mock_poll_job_status, mock_client):
        """Test that the get_job_description method raises  correct exception when the error code
        from boto3 api is not TooManyRequestsException"""
        mock_poll_job_status.return_value = True
        mock_client.return_value.__aenter__.return_value.describe_jobs.side_effect = (
            botocore.exceptions.ClientError(
                error_response={"Error": {"Code": "InvalidClientTokenId", "Message": "Malformed Token"}},
                operation_name="get job description",
            )
        )
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None, status_retries=1)
        with pytest.raises(AirflowException) as exc_info:
            await hook.get_job_description(job_id=self.JOB_ID)
        assert (
            str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) description error: An error "
            "occurred (InvalidClientTokenId) when calling the get job description operation: "
            "Malformed Token"
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_check_job_success(self, mock_client):
        """Tests that the check_job_success method returns True when job succeeds"""
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = (
            self.BATCH_API_SUCCESS_RESPONSE
        )
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None)
        result = await hook.check_job_success(job_id=self.JOB_ID)
        assert result is True

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_check_job_raises_exception_failed(self, mock_client):
        """Tests that the check_job_success method raises exception correctly as per job state"""
        mock_job = {"jobs": [{"jobId": self.JOB_ID, "status": "FAILED"}]}
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = mock_job
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None)
        with pytest.raises(AirflowException) as exc_info:
            await hook.check_job_success(job_id=self.JOB_ID)
        assert str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) failed" + ": " + str(
            mock_job["jobs"][0]
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_check_job_raises_exception_pending(self, mock_client):
        """Tests that the check_job_success method raises exception correctly as per job state"""
        mock_job = {"jobs": [{"jobId": self.JOB_ID, "status": "PENDING"}]}
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = mock_job
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None)
        with pytest.raises(AirflowException) as exc_info:
            await hook.check_job_success(job_id=self.JOB_ID)
        assert str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) is not complete" + ": " + str(
            mock_job["jobs"][0]
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_check_job_raises_exception_strange(self, mock_client):
        """Tests that the check_job_success method raises exception correctly as per job state"""
        mock_job = {"jobs": [{"jobId": self.JOB_ID, "status": "STRANGE"}]}
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = mock_job
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None)
        with pytest.raises(AirflowException) as exc_info:
            await hook.check_job_success(job_id=self.JOB_ID)
        assert str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) has unknown status" + ": " + str(
            mock_job["jobs"][0]
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_check_job_raises_exception_runnable(self, mock_client):
        """Tests that the check_job_success method raises exception correctly as per job state"""
        mock_job = {"jobs": [{"jobId": self.JOB_ID, "status": "RUNNABLE"}]}
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = mock_job
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None)
        with pytest.raises(AirflowException) as exc_info:
            await hook.check_job_success(job_id=self.JOB_ID)
        assert str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) is not complete" + ": " + str(
            mock_job["jobs"][0]
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_check_job_raises_exception_submitted(self, mock_client):
        """Tests that the check_job_success method raises exception correctly as per job state"""
        mock_job = {"jobs": [{"jobId": self.JOB_ID, "status": "SUBMITTED"}]}
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = mock_job
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None)
        with pytest.raises(AirflowException) as exc_info:
            await hook.check_job_success(job_id=self.JOB_ID)
        assert str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) is not complete" + ": " + str(
            mock_job["jobs"][0]
        )

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_poll_job_status_raises_for_max_retries(self, mock_client):
        mock_job = {"jobs": [{"jobId": self.JOB_ID, "status": "RUNNABLE"}]}
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = mock_job
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None, max_retries=1)
        with pytest.raises(AirflowException) as exc_info:
            await hook.poll_job_status(job_id=self.JOB_ID, match_status=["SUCCEEDED"])
        assert str(exc_info.value) == f"AWS Batch job ({self.JOB_ID}) status checks exceed " "max_retries"

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientAsyncHook.get_client_async")
    async def test_poll_job_status_in_match_status(self, mock_client):
        mock_job = self.BATCH_API_SUCCESS_RESPONSE
        mock_client.return_value.__aenter__.return_value.describe_jobs.return_value = mock_job
        hook = BatchClientAsyncHook(job_id=self.JOB_ID, waiters=None, max_retries=1)
        result = await hook.poll_job_status(job_id=self.JOB_ID, match_status=["SUCCEEDED"])
        assert result is True
