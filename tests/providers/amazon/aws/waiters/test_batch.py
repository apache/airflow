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

import boto3
import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook


class TestCustomBatchServiceWaiters:
    JOB_ID = "test_job_id"

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("batch", region_name="eu-west-3")
        monkeypatch.setattr(BatchClientHook, "conn", self.client)

    @pytest.fixture
    def mock_describe_jobs(self):
        """Mock ``BatchClientHook.Client.describe_jobs`` method."""
        with mock.patch.object(self.client, "describe_jobs") as m:
            yield m

    def test_service_waiters(self):
        hook_waiters = BatchClientHook(aws_conn_id=None).list_waiters()
        assert "batch_job_complete" in hook_waiters

    @staticmethod
    def describe_jobs(status: str):
        """
        Helper function for generate minimal DescribeJobs response for a single job.
        https://docs.aws.amazon.com/batch/latest/APIReference/API_DescribeJobs.html
        """
        return {
            "jobs": [
                {
                    "status": status,
                },
            ],
        }

    def test_job_succeeded(self, mock_describe_jobs):
        """Test job succeeded"""
        mock_describe_jobs.side_effect = [
            self.describe_jobs(BatchClientHook.RUNNING_STATE),
            self.describe_jobs(BatchClientHook.SUCCESS_STATE),
        ]
        waiter = BatchClientHook(aws_conn_id=None).get_waiter("batch_job_complete")
        waiter.wait(jobs=[self.JOB_ID], WaiterConfig={"Delay": 0.01, "MaxAttempts": 2})

    def test_job_failed(self, mock_describe_jobs):
        """Test job failed"""
        mock_describe_jobs.side_effect = [
            self.describe_jobs(BatchClientHook.RUNNING_STATE),
            self.describe_jobs(BatchClientHook.FAILURE_STATE),
        ]
        waiter = BatchClientHook(aws_conn_id=None).get_waiter("batch_job_complete")

        with pytest.raises(WaiterError, match="Waiter encountered a terminal failure state"):
            waiter.wait(jobs=[self.JOB_ID], WaiterConfig={"Delay": 0.01, "MaxAttempts": 2})
