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

from airflow.providers.amazon.aws.hooks.glue_databrew import GlueDataBrewHook

RUNNING_STATES = ["STARTING", "RUNNING", "STOPPING"]
TERMINAL_STATES = ["STOPPED", "SUCCEEDED", "FAILED"]


class TestCustomDataBrewWaiters:
    """Test waiters from ``amazon/aws/waiters/glue.json``."""

    JOB_NAME = "test_job"
    RUN_ID = "123"

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("databrew", region_name="eu-west-3")
        monkeypatch.setattr(GlueDataBrewHook, "conn", self.client)

    def test_service_waiters(self):
        hook_waiters = GlueDataBrewHook(aws_conn_id=None).list_waiters()
        assert "job_complete" in hook_waiters

    @pytest.fixture
    def mock_describe_job_runs(self):
        """Mock ``GlueDataBrewHook.Client.describe_job_run`` method."""
        with mock.patch.object(self.client, "describe_job_run") as m:
            yield m

    @staticmethod
    def describe_jobs(status: str):
        """
        Helper function for generate minimal DescribeJobRun response for a single job.

        https://docs.aws.amazon.com/databrew/latest/dg/API_DescribeJobRun.html
        """
        return {"State": status}

    def test_job_succeeded(self, mock_describe_job_runs):
        """Test job succeeded"""
        mock_describe_job_runs.side_effect = [
            self.describe_jobs(RUNNING_STATES[1]),
            self.describe_jobs(TERMINAL_STATES[1]),
        ]
        waiter = GlueDataBrewHook(aws_conn_id=None).get_waiter("job_complete")
        waiter.wait(
            name=self.JOB_NAME,
            runId=self.RUN_ID,
            WaiterConfig={"Delay": 0.2, "MaxAttempts": 2},
        )
