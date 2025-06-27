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
import botocore
import pytest

from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.sensors.glue import (
    GlueDataQualityRuleRecommendationRunSensor,
    GlueDataQualityRuleSetEvaluationRunSensor,
)


class TestGlueDataQualityCustomWaiters:
    def test_evaluation_run_waiters(self):
        assert "data_quality_ruleset_evaluation_run_complete" in GlueDataQualityHook().list_waiters()

    def test_recommendation_run_waiters(self):
        assert "data_quality_rule_recommendation_run_complete" in GlueDataQualityHook().list_waiters()


class TestGlueDataQualityCustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("glue")
        monkeypatch.setattr(GlueDataQualityHook, "conn", self.client)


class TestGlueDataQualityRuleSetEvaluationRunCompleteWaiter(TestGlueDataQualityCustomWaitersBase):
    WAITER_NAME = "data_quality_ruleset_evaluation_run_complete"

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "get_data_quality_ruleset_evaluation_run") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", GlueDataQualityRuleSetEvaluationRunSensor.SUCCESS_STATES)
    def test_data_quality_ruleset_evaluation_run_complete(self, state, mock_get_job):
        mock_get_job.return_value = {"Status": state}

        GlueDataQualityHook().get_waiter(self.WAITER_NAME).wait(RunId="run_id")

    @pytest.mark.parametrize("state", GlueDataQualityRuleSetEvaluationRunSensor.FAILURE_STATES)
    def test_data_quality_ruleset_evaluation_run_failed(self, state, mock_get_job):
        mock_get_job.return_value = {"Status": state}

        with pytest.raises(botocore.exceptions.WaiterError):
            GlueDataQualityHook().get_waiter(self.WAITER_NAME).wait(RunId="run_id")

    def test_data_quality_ruleset_evaluation_run_wait(self, mock_get_job):
        wait = {"Status": "RUNNING"}
        success = {"Status": "SUCCEEDED"}
        mock_get_job.side_effect = [wait, wait, success]

        GlueDataQualityHook().get_waiter(self.WAITER_NAME).wait(
            RunIc="run_id", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )


class TestGlueDataQualityRuleRecommendationRunCompleteWaiter(TestGlueDataQualityCustomWaitersBase):
    WAITER_NAME = "data_quality_rule_recommendation_run_complete"

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "get_data_quality_rule_recommendation_run") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", GlueDataQualityRuleRecommendationRunSensor.SUCCESS_STATES)
    def test_data_quality_rule_recommendation_run_complete(self, state, mock_get_job):
        mock_get_job.return_value = {"Status": state}

        GlueDataQualityHook().get_waiter(self.WAITER_NAME).wait(RunId="run_id")

    @pytest.mark.parametrize("state", GlueDataQualityRuleRecommendationRunSensor.FAILURE_STATES)
    def test_data_quality_rule_recommendation_run_failed(self, state, mock_get_job):
        mock_get_job.return_value = {"Status": state}

        with pytest.raises(botocore.exceptions.WaiterError):
            GlueDataQualityHook().get_waiter(self.WAITER_NAME).wait(RunId="run_id")

    def test_data_quality_rule_recommendation_run_wait(self, mock_get_job):
        wait = {"Status": "RUNNING"}
        success = {"Status": "SUCCEEDED"}
        mock_get_job.side_effect = [wait, wait, success]

        GlueDataQualityHook().get_waiter(self.WAITER_NAME).wait(
            RunIc="run_id", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )


class TestGlueJobCompleteCustomWaiterBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("glue")
        monkeypatch.setattr(GlueJobHook, "conn", self.client)


class TestGlueJobCompleteWaiter(TestGlueJobCompleteCustomWaiterBase):
    WAITER_NAME = "job_complete"

    @pytest.fixture
    def mock_get_job(self):
        with mock.patch.object(self.client, "get_job_run") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", ["SUCCEEDED"])
    def test_glue_job_run_success(self, state, mock_get_job):
        mock_get_job.return_value = {"JobRun": {"JobRunState": state}}

        GlueJobHook().get_waiter(self.WAITER_NAME).wait(JobName="example", RunId="run_id")

    @pytest.mark.parametrize("state", ["STOPPED", "FAILED", "ERROR", "TIMEOUT"])
    def test_glue_job_run_failure(self, state, mock_get_job):
        mock_get_job.return_value = {"JobRun": {"JobRunState": state}}

        with pytest.raises(botocore.exceptions.WaiterError):
            GlueJobHook().get_waiter(self.WAITER_NAME).wait(JobName="example", RunId="run_id")

    @pytest.mark.parametrize("intermediate", ["STARTING", "RUNNING", "STOPPING"])
    def test_glue_job_run_retry_then_success(self, intermediate, mock_get_job):
        mock_get_job.side_effect = [
            {"JobRun": {"JobRunState": intermediate}},
            {"JobRun": {"JobRunState": intermediate}},
            {"JobRun": {"JobRunState": "SUCCEEDED"}},
        ]

        GlueJobHook().get_waiter(self.WAITER_NAME).wait(
            JobName="example",
            RunId="run_id",
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )
