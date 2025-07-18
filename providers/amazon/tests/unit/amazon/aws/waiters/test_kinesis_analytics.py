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

from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.sensors.kinesis_analytics import (
    KinesisAnalyticsV2StartApplicationCompletedSensor,
    KinesisAnalyticsV2StopApplicationCompletedSensor,
)


class TestKinesisAnalyticsV2CustomWaiters:
    def test_service_waiters(self):
        assert "application_start_complete" in KinesisAnalyticsV2Hook().list_waiters()
        assert "application_stop_complete" in KinesisAnalyticsV2Hook().list_waiters()


class TestKinesisAnalyticsV2CustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("kinesisanalyticsv2")
        monkeypatch.setattr(KinesisAnalyticsV2Hook, "conn", self.client)


class TestKinesisAnalyticsV2ApplicationStartWaiter(TestKinesisAnalyticsV2CustomWaitersBase):
    APPLICATION_NAME = "demo"
    WAITER_NAME = "application_start_complete"

    @pytest.fixture
    def mock_describe_application(self):
        with mock.patch.object(self.client, "describe_application") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", KinesisAnalyticsV2StartApplicationCompletedSensor.SUCCESS_STATES)
    def test_start_application_complete(self, state, mock_describe_application):
        mock_describe_application.return_value = {"ApplicationDetail": {"ApplicationStatus": state}}

        KinesisAnalyticsV2Hook().get_waiter(self.WAITER_NAME).wait(ApplicationName=self.APPLICATION_NAME)

    @pytest.mark.parametrize("state", KinesisAnalyticsV2StartApplicationCompletedSensor.FAILURE_STATES)
    def test_start_application_complete_failed(self, state, mock_describe_application):
        mock_describe_application.return_value = {"ApplicationDetail": {"ApplicationStatus": state}}
        with pytest.raises(botocore.exceptions.WaiterError):
            KinesisAnalyticsV2Hook().get_waiter(self.WAITER_NAME).wait(ApplicationName=self.APPLICATION_NAME)

    def test_start_application_complete_wait(self, mock_describe_application):
        wait = {"ApplicationDetail": {"ApplicationStatus": "STARTING"}}
        success = {"ApplicationDetail": {"ApplicationStatus": "RUNNING"}}

        mock_describe_application.side_effect = [wait, wait, success]

        KinesisAnalyticsV2Hook().get_waiter(self.WAITER_NAME).wait(
            ApplicationName=self.APPLICATION_NAME, WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )


class TestKinesisAnalyticsV2ApplicationStopWaiter(TestKinesisAnalyticsV2CustomWaitersBase):
    APPLICATION_NAME = "demo"
    WAITER_NAME = "application_stop_complete"

    @pytest.fixture
    def mock_describe_application(self):
        with mock.patch.object(self.client, "describe_application") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", KinesisAnalyticsV2StopApplicationCompletedSensor.SUCCESS_STATES)
    def test_stop_application_complete(self, state, mock_describe_application):
        mock_describe_application.return_value = {"ApplicationDetail": {"ApplicationStatus": state}}

        KinesisAnalyticsV2Hook().get_waiter(self.WAITER_NAME).wait(ApplicationName=self.APPLICATION_NAME)

    @pytest.mark.parametrize("state", KinesisAnalyticsV2StopApplicationCompletedSensor.FAILURE_STATES)
    def test_stop_application_complete_failed(self, state, mock_describe_application):
        mock_describe_application.return_value = {"ApplicationDetail": {"ApplicationStatus": state}}
        with pytest.raises(botocore.exceptions.WaiterError):
            KinesisAnalyticsV2Hook().get_waiter(self.WAITER_NAME).wait(ApplicationName=self.APPLICATION_NAME)

    def test_stop_application_complete_wait(self, mock_describe_application):
        wait = {"ApplicationDetail": {"ApplicationStatus": "STOPPING"}}
        success = {"ApplicationDetail": {"ApplicationStatus": "READY"}}

        mock_describe_application.side_effect = [wait, wait, success]

        KinesisAnalyticsV2Hook().get_waiter(self.WAITER_NAME).wait(
            ApplicationName=self.APPLICATION_NAME, WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )
