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

from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook

INGESTION_WAITER_ARGS = {
    "AwsAccountId": "123456789012",
    "DataSetId": "DemoDataSet",
    "IngestionId": "DemoDataSet_Ingestion",
}


class TestQuickSightCustomWaiters:
    def test_service_waiters(self):
        assert "ingestion_complete" in QuickSightHook().list_waiters()


class TestQuickSightCustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("quicksight")
        monkeypatch.setattr(QuickSightHook, "conn", self.client)


class TestQuickSightIngestionCompleteWaiter(TestQuickSightCustomWaitersBase):
    WAITER_NAME = "ingestion_complete"

    @pytest.fixture
    def mock_describe_ingestion(self):
        with mock.patch.object(self.client, "describe_ingestion") as mock_getter:
            yield mock_getter

    def test_ingestion_complete(self, mock_describe_ingestion):
        mock_describe_ingestion.return_value = {"Ingestion": {"IngestionStatus": "COMPLETED"}}

        QuickSightHook().get_waiter(self.WAITER_NAME).wait(**INGESTION_WAITER_ARGS)

    @pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
    def test_ingestion_failed(self, state, mock_describe_ingestion):
        mock_describe_ingestion.return_value = {"Ingestion": {"IngestionStatus": state}}

        with pytest.raises(botocore.exceptions.WaiterError):
            QuickSightHook().get_waiter(self.WAITER_NAME).wait(**INGESTION_WAITER_ARGS)

    def test_ingestion_wait(self, mock_describe_ingestion):
        mock_describe_ingestion.side_effect = [
            {"Ingestion": {"IngestionStatus": "INITIALIZED"}},
            {"Ingestion": {"IngestionStatus": "QUEUED"}},
            {"Ingestion": {"IngestionStatus": "RUNNING"}},
            {"Ingestion": {"IngestionStatus": "COMPLETED"}},
        ]

        QuickSightHook().get_waiter(self.WAITER_NAME).wait(
            **INGESTION_WAITER_ARGS, WaiterConfig={"Delay": 0.01, "MaxAttempts": 4}
        )
