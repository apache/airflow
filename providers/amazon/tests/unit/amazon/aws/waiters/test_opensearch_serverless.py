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

from airflow.providers.amazon.aws.hooks.opensearch_serverless import OpenSearchServerlessHook
from airflow.providers.amazon.aws.sensors.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveSensor,
)


class TestOpenSearchServerlessCustomWaiters:
    def test_service_waiters(self):
        assert "collection_available" in OpenSearchServerlessHook().list_waiters()


class TestOpenSearchServerlessCustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("opensearchserverless")
        monkeypatch.setattr(OpenSearchServerlessHook, "conn", self.client)


class TestCollectionAvailableWaiter(TestOpenSearchServerlessCustomWaitersBase):
    WAITER_NAME = "collection_available"

    @pytest.fixture
    def mock_getter(self):
        with mock.patch.object(self.client, "batch_get_collection") as getter:
            yield getter

    @pytest.mark.parametrize("state", OpenSearchServerlessCollectionActiveSensor.SUCCESS_STATES)
    def test_model_customization_job_complete(self, state, mock_getter):
        mock_getter.return_value = {"collectionDetails": [{"status": state}]}

        OpenSearchServerlessHook().get_waiter(self.WAITER_NAME).wait(collection_id="collection_id")

    @pytest.mark.parametrize("state", OpenSearchServerlessCollectionActiveSensor.FAILURE_STATES)
    def test_model_customization_job_failed(self, state, mock_getter):
        mock_getter.return_value = {"collectionDetails": [{"status": state}]}

        with pytest.raises(botocore.exceptions.WaiterError):
            OpenSearchServerlessHook().get_waiter(self.WAITER_NAME).wait(collection_id="collection_id")

    def test_model_customization_job_wait(self, mock_getter):
        wait = {"collectionDetails": [{"status": "CREATING"}]}
        success = {"collectionDetails": [{"status": "ACTIVE"}]}
        mock_getter.side_effect = [wait, wait, success]

        OpenSearchServerlessHook().get_waiter(self.WAITER_NAME).wait(
            collection_id="collection_id", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )
