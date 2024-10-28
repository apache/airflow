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

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook
from airflow.providers.amazon.aws.sensors.bedrock import (
    BedrockIngestionJobSensor,
    BedrockKnowledgeBaseActiveSensor,
)


class TestBedrockAgentCustomWaiters:
    def test_service_waiters(self):
        assert "knowledge_base_active" in BedrockAgentHook().list_waiters()
        assert "ingestion_job_complete" in BedrockAgentHook().list_waiters()


class TestBedrockAgentCustomWaitersBase:
    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("bedrock-agent")
        monkeypatch.setattr(BedrockAgentHook, "conn", self.client)


class TestKnowledgeBaseActiveWaiter(TestBedrockAgentCustomWaitersBase):
    WAITER_NAME = "knowledge_base_active"
    WAITER_ARGS = {"knowledgeBaseId": "kb_id"}
    SENSOR = BedrockKnowledgeBaseActiveSensor

    @pytest.fixture
    def mock_getter(self):
        with mock.patch.object(self.client, "get_knowledge_base") as getter:
            yield getter

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    def test_knowledge_base_active_complete(self, state, mock_getter):
        mock_getter.return_value = {"knowledgeBase": {"status": state}}

        BedrockAgentHook().get_waiter(self.WAITER_NAME).wait(**self.WAITER_ARGS)

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    def test_knowledge_base_active_failed(self, state, mock_getter):
        mock_getter.return_value = {"knowledgeBase": {"status": state}}

        with pytest.raises(botocore.exceptions.WaiterError):
            BedrockAgentHook().get_waiter(self.WAITER_NAME).wait(**self.WAITER_ARGS)

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    def test_knowledge_base_active_wait(self, state, mock_getter):
        wait = {"knowledgeBase": {"status": state}}
        success = {"knowledgeBase": {"status": "ACTIVE"}}
        mock_getter.side_effect = [wait, wait, success]

        BedrockAgentHook().get_waiter(self.WAITER_NAME).wait(
            **self.WAITER_ARGS,
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )


class TestIngestionJobWaiter(TestBedrockAgentCustomWaitersBase):
    WAITER_NAME = "ingestion_job_complete"
    WAITER_ARGS = {
        "knowledgeBaseId": "kb_id",
        "dataSourceId": "ds_id",
        "ingestionJobId": "job_id",
    }
    SENSOR = BedrockIngestionJobSensor

    @pytest.fixture
    def mock_getter(self):
        with mock.patch.object(self.client, "get_ingestion_job") as getter:
            yield getter

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    def test_knowledge_base_active_complete(self, state, mock_getter):
        mock_getter.return_value = {"ingestionJob": {"status": state}}

        BedrockAgentHook().get_waiter(self.WAITER_NAME).wait(**self.WAITER_ARGS)

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    def test_knowledge_base_active_failed(self, state, mock_getter):
        mock_getter.return_value = {"ingestionJob": {"status": state}}

        with pytest.raises(botocore.exceptions.WaiterError):
            BedrockAgentHook().get_waiter(self.WAITER_NAME).wait(**self.WAITER_ARGS)

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    def test_knowledge_base_active_wait(self, state, mock_getter):
        wait = {"ingestionJob": {"status": state}}
        success = {"ingestionJob": {"status": "COMPLETE"}}
        mock_getter.side_effect = [wait, wait, success]

        BedrockAgentHook().get_waiter(self.WAITER_NAME).wait(
            **self.WAITER_ARGS, WaiterConfig={"Delay": 0.01, "MaxAttempts": 3}
        )
