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
from unittest.mock import AsyncMock

import pytest

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentHook, BedrockHook
from airflow.providers.amazon.aws.triggers.bedrock import (
    BedrockCustomizeModelCompletedTrigger,
    BedrockIngestionJobTrigger,
    BedrockKnowledgeBaseActiveTrigger,
    BedrockProvisionModelThroughputCompletedTrigger,
)
from airflow.triggers.base import TriggerEvent

from providers.tests.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.bedrock."


class TestBaseBedrockTrigger:
    EXPECTED_WAITER_NAME: str | None = None

    def test_setup(self):
        # Ensure that all subclasses have an expected waiter name set.
        if self.__class__.__name__ != "TestBaseBedrockTrigger":
            assert isinstance(self.EXPECTED_WAITER_NAME, str)


class TestBedrockCustomizeModelCompletedTrigger(TestBaseBedrockTrigger):
    EXPECTED_WAITER_NAME = "model_customization_job_complete"
    JOB_NAME = "test_job"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = BedrockCustomizeModelCompletedTrigger(job_name=self.JOB_NAME)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath == BASE_TRIGGER_CLASSPATH + "BedrockCustomizeModelCompletedTrigger"
        )
        assert kwargs.get("job_name") == self.JOB_NAME

    @pytest.mark.asyncio
    @mock.patch.object(BedrockHook, "get_waiter")
    @mock.patch.object(BedrockHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = BedrockCustomizeModelCompletedTrigger(job_name=self.JOB_NAME)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "job_name": self.JOB_NAME})
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()


class TestBedrockProvisionModelThroughputCompletedTrigger(TestBaseBedrockTrigger):
    EXPECTED_WAITER_NAME = "provisioned_model_throughput_complete"
    PROVISIONED_MODEL_ID = "provisioned_model_id"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = BedrockProvisionModelThroughputCompletedTrigger(
            provisioned_model_id=self.PROVISIONED_MODEL_ID
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == BASE_TRIGGER_CLASSPATH + "BedrockProvisionModelThroughputCompletedTrigger"
        )
        assert kwargs.get("provisioned_model_id") == self.PROVISIONED_MODEL_ID

    @pytest.mark.asyncio
    @mock.patch.object(BedrockHook, "get_waiter")
    @mock.patch.object(BedrockHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = BedrockProvisionModelThroughputCompletedTrigger(
            provisioned_model_id=self.PROVISIONED_MODEL_ID
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "provisioned_model_id": self.PROVISIONED_MODEL_ID}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()


class TestBedrockKnowledgeBaseActiveTrigger(TestBaseBedrockTrigger):
    EXPECTED_WAITER_NAME = "knowledge_base_active"
    KNOWLEDGE_BASE_NAME = "test_kb"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = BedrockKnowledgeBaseActiveTrigger(
            knowledge_base_id=self.KNOWLEDGE_BASE_NAME
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "BedrockKnowledgeBaseActiveTrigger"
        assert kwargs.get("knowledge_base_id") == self.KNOWLEDGE_BASE_NAME

    @pytest.mark.asyncio
    @mock.patch.object(BedrockAgentHook, "get_waiter")
    @mock.patch.object(BedrockAgentHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = BedrockKnowledgeBaseActiveTrigger(
            knowledge_base_id=self.KNOWLEDGE_BASE_NAME
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "knowledge_base_id": self.KNOWLEDGE_BASE_NAME}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()


class TestBedrockIngestionJobTrigger(TestBaseBedrockTrigger):
    EXPECTED_WAITER_NAME = "ingestion_job_complete"

    KNOWLEDGE_BASE_ID = "test_kb"
    DATA_SOURCE_ID = "test_ds"
    INGESTION_JOB_ID = "test_ingestion_job"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = BedrockIngestionJobTrigger(
            knowledge_base_id=self.KNOWLEDGE_BASE_ID,
            data_source_id=self.DATA_SOURCE_ID,
            ingestion_job_id=self.INGESTION_JOB_ID,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "BedrockIngestionJobTrigger"
        assert kwargs.get("knowledge_base_id") == self.KNOWLEDGE_BASE_ID
        assert kwargs.get("data_source_id") == self.DATA_SOURCE_ID
        assert kwargs.get("ingestion_job_id") == self.INGESTION_JOB_ID

    @pytest.mark.asyncio
    @mock.patch.object(BedrockAgentHook, "get_waiter")
    @mock.patch.object(BedrockAgentHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = BedrockIngestionJobTrigger(
            knowledge_base_id=self.KNOWLEDGE_BASE_ID,
            data_source_id=self.DATA_SOURCE_ID,
            ingestion_job_id=self.INGESTION_JOB_ID,
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        assert response == TriggerEvent(
            {"status": "success", "ingestion_job_id": self.INGESTION_JOB_ID}
        )
        mock_get_waiter().wait.assert_called_once()
