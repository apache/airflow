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

from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook
from airflow.providers.amazon.aws.triggers.bedrock import BedrockCustomizeModelCompletedTrigger
from airflow.triggers.base import TriggerEvent

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.bedrock."


class TestBedrockCustomizeModelCompletedTrigger:
    JOB_NAME = "test_job"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = BedrockCustomizeModelCompletedTrigger(job_name=self.JOB_NAME)
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "BedrockCustomizeModelCompletedTrigger"
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
        assert mock_get_waiter().wait.call_count == 1
