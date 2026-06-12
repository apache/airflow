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

from unittest import mock

import pytest

from airflow.providers.google.cloud.triggers.vertex_ai import AgentEngineDeleteTrigger
from airflow.triggers.base import TriggerEvent

GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
AGENT_ENGINE_NAME = "projects/test-project/locations/us-central1/reasoningEngines/123"


@pytest.fixture
def delete_trigger():
    return AgentEngineDeleteTrigger(
        project_id=GCP_PROJECT,
        location=GCP_LOCATION,
        name=AGENT_ENGINE_NAME,
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
        poll_interval=1,
        timeout=60,
    )


class TestAgentEngineDeleteTrigger:
    def test_serialize(self, delete_trigger):
        assert delete_trigger.serialize() == (
            "airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineDeleteTrigger",
            {
                "project_id": GCP_PROJECT,
                "location": GCP_LOCATION,
                "name": AGENT_ENGINE_NAME,
                "gcp_conn_id": GCP_CONN_ID,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "poll_interval": 1,
                "timeout": 60,
            },
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineHook", autospec=True)
    async def test_run_loop_return_success_event(self, mock_hook, delete_trigger):
        mock_hook.return_value.is_agent_engine_deleted.return_value = True

        event = await delete_trigger.run().asend(None)

        mock_hook.return_value.is_agent_engine_deleted.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
        )
        assert event == TriggerEvent(
            {
                "status": "success",
                "message": "Agent Engine deleted",
                "name": AGENT_ENGINE_NAME,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.asyncio.sleep", autospec=True)
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineHook", autospec=True)
    async def test_run_loop_return_timeout_event(self, mock_hook, mock_sleep, delete_trigger):
        delete_trigger.timeout = -1
        mock_hook.return_value.is_agent_engine_deleted.return_value = False

        event = await delete_trigger.run().asend(None)

        mock_sleep.assert_not_called()
        assert event == TriggerEvent(
            {
                "status": "error",
                "message": f"Timed out waiting for Agent Engine {AGENT_ENGINE_NAME} to be deleted",
                "name": AGENT_ENGINE_NAME,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineHook", autospec=True)
    async def test_run_loop_return_error_event(self, mock_hook, delete_trigger):
        mock_hook.return_value.is_agent_engine_deleted.side_effect = RuntimeError("boom")

        event = await delete_trigger.run().asend(None)

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": "boom",
                "name": AGENT_ENGINE_NAME,
            }
        )
