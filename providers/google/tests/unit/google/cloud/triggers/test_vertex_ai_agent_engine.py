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

from airflow.providers.google.cloud.triggers.vertex_ai import AgentEngineQueryJobTrigger
from airflow.triggers.base import TriggerEvent

GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
AGENT_ENGINE_ID = "123"
QUERY_OPERATION_NAME = "projects/test-project/locations/us-central1/operations/query-123"
QUERY_OPERATION_ID = "query-123"
CHECK_QUERY_CONFIG = {"retrieve_result": True}


class FakeModel:
    def __init__(self, payload):
        self.payload = payload
        for key, value in payload.items():
            setattr(self, key, value)

    def model_dump(self, mode="json"):
        return self.payload


@pytest.fixture
def query_job_trigger():
    return AgentEngineQueryJobTrigger(
        project_id=GCP_PROJECT,
        location=GCP_LOCATION,
        operation_id=QUERY_OPERATION_ID,
        config=CHECK_QUERY_CONFIG,
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
        poll_interval=1,
        timeout=60,
    )


class TestAgentEngineQueryJobTrigger:
    def test_serialize(self, query_job_trigger):
        assert query_job_trigger.serialize() == (
            "airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineQueryJobTrigger",
            {
                "project_id": GCP_PROJECT,
                "location": GCP_LOCATION,
                "operation_id": QUERY_OPERATION_ID,
                "config": CHECK_QUERY_CONFIG,
                "gcp_conn_id": GCP_CONN_ID,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "poll_interval": 1,
                "timeout": 60,
            },
        )

    def test_serialize_with_pydantic_config(self):
        pydantic_config = FakeModel(CHECK_QUERY_CONFIG)
        trigger = AgentEngineQueryJobTrigger(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=pydantic_config,
            gcp_conn_id=GCP_CONN_ID,
            poll_interval=1,
        )
        _, kwargs = trigger.serialize()
        assert kwargs["config"] == CHECK_QUERY_CONFIG

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineAsyncHook", autospec=True)
    async def test_run_loop_return_success_event(self, mock_hook, query_job_trigger):
        query_job = {
            "operation_name": QUERY_OPERATION_NAME,
            "output_gcs_uri": "gs://test-bucket/query-output/output.json",
            "status": "SUCCESS",
            "result": "done",
        }
        mock_hook.return_value.check_query_agent_engine_job.return_value = FakeModel(query_job)

        event = await query_job_trigger.run().asend(None)

        mock_hook.return_value.check_query_agent_engine_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
        )
        assert event == TriggerEvent(
            {
                "status": "success",
                "message": "Agent Engine query job completed",
                "query_job": query_job,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.asyncio.sleep", autospec=True)
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineAsyncHook", autospec=True)
    async def test_run_loop_polls_until_success(self, mock_hook, mock_sleep, query_job_trigger):
        running_job = FakeModel({"operation_name": QUERY_OPERATION_NAME, "status": "RUNNING"})
        success_job = FakeModel({"operation_name": QUERY_OPERATION_NAME, "status": "SUCCESS"})
        mock_hook.return_value.check_query_agent_engine_job.side_effect = [
            running_job,
            running_job,
            success_job,
        ]

        event = await query_job_trigger.run().asend(None)

        assert mock_hook.return_value.check_query_agent_engine_job.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_awaited_with(1)
        assert event == TriggerEvent(
            {
                "status": "success",
                "message": "Agent Engine query job completed",
                "query_job": {"operation_name": QUERY_OPERATION_NAME, "status": "SUCCESS"},
            }
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineAsyncHook", autospec=True)
    async def test_run_loop_return_failed_event(self, mock_hook, query_job_trigger):
        query_job = {"operation_name": QUERY_OPERATION_NAME, "status": "FAILED"}
        mock_hook.return_value.check_query_agent_engine_job.return_value = FakeModel(query_job)

        event = await query_job_trigger.run().asend(None)

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": f"Agent Engine query job {QUERY_OPERATION_ID} failed.",
                "query_job": query_job,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.asyncio.sleep", autospec=True)
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineAsyncHook", autospec=True)
    async def test_run_loop_return_timeout_event(self, mock_hook, mock_sleep, query_job_trigger):
        query_job_trigger.timeout = -1
        mock_hook.return_value.check_query_agent_engine_job.return_value = FakeModel({"status": "RUNNING"})

        event = await query_job_trigger.run().asend(None)

        mock_sleep.assert_not_called()
        assert event == TriggerEvent(
            {
                "status": "timeout",
                "message": f"Timed out waiting for Agent Engine query job {QUERY_OPERATION_ID}",
                "query_job": {"status": "RUNNING"},
            }
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineAsyncHook", autospec=True)
    async def test_run_loop_return_error_event_for_unexpected_status(self, mock_hook, query_job_trigger):
        query_job = {"operation_name": QUERY_OPERATION_NAME, "status": "CANCELLED"}
        mock_hook.return_value.check_query_agent_engine_job.return_value = FakeModel(query_job)

        event = await query_job_trigger.run().asend(None)

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"Agent Engine query job {QUERY_OPERATION_ID} completed with unexpected status CANCELLED."
                ),
                "query_job": query_job,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.vertex_ai.AgentEngineAsyncHook", autospec=True)
    async def test_run_loop_return_error_event(self, mock_hook, query_job_trigger):
        mock_hook.return_value.check_query_agent_engine_job.side_effect = RuntimeError("boom")

        event = await query_job_trigger.run().asend(None)

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": "Failed while polling Agent Engine query job: boom",
                "query_job": {"operation_id": QUERY_OPERATION_ID},
            }
        )
