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

from airflow.providers.google.cloud.hooks.vertex_ai.agent_engine import AgentEngineAsyncHook, AgentEngineHook

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
AGENT_ENGINE_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.agent_engine.{}"

TEST_GCP_CONN_ID = "test-gcp-conn-id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"
AGENT_ENGINE_ID = "123"
AGENT_ENGINE_NAME = "projects/test-project/locations/us-central1/reasoningEngines/123"
OPERATION_NAME = "projects/test-project/locations/us-central1/operations/delete-123"
QUERY_OPERATION_NAME = "projects/test-project/locations/us-central1/operations/query-123"
OPERATION_ID = "delete-123"
QUERY_OPERATION_ID = "query-123"
CONFIG = {"display_name": "test-agent-engine"}
QUERY_CONFIG = {"query": "hello", "output_gcs_uri": "gs://test-bucket/query-output/"}
CHECK_QUERY_CONFIG = {"retrieve_result": True}


class TestAgentEngineHookWithDefaultProjectId:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = AgentEngineHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(AGENT_ENGINE_STRING.format("Client"), autospec=True)
    def test_get_agent_engine_client(self, mock_client):
        self.hook.get_credentials = mock.Mock(return_value=mock.sentinel.credentials, spec=())

        result = self.hook.get_agent_engine_client(project_id=GCP_PROJECT, location=GCP_LOCATION)

        mock_client.assert_called_once_with(
            project=GCP_PROJECT,
            location=GCP_LOCATION,
            credentials=mock.sentinel.credentials,
        )
        assert result == mock_client.return_value.agent_engines

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_create_agent_engine(self, mock_get_client):
        result = self.hook.create_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            config=CONFIG,
        )

        mock_get_client.assert_called_once_with(self.hook, project_id=GCP_PROJECT, location=GCP_LOCATION)
        mock_get_client.return_value.create.assert_called_once_with(
            agent=None,
            config=CONFIG,
        )
        assert result == mock_get_client.return_value.create.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_get_agent_engine(self, mock_get_client):
        result = self.hook.get_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
        )

        mock_get_client.return_value.get.assert_called_once_with(name=AGENT_ENGINE_NAME, config=None)
        assert result == mock_get_client.return_value.get.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_get_agent_engine_with_config(self, mock_get_client):
        result = self.hook.get_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=CONFIG,
        )

        mock_get_client.return_value.get.assert_called_once_with(name=AGENT_ENGINE_NAME, config=CONFIG)
        assert result == mock_get_client.return_value.get.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_run_query_job(self, mock_get_client):
        result = self.hook.run_query_job(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=QUERY_CONFIG,
        )

        mock_get_client.return_value.run_query_job.assert_called_once_with(
            name=AGENT_ENGINE_NAME,
            config=QUERY_CONFIG,
        )
        assert result == mock_get_client.return_value.run_query_job.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_check_query_agent_engine_job(self, mock_get_client):
        result = self.hook.check_query_agent_engine_job(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
        )

        mock_get_client.return_value.check_query_job.assert_called_once_with(
            name=QUERY_OPERATION_NAME,
            config=CHECK_QUERY_CONFIG,
        )
        assert result == mock_get_client.return_value.check_query_job.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.check_query_agent_engine_job"), autospec=True)
    def test_wait_for_query_agent_engine_job_returns_when_successful(self, mock_check_query_job):
        mock_check_query_job.return_value.status = "SUCCESS"

        result = self.hook.wait_for_query_agent_engine_job(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
        )

        mock_check_query_job.assert_called_once_with(
            self.hook,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
        )
        assert result == mock_check_query_job.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("time.sleep"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.check_query_agent_engine_job"), autospec=True)
    def test_wait_for_query_agent_engine_job_polls_until_success(self, mock_check_query_job, mock_sleep):
        running_job = mock.Mock(status="RUNNING")
        success_job = mock.Mock(status="SUCCESS")
        mock_check_query_job.side_effect = [running_job, running_job, success_job]

        result = self.hook.wait_for_query_agent_engine_job(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
            poll_interval=10,
        )

        assert result is success_job
        assert mock_check_query_job.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(10)

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.check_query_agent_engine_job"), autospec=True)
    def test_wait_for_query_agent_engine_job_raises_on_failed_status(self, mock_check_query_job):
        mock_check_query_job.return_value.status = "FAILED"

        with pytest.raises(RuntimeError, match="Agent Engine query job .* failed"):
            self.hook.wait_for_query_agent_engine_job(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                operation_id=QUERY_OPERATION_ID,
                config=CHECK_QUERY_CONFIG,
            )

    @mock.patch(AGENT_ENGINE_STRING.format("time.sleep"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.check_query_agent_engine_job"), autospec=True)
    def test_wait_for_query_agent_engine_job_raises_on_unexpected_status(
        self, mock_check_query_job, mock_sleep
    ):
        mock_check_query_job.return_value.status = "CANCELLED"

        with pytest.raises(
            RuntimeError,
            match=f"Agent Engine query job {QUERY_OPERATION_NAME} completed with unexpected status CANCELLED.",
        ):
            self.hook.wait_for_query_agent_engine_job(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                operation_id=QUERY_OPERATION_ID,
                config=CHECK_QUERY_CONFIG,
            )

        mock_sleep.assert_not_called()

    @mock.patch(AGENT_ENGINE_STRING.format("time.sleep"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("time.monotonic"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.check_query_agent_engine_job"), autospec=True)
    def test_wait_for_query_agent_engine_job_times_out(
        self, mock_check_query_job, mock_monotonic, mock_sleep
    ):
        mock_check_query_job.return_value.status = "RUNNING"
        mock_monotonic.side_effect = [1, 3]

        with pytest.raises(TimeoutError, match="Timed out waiting for Agent Engine query job"):
            self.hook.wait_for_query_agent_engine_job(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                operation_id=QUERY_OPERATION_ID,
                config=CHECK_QUERY_CONFIG,
                timeout=1,
            )

        mock_sleep.assert_not_called()

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_update_agent_engine(self, mock_get_client):
        result = self.hook.update_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            config=CONFIG,
        )

        mock_get_client.return_value.update.assert_called_once_with(
            name=AGENT_ENGINE_NAME,
            agent=None,
            config=CONFIG,
        )
        assert result == mock_get_client.return_value.update.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_delete_agent_engine(self, mock_get_client):
        result = self.hook.delete_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            agent_engine_id=AGENT_ENGINE_ID,
            force=True,
            config=CONFIG,
        )

        mock_get_client.return_value.delete.assert_called_once_with(
            name=AGENT_ENGINE_NAME,
            force=True,
            config=CONFIG,
        )
        assert result == mock_get_client.return_value.delete.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("google.auth.transport.requests.AuthorizedSession"), autospec=True)
    def test_get_agent_engine_operation(self, mock_session):
        self.hook.get_credentials = mock.Mock(return_value=mock.sentinel.credentials, spec=())
        mock_session.return_value.get.return_value.json.return_value = {"name": OPERATION_NAME, "done": True}

        result = self.hook.get_agent_engine_operation(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=OPERATION_ID,
        )

        mock_session.assert_called_once_with(mock.sentinel.credentials)
        mock_session.return_value.get.assert_called_once_with(
            f"https://{GCP_LOCATION}-aiplatform.googleapis.com/v1beta1/{OPERATION_NAME}",
            timeout=60.0,
        )
        mock_session.return_value.get.return_value.raise_for_status.assert_called_once_with()
        assert result == {"name": OPERATION_NAME, "done": True}

    @mock.patch(AGENT_ENGINE_STRING.format("google.auth.transport.requests.AuthorizedSession"), autospec=True)
    def test_get_agent_engine_operation_with_request_timeout(self, mock_session):
        self.hook.get_credentials = mock.Mock(return_value=mock.sentinel.credentials, spec=())
        mock_session.return_value.get.return_value.json.return_value = {"name": OPERATION_NAME, "done": True}

        self.hook.get_agent_engine_operation(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=OPERATION_ID,
            request_timeout=10,
        )

        mock_session.return_value.get.assert_called_once_with(
            f"https://{GCP_LOCATION}-aiplatform.googleapis.com/v1beta1/{OPERATION_NAME}",
            timeout=10,
        )

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_operation"), autospec=True)
    def test_wait_for_agent_engine_operation_returns_when_done(self, mock_get_operation):
        mock_get_operation.return_value = {"name": OPERATION_NAME, "done": True}

        self.hook.wait_for_agent_engine_operation(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=OPERATION_ID,
        )

        mock_get_operation.assert_called_once_with(
            self.hook,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=OPERATION_ID,
        )

    @mock.patch(AGENT_ENGINE_STRING.format("time.sleep"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_operation"), autospec=True)
    def test_wait_for_agent_engine_operation_polls_until_done(self, mock_get_operation, mock_sleep):
        running_operation = {"name": OPERATION_NAME, "done": False}
        done_operation = {"name": OPERATION_NAME, "done": True}
        mock_get_operation.side_effect = [running_operation, running_operation, done_operation]

        self.hook.wait_for_agent_engine_operation(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=OPERATION_ID,
            poll_interval=10,
        )

        assert mock_get_operation.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(10)

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_operation"), autospec=True)
    def test_wait_for_agent_engine_operation_raises_on_error(self, mock_get_operation):
        mock_get_operation.return_value = {"name": OPERATION_NAME, "done": True, "error": {"message": "boom"}}

        with pytest.raises(RuntimeError, match="Agent Engine operation .* failed"):
            self.hook.wait_for_agent_engine_operation(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                operation_id=OPERATION_ID,
            )

    @mock.patch(AGENT_ENGINE_STRING.format("time.sleep"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("time.monotonic"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_operation"), autospec=True)
    def test_wait_for_agent_engine_operation_times_out(self, mock_get_operation, mock_monotonic, mock_sleep):
        mock_get_operation.return_value = {"name": OPERATION_NAME, "done": False}
        mock_monotonic.side_effect = [1, 3]

        with pytest.raises(TimeoutError, match="Timed out waiting for Agent Engine operation"):
            self.hook.wait_for_agent_engine_operation(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                operation_id=OPERATION_ID,
                timeout=1,
            )

        mock_sleep.assert_not_called()


class TestAgentEngineAsyncHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseAsyncHook.__init__"),
            return_value=None,
        ):
            self.hook = AgentEngineAsyncHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @pytest.mark.asyncio
    async def test_check_query_agent_engine_job_calls_sync_hook(self):
        sync_hook = mock.Mock(spec=AgentEngineHook)
        sync_hook.check_query_agent_engine_job.return_value = mock.sentinel.query_job
        self.hook.get_sync_hook = mock.AsyncMock(return_value=sync_hook)

        result = await self.hook.check_query_agent_engine_job(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
        )

        sync_hook.check_query_agent_engine_job.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            operation_id=QUERY_OPERATION_ID,
            config=CHECK_QUERY_CONFIG,
        )
        assert result == mock.sentinel.query_job
