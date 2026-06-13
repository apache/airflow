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
from google.genai.errors import ClientError

from airflow.providers.google.cloud.hooks.vertex_ai.agent_engine import AgentEngineHook

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
AGENT_ENGINE_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.agent_engine.{}"

TEST_GCP_CONN_ID = "test-gcp-conn-id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"
AGENT_ENGINE_NAME = "projects/test-project/locations/us-central1/reasoningEngines/123"
CONFIG = {"display_name": "test-agent-engine"}
QUERY_CONFIG = {"class_method": "query", "input": {"prompt": "hello"}}


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
            agent_engine=None,
            config=CONFIG,
        )
        assert result == mock_get_client.return_value.create.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_get_agent_engine(self, mock_get_client):
        result = self.hook.get_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
        )

        mock_get_client.return_value.get.assert_called_once_with(name=AGENT_ENGINE_NAME)
        assert result == mock_get_client.return_value.get.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("google.auth.transport.requests.AuthorizedSession"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_credentials"), autospec=True)
    def test_query_agent_engine(self, mock_get_credentials, mock_session_cls):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"output": {"answer": "hello"}}
        mock_session_cls.return_value.post.return_value = mock_response

        result = self.hook.query_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
            config=QUERY_CONFIG,
        )

        mock_session_cls.assert_called_once_with(mock_get_credentials.return_value)
        mock_session_cls.return_value.post.assert_called_once_with(
            f"https://{GCP_LOCATION}-aiplatform.googleapis.com/v1beta1/{AGENT_ENGINE_NAME}:query",
            json={"classMethod": "query", "input": {"prompt": "hello"}},
        )
        mock_response.raise_for_status.assert_called_once_with()
        assert result == {"answer": "hello"}

    @mock.patch(AGENT_ENGINE_STRING.format("google.auth.transport.requests.AuthorizedSession"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_credentials"), autospec=True)
    def test_query_agent_engine_returns_full_response_when_output_missing(
        self, mock_get_credentials, mock_session_cls
    ):
        full_response = {"someOtherField": "value"}
        mock_response = mock.Mock()
        mock_response.json.return_value = full_response
        mock_session_cls.return_value.post.return_value = mock_response

        result = self.hook.query_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
            config=QUERY_CONFIG,
        )

        assert result == full_response

    @mock.patch(AGENT_ENGINE_STRING.format("google.auth.transport.requests.AuthorizedSession"), autospec=True)
    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_credentials"), autospec=True)
    def test_query_agent_engine_parses_json_string_input(self, mock_get_credentials, mock_session_cls):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"output": {"answer": "hello"}}
        mock_session_cls.return_value.post.return_value = mock_response

        result = self.hook.query_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
            config={"class_method": "query", "input": '{"prompt": "hello"}'},
        )

        mock_session_cls.return_value.post.assert_called_once_with(
            f"https://{GCP_LOCATION}-aiplatform.googleapis.com/v1beta1/{AGENT_ENGINE_NAME}:query",
            json={"classMethod": "query", "input": {"prompt": "hello"}},
        )
        assert result == {"answer": "hello"}

    @pytest.mark.parametrize(
        "input_value",
        [
            "test string",
            '"test string"',
            '["prompt", "hello"]',
            1,
            ["prompt", "hello"],
        ],
    )
    @mock.patch(AGENT_ENGINE_STRING.format("google.auth.transport.requests.AuthorizedSession"), autospec=True)
    def test_query_agent_engine_rejects_non_object_input(self, mock_session_cls, input_value):
        with pytest.raises(ValueError, match="Agent Engine query input must be a JSON object."):
            self.hook.query_agent_engine(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                name=AGENT_ENGINE_NAME,
                config={"class_method": "query", "input": input_value},
            )

        mock_session_cls.assert_not_called()

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_update_agent_engine(self, mock_get_client):
        result = self.hook.update_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
            config=CONFIG,
        )

        mock_get_client.return_value.update.assert_called_once_with(
            name=AGENT_ENGINE_NAME,
            agent=None,
            agent_engine=None,
            config=CONFIG,
        )
        assert result == mock_get_client.return_value.update.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_update_agent_engine_with_deprecated_agent_engine_alias(self, mock_get_client):
        agent_engine = object()

        result = self.hook.update_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
            agent_engine=agent_engine,
            config=CONFIG,
        )

        mock_get_client.return_value.update.assert_called_once_with(
            name=AGENT_ENGINE_NAME,
            agent=None,
            agent_engine=agent_engine,
            config=CONFIG,
        )
        assert result == mock_get_client.return_value.update.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine_client"), autospec=True)
    def test_delete_agent_engine(self, mock_get_client):
        result = self.hook.delete_agent_engine(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
            force=True,
            config=CONFIG,
        )

        mock_get_client.return_value.delete.assert_called_once_with(
            name=AGENT_ENGINE_NAME,
            force=True,
            config=CONFIG,
        )
        assert result == mock_get_client.return_value.delete.return_value

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine"), autospec=True)
    def test_is_agent_engine_deleted_returns_false_when_resource_exists(self, mock_get_agent_engine):
        assert not self.hook.is_agent_engine_deleted(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
        )
        mock_get_agent_engine.assert_called_once_with(
            self.hook,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
        )

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine"), autospec=True)
    def test_is_agent_engine_deleted_returns_true_on_404(self, mock_get_agent_engine):
        mock_get_agent_engine.side_effect = ClientError(code=404, response_json={"error": "not found"})

        assert self.hook.is_agent_engine_deleted(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            name=AGENT_ENGINE_NAME,
        )

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine"), autospec=True)
    def test_is_agent_engine_deleted_reraises_non_404(self, mock_get_agent_engine):
        mock_get_agent_engine.side_effect = ClientError(code=500, response_json={"error": "server error"})

        with pytest.raises(ClientError) as err:
            self.hook.is_agent_engine_deleted(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                name=AGENT_ENGINE_NAME,
            )

        assert err.value.code == 500

    @mock.patch(AGENT_ENGINE_STRING.format("AgentEngineHook.get_agent_engine"), autospec=True)
    def test_is_agent_engine_deleted_reraises_non_404_with_404_in_message(self, mock_get_agent_engine):
        mock_get_agent_engine.side_effect = ClientError(
            code=500,
            response_json={"error": "server error for resource 404"},
        )

        with pytest.raises(ClientError) as err:
            self.hook.is_agent_engine_deleted(
                project_id=GCP_PROJECT,
                location=GCP_LOCATION,
                name=AGENT_ENGINE_NAME,
            )

        assert err.value.code == 500
