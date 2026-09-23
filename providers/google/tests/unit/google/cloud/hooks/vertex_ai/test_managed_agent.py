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
from google.api_core.exceptions import InvalidArgument, NotFound, PermissionDenied, Unauthenticated
from google.cloud.aiplatform_v1 import ReasoningEngineExecutionServiceClient
from google.cloud.aiplatform_v1.types import QueryReasoningEngineResponse

from airflow.providers.google.cloud.hooks.vertex_ai.agent_engine import AgentEngineHook

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

pytest.importorskip("airflow.providers.common.ai.managed_agents.contract")

from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
from airflow.providers.common.ai.managed_agents import (
    ManagedAgentCapabilities,
    ManagedAgentRef,
    ManagedAgentRequest,
)
from airflow.providers.google.cloud.hooks.vertex_ai.managed_agent import (
    AgentEngineManagedAgentHook,
)

ENGINE = "projects/test-project/locations/us-central1/reasoningEngines/123"
BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"


@pytest.fixture
def execution_client():
    client = mock.create_autospec(ReasoningEngineExecutionServiceClient, instance=True)
    client.reasoning_engine_path.side_effect = ReasoningEngineExecutionServiceClient.reasoning_engine_path
    client.query_reasoning_engine.return_value = QueryReasoningEngineResponse(
        output={"output": "42", "steps": 3}
    )
    with mock.patch.object(
        AgentEngineHook, "get_reasoning_engine_execution_service_client", autospec=True, return_value=client
    ):
        yield client


@pytest.fixture
def hook():
    with mock.patch(BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id):
        yield AgentEngineManagedAgentHook(gcp_conn_id="google_cloud_default")


class TestContract:
    def test_resolves_a_full_resource_name(self, hook):
        assert hook.resolve_agent(ENGINE) == ManagedAgentRef(platform="gcp.vertex_agent_engine", name=ENGINE)

    @pytest.mark.parametrize("agent", ["123", "projects/p/locations/l/agents/123", ENGINE + "/operations/1"])
    def test_rejects_anything_but_a_full_resource_name(self, hook, agent):
        with pytest.raises(ValueError, match="reasoningEngines"):
            hook.resolve_agent(agent)

    def test_capabilities(self, hook):
        assert hook.agent_capabilities(ENGINE) == ManagedAgentCapabilities(structured_output=True)


class TestInvoke:
    def test_prompt_goes_to_the_query_method_and_output_is_unwrapped(self, hook, execution_client):
        response = hook.agent(ENGINE).invoke(ManagedAgentRequest(prompt="Sum?", timeout=12.5))
        kwargs = execution_client.query_reasoning_engine.call_args.kwargs
        assert kwargs["request"] == {"name": ENGINE, "class_method": "query", "input": {"input": "Sum?"}}
        assert kwargs["timeout"] == 12.5
        assert response.text == "42"
        assert response.structured == {"output": "42", "steps": 3.0}
        assert response.raw == {"output": {"output": "42", "steps": 3.0}}

    def test_class_method_and_input_key_are_configurable_on_the_hook(self, execution_client):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            hook = AgentEngineManagedAgentHook(class_method="ask", input_key="question")
        hook.agent(ENGINE).invoke(ManagedAgentRequest(prompt="Sum?"))
        request = execution_client.query_reasoning_engine.call_args.kwargs["request"]
        assert request["class_method"] == "ask"
        assert request["input"] == {"question": "Sum?"}

    def test_class_method_can_be_overridden_per_request(self, hook, execution_client):
        hook.agent(ENGINE).invoke(ManagedAgentRequest(prompt="Sum?", vendor_options={"class_method": "plan"}))
        assert execution_client.query_reasoning_engine.call_args.kwargs["request"]["class_method"] == "plan"

    def test_messages_are_sent_as_messages(self, hook, execution_client):
        messages = [{"role": "user", "content": "hi"}]
        hook.agent(ENGINE).invoke(ManagedAgentRequest(messages=messages))
        assert execution_client.query_reasoning_engine.call_args.kwargs["request"]["input"] == {
            "messages": messages
        }

    def test_a_plain_string_output_is_the_text(self, hook, execution_client):
        execution_client.query_reasoning_engine.return_value = QueryReasoningEngineResponse(output="plain")
        response = hook.agent(ENGINE).invoke(ManagedAgentRequest(prompt="x"))
        assert response.text == "plain"
        assert response.structured is None

    @pytest.mark.parametrize("option", ["reasoning_engine_id", "project_id", "output_gcs_uri"])
    def test_only_the_query_methods_own_options_are_accepted(self, hook, execution_client, option):
        with pytest.raises(ValueError, match=option):
            hook.agent(ENGINE).invoke(ManagedAgentRequest(prompt="x", vendor_options={option: "9"}))
        execution_client.query_reasoning_engine.assert_not_called()

    def test_a_session_is_refused_rather_than_silently_dropped(self, hook, execution_client):
        with pytest.raises(ValueError, match="conversation state"):
            hook.agent(ENGINE).invoke(ManagedAgentRequest(prompt="x", session_id="t"))
        execution_client.query_reasoning_engine.assert_not_called()

    @pytest.mark.parametrize(
        ("error", "expected"),
        [
            (InvalidArgument("bad input"), "class_method='query'"),
            (NotFound("no such engine"), "no such engine"),
            (PermissionDenied("denied"), "denied"),
            (Unauthenticated("expired"), "expired"),
        ],
    )
    def test_api_errors_are_terminal_and_name_the_engine(self, hook, execution_client, error, expected):
        # INVALID_ARGUMENT is an author-side mistake on Agent Engine, not something a rephrase fixes.
        execution_client.query_reasoning_engine.side_effect = error
        with pytest.raises(ManagedAgentInvocationError, match=expected) as excinfo:
            hook.agent(ENGINE).invoke(ManagedAgentRequest(prompt="x"))
        assert ENGINE in str(excinfo.value)

    def test_a_bad_agent_name_fails_before_any_client_is_built(self, hook):
        with mock.patch.object(
            AgentEngineHook, "get_reasoning_engine_execution_service_client", autospec=True
        ) as get:
            with pytest.raises(ValueError, match="reasoningEngines"):
                hook.agent("123").invoke(ManagedAgentRequest(prompt="x"))
        get.assert_not_called()
